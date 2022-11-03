package recorder

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	minio "github.com/minio/minio-go/v7"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v3"
)

const (
	RECONNECTION_INTERVAL time.Duration = 2 * time.Second

	VIDEO_FOLDERNAME string = "/video/"
	AUDIO_FOLDERNAME string = "/audio/"
)

type TrackEvent struct {
	timestamp time.Time
	state     sdk.TrackEvent_State
	peerId    string
	tracks    []sdk.TrackInfo
}

type TrackRemote struct {
	Id          string
	StreamID    string
	PayloadType webrtc.PayloadType
	Kind        webrtc.RTPCodecType
	Ssrc        webrtc.SSRC
	Codec       webrtc.RTPCodecParameters
	Rid         string
}

type OnTrack struct {
	timestamp   time.Time
	trackId     string
	trackRemote TrackRemote
}

type Track struct {
	Timestamp time.Time
	Data      []byte
}

func (s *RoomRecorderService) onRTCTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	log.Infof("onRTCTrack track: %+v", track)
	log.Infof("onRTCTrack receiver: %+v", receiver)

	trackCh := make(chan Track, 128)
	eofCh := make(chan struct{})
	go s.insertTracksOnInterval(
		OnTrack{
			time.Now(),
			track.ID(),
			TrackRemote{
				track.ID(),
				track.StreamID(),
				track.PayloadType(),
				track.Kind(),
				track.SSRC(),
				track.Codec(),
				track.RID()}},
		trackCh,
		eofCh)

	// Send a PLI on an interval so that the publisher is pushing a keyframe every rtcpPLIInterval
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			rtcpSendErr := s.roomRTC.GetSubTransport().GetPeerConnection().WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())}})
			if rtcpSendErr != nil {
				log.Errorf("%v", rtcpSendErr)
			}
		}
	}()

	codecName := strings.Split(track.Codec().RTPCodecCapability.MimeType, "/")[1]
	log.Infof("Track has started, of type %d: %s \n", track.PayloadType(), codecName)
	buf := make([]byte, 65535)
	for {
		readCnt, _, readErr := track.Read(buf)
		if readErr != nil {
			if readErr.Error() == "EOF" {
				log.Infof("End of stream")
			} else {
				log.Errorf("%v", readErr)
			}
			close(eofCh)
			return
		}
		if readCnt == 0 {
			continue
		}
		track := make([]byte, readCnt)
		copy(track, buf)
		trackCh <- Track{time.Now(), track}
	}
}

func (s *RoomRecorderService) onRTCError(err error) {
	log.Errorf("onRTCError: %+v", err)
	s.joinRoomCh <- struct{}{}
}

func (s *RoomRecorderService) onRTCTrackEvent(event sdk.TrackEvent) {
	log.Infof("onRTCTrackEvent: %+v", event)
	tracks := make([]sdk.TrackInfo, 0)
	for _, track := range event.Tracks {
		log.Infof("onTrackEvent: %+v", track)
		tracks = append(tracks, *track)
	}
	go s.insertTrackEvent(
		TrackEvent{
			time.Now(),
			event.State,
			event.Uid,
			tracks})
}

func (s *RoomRecorderService) onRoomJoin(success bool, info sdk.RoomInfo, err error) {
	log.Infof("onRoomJoin success = %v, info = %v, err = %v", success, info, err)

	s.roomRTC.OnTrack = s.onRTCTrack
	s.roomRTC.OnDataChannel = s.onRTCDataChannel
	s.roomRTC.OnError = s.onRTCError
	s.roomRTC.OnTrackEvent = s.onRTCTrackEvent
	s.roomRTC.OnSpeaker = s.onRTCSpeaker

	err = s.roomRTC.Join(s.roomId, s.systemUid)
	if err != nil {
		s.joinRoomCh <- struct{}{}
		return
	}
	s.timeReady = time.Now().Format(time.RFC3339)
	log.Infof("rtc.Join ok roomid=%v", s.roomId)
}

func (s *RoomRecorderService) onRoomPeerEvent(state sdk.PeerState, peer sdk.PeerInfo) {
}

func (s *RoomRecorderService) onRoomMessage(from string, to string, data map[string]interface{}) {
}

func (s *RoomRecorderService) onRoomDisconnect(sid, reason string) {
	log.Infof("onRoomDisconnect sid = %+v, reason = %+v", sid, reason)
	s.joinRoomCh <- struct{}{}
}

func (s *RoomRecorderService) onRoomError(err error) {
	log.Errorf("onRoomError %v", err)
	s.joinRoomCh <- struct{}{}
}

func (s *RoomRecorderService) onRoomLeave(success bool, err error) {
}

func (s *RoomRecorderService) onRoomInfo(info sdk.RoomInfo) {
}

func (s *RoomRecorderService) onRTCDataChannel(dc *webrtc.DataChannel) {
}

func (s *RoomRecorderService) onRTCSpeaker(event []string) {
}

func (s *RoomRecorderService) closeRoom() {
	if s.roomRTC != nil {
		s.roomRTC = nil
	}
	if s.roomService != nil {
		s.roomService = nil
	}
	if s.sdkConnector != nil {
		s.sdkConnector = nil
	}
}

func getRoomService(config Config) (*sdk.Connector, *sdk.Room, *sdk.RTC, error) {
	log.Infof("--- Connecting to Room Signal ---")
	log.Infof("attempt gRPC connection to %s", config.Signal.Addr)
	sdkConnector := sdk.NewConnector(config.Signal.Addr)
	if sdkConnector == nil {
		log.Errorf("connection to %s fail", config.Signal.Addr)
		return nil, nil, nil, errors.New("")
	}
	roomService := sdk.NewRoom(sdkConnector)
	roomRTC, err := sdk.NewRTC(sdkConnector)
	if err != nil {
		log.Errorf("rtc connector fail: %s", err)
		return nil, nil, nil, errors.New("")
	}
	return sdkConnector, roomService, roomRTC, nil
}

func (s *RoomRecorderService) openRoom() {
	s.closeRoom()
	var err error
	for {
		time.Sleep(RECONNECTION_INTERVAL)
		s.sdkConnector, s.roomService, s.roomRTC, err = getRoomService(s.conf)
		if err == nil {
			s.isSdkConnected = true
			break
		}
	}
	s.joinRoom()
}

func (s *RoomRecorderService) checkForRoomError() {
	for {
		<-s.joinRoomCh
		s.timeReady = ""
		if s.isSdkConnected {
			s.isSdkConnected = false
			go s.openRoom()
		}
	}
}

func (s *RoomRecorderService) joinRoom() {
	log.Infof("--- Joining Room ---")
	var err error
	s.roomService.OnJoin = s.onRoomJoin
	s.roomService.OnPeerEvent = s.onRoomPeerEvent
	s.roomService.OnMessage = s.onRoomMessage
	s.roomService.OnDisconnect = s.onRoomDisconnect
	s.roomService.OnError = s.onRoomError
	s.roomService.OnLeave = s.onRoomLeave
	s.roomService.OnRoomInfo = s.onRoomInfo

	// join room
	err = s.roomService.Join(
		sdk.JoinInfo{
			Sid: s.roomId,
			Uid: s.systemUid + sdk.RandomKey(16),
		},
	)
	if err != nil {
		s.joinRoomCh <- struct{}{}
		return
	}

	log.Infof("room.Join ok roomid=%v", s.roomId)
}

func (s *RoomRecorderService) insertTrackEvent(trackEvent TrackEvent) {
	var err error
	insertStmt := `INSERT INTO "` + s.roomRecordSchema + `"."trackEvent"(
					"id",
					"roomId",
					"timestamp",
					"state",
					"peerId")
					VALUES($1, $2, $3, $4, $5)`
	s.waitUpload.Add(1)
	defer s.waitUpload.Done()
	trackEventId := uuid.NewString()
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			trackEventId,
			s.roomId,
			trackEvent.timestamp,
			trackEvent.state,
			trackEvent.peerId)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			trackEventId = uuid.NewString()
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
		return
	}

	insertStmt = `INSERT INTO "` + s.roomRecordSchema + `"."trackInfo"(
					"id",
					"trackEventId",
					"roomId",
					"trackId",
					"kind",
					"muted",
					"type",
					"streamId",
					"label",
					"subscribe",
					"layer",
					"direction",
					"width",
					"height",
					"frameRate")
					VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
	for _, trackInfo := range trackEvent.tracks {
		dbId := uuid.NewString()
		for retry := 0; retry < RETRY_COUNT; retry++ {
			_, err = s.postgresDB.Exec(insertStmt,
				dbId,
				trackEventId,
				s.roomId,
				trackInfo.Id,
				trackInfo.Kind,
				trackInfo.Muted,
				trackInfo.Type,
				trackInfo.StreamId,
				trackInfo.Label,
				trackInfo.Subscribe,
				trackInfo.Layer,
				trackInfo.Direction,
				trackInfo.Width,
				trackInfo.Height,
				trackInfo.FrameRate)
			if err == nil {
				break
			}
			if strings.Contains(err.Error(), DUP_PK) {
				dbId = uuid.NewString()
			}
			time.Sleep(RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not insert into database: %s", err)
			return
		}
	}
	for id := range trackEvent.tracks {
		trackEvent.tracks[id] = sdk.TrackInfo{}
	}
	trackEvent = TrackEvent{}
}

func (s *RoomRecorderService) insertTracksOnInterval(
	onTrack OnTrack,
	trackCh chan Track,
	eofCh chan struct{}) {

	s.waitUpload.Add(1)
	defer s.waitUpload.Done()
	dbId := s.insertOnTracks(onTrack)
	trackId := onTrack.trackId

	var folderName string
	if onTrack.trackRemote.Kind == webrtc.RTPCodecTypeAudio {
		folderName = AUDIO_FOLDERNAME
	} else {
		folderName = VIDEO_FOLDERNAME
	}
	onTrack.trackRemote = TrackRemote{}
	onTrack = OnTrack{}

	trackSavedId := 0
	tracks := make([]Track, 0)
	lastSavedTime := time.Now()
	for {
		select {
		case track := <-trackCh:
			tracks = append(tracks, track)
		case <-eofCh:
			s.insertTracks(
				tracks,
				trackId,
				dbId,
				folderName,
				trackSavedId,
				len(tracks))
			return
		case <-s.exitCh:
			s.insertTracks(
				tracks,
				trackId,
				dbId,
				folderName,
				trackSavedId,
				len(tracks))
			return
		default:
			time.Sleep(time.Second)
			if time.Since(lastSavedTime) > s.chopInterval {
				lastSavedTime = time.Now()
				newSavedId := len(tracks)
				go s.insertTracks(
					tracks,
					trackId,
					dbId,
					folderName,
					trackSavedId,
					newSavedId)
				trackSavedId = newSavedId
			}
		}
	}
}

func (s *RoomRecorderService) insertOnTracks(onTrack OnTrack) string {
	var err error
	trackRemote, _ := json.Marshal(onTrack.trackRemote)
	insertStmt := `INSERT INTO "` + s.roomRecordSchema + `"."onTrack"(
					"id",
					"roomId",
					"trackId",
					"timestamp",
					"trackRemote")
					VALUES($1, $2, $3, $4, $5)`
	dbId := uuid.NewString()
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			dbId,
			s.roomId,
			onTrack.trackId,
			onTrack.timestamp,
			trackRemote)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			dbId = uuid.NewString()
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
	}
	return dbId
}

func (s *RoomRecorderService) insertTracks(
	tracks []Track,
	trackID, dbId, folderName string,
	startId, endId int) {

	if startId >= endId {
		return
	}

	s.waitUpload.Add(1)
	defer s.waitUpload.Done()
	var err error
	insertStmt := `INSERT INTO "` + s.roomRecordSchema + `"."trackStream"(
					"id",
					"onTrackId",
					"roomId",
					"timestamp",
					"filePath")
					VALUES($1, $2, $3, $4, $5)`
	objName := uuid.NewString()
	filePath := folderName + objName
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			objName,
			dbId,
			s.roomId,
			tracks[startId].Timestamp,
			filePath)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			objName = uuid.NewString()
			filePath = folderName + objName
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
	}

	var data bytes.Buffer
	err = gob.NewEncoder(&data).Encode(tracks[startId:endId])
	if err != nil {
		log.Errorf("track encoding error:", err)
		return
	}
	var uploadInfo minio.UploadInfo
	for retry := 0; retry < RETRY_COUNT; retry++ {
		uploadInfo, err = s.minioClient.PutObject(context.Background(),
			s.bucketName,
			s.roomId+filePath,
			&data,
			int64(data.Len()),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not upload file: %s", err)
		return
	}
	for id := startId; id < endId; id++ {
		tracks[id].Data = nil
		tracks[id] = Track{}
	}
	log.Infof("successfully uploaded bytes: ", uploadInfo)
}
