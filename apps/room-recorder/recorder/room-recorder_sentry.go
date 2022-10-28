package recorder

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"os"
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
	RECONNECTION_INTERVAL time.Duration = 10 * time.Second
)

func (s *RoomRecorderService) onRTCTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	log.Infof("onRTCTrack track: %+v", track)
	log.Infof("onRTCTrack receiver: %+v", receiver)

	trackCh := make(chan Track, 128)
	eofCh := make(chan bool)
	go s.insertTracksOnInterval(
		OnTrack{
			time.Since(s.startTime),
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
	buf := make([]byte, 65536)
	for {
		readCnt, _, readErr := track.Read(buf)
		if readErr != nil {
			if readErr.Error() == "EOF" {
				log.Infof("End of stream")
			} else {
				log.Errorf("%v", readErr)
			}
			eofCh <- true
			return
		}
		if readCnt == 0 {
			continue
		}
		track := make([]byte, readCnt)
		copy(track, buf)
		trackCh <- Track{time.Since(s.startTime), track}
	}
}

func (s *RoomRecorderService) onRTCError(err error) {
	log.Errorf("onRTCError: %+v", err)
	s.joinRoomCh <- true
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
			time.Since(s.startTime),
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
		s.joinRoomCh <- true
		return
	}
	s.timeReady = time.Now().Format(time.RFC3339)
	log.Infof("rtc.Join ok roomid=%v", s.roomId)
}

func (s *RoomRecorderService) onRoomPeerEvent(state sdk.PeerState, peer sdk.PeerInfo) {
	log.Infof("onRoomPeerEvent state = %+v, peer = %+v", state, peer)
	go s.insertPeerEvent(
		PeerEvent{
			time.Since(s.startTime),
			state,
			peer.Uid,
			peer.DisplayName})
}

func (s *RoomRecorderService) onRoomMessage(from string, to string, data map[string]interface{}) {
	log.Infof("onRoomMessage from = %+v, to = %+v, data = %+v", from, to, data)
	go s.insertChats(Chat{time.Since(s.startTime), data})
}

func (s *RoomRecorderService) onRoomDisconnect(sid, reason string) {
	log.Infof("onRoomDisconnect sid = %+v, reason = %+v", sid, reason)
	s.joinRoomCh <- true
}

func (s *RoomRecorderService) onRoomError(err error) {
	log.Errorf("onRoomError %v", err)
	s.joinRoomCh <- true
}

// TBD onRoomLeave
func (s *RoomRecorderService) onRoomLeave(success bool, err error) {
	log.Warnf("Not Implemented onRoomLeave: success %v, onLeave %v", success, err)
}

// TBD onRoomInfo
func (s *RoomRecorderService) onRoomInfo(info sdk.RoomInfo) {
	log.Warnf("Not Implemented onRoomInfo: %v", info)
}

// TBD onRTCDataChannel
func (s *RoomRecorderService) onRTCDataChannel(dc *webrtc.DataChannel) {
	log.Warnf("Not Implemented onRTCDataChannel: %+v", dc)
}

// TBD onRTCSpeaker
func (s *RoomRecorderService) onRTCSpeaker(event []string) {
	log.Warnf("Not Implemented onRTCSpeaker: %+v", event)
}

func (s *RoomRecorderService) closeRoom() {
	if s.roomRTC != nil {
		s.roomRTC.Close()
		s.roomRTC = nil
	}
	if s.roomService != nil {
		s.roomService.Leave(s.roomId, s.systemUid)
		s.roomService.Close()
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

func (s *RoomRecorderService) getRoomInfo() {
	var room Room
	for {
		room = s.getRoomsByRoomid(s.roomId)
		if room.status == ROOM_BOOKED {
			log.Warnf("Room is not started yet, check again in a minute")
			time.Sleep(time.Minute)
			continue
		}
		break
	}
	s.startTime = room.startTime
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

func (s *RoomRecorderService) roomRecorderSentry() {
	for {
		time.Sleep(s.chopInterval)
		s.UpdateRoomRecord()
	}
}

func (s *RoomRecorderService) joinRoom() {
	log.Infof("--- Joining Room ---")
	var err error
	if s.roomRecordId == "" {
		for retryDelay := 0; retryDelay < DB_RETRY; retryDelay++ {
			s.roomRecordId = uuid.NewString()
			insertStmt := `insert into  "` + s.roomRecordSchema + `"."roomRecord"(
					"id",
					"roomId",
					"startTime",
					"endTime",
					"folderPath")
					values($1, $2, $3, $4, $5)`
			for retry := 0; retry < DB_RETRY; retry++ {
				_, err = s.postgresDB.Exec(insertStmt,
					s.roomRecordId,
					s.roomId,
					s.startTime,
					time.Now(),
					s.bucketName+s.folderName)
				if err == nil {
					break
				}
				if strings.Contains(err.Error(), DUP_PK) {
					s.roomRecordId = uuid.NewString()
				}
			}
			if err != nil {
				log.Errorf("could not insert into database: %s", err)
			}
			if err == nil {
				break
			}
			time.Sleep(5 * time.Minute)
		}
	}
	if err != nil {
		log.Errorf("could not proceed with room recording")
		os.Exit(1)
	}
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
		s.joinRoomCh <- true
		return
	}

	log.Infof("room.Join ok roomid=%v", s.roomId)
}

func (s *RoomRecorderService) UpdateRoomRecord() {
	if s.roomRecordId == "" {
		return
	}
	updateStmt := `update "` + s.roomRecordSchema + `"."roomRecord" set "endTime"=$1 where "id"=$2`
	var err error
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(updateStmt, time.Now(), s.roomRecordId)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("could not update database: %s", err)
	}
}

func (s *RoomRecorderService) insertTrackEvent(trackEvent TrackEvent) {
	var err error
	insertStmt := `insert into "` + s.roomRecordSchema + `"."trackEvent"(
					"id",
					"roomRecordId",
					"roomId",
					"timeElapsed",
					"state",
					"trackEventId")
					values($1, $2, $3, $4, $5, $6)`
	trackEventId := uuid.NewString()
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			trackEventId,
			s.roomRecordId,
			s.roomId,
			trackEvent.timeElapsed,
			trackEvent.state,
			trackEvent.trackEventId)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			trackEventId = uuid.NewString()
		}
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
		return
	}

	insertStmt = `insert into "` + s.roomRecordSchema + `"."trackInfo"(
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
					values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
	for _, trackInfo := range trackEvent.tracks {
		dbId := uuid.NewString()
		for retry := 0; retry < DB_RETRY; retry++ {
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

func (s *RoomRecorderService) insertPeerEvent(peerEvent PeerEvent) {
	var err error
	insertStmt := `insert into "` + s.roomRecordSchema + `"."peerEvent"(
					"id",
					"roomRecordId",
					"roomId",
					"timeElapsed",
					"state",
					"peerId",
					"peerName")
					values($1, $2, $3, $4, $5, $6, $7)`
	dbId := uuid.NewString()
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			dbId,
			s.roomRecordId,
			s.roomId,
			peerEvent.timeElapsed,
			peerEvent.state,
			peerEvent.peerId,
			peerEvent.peerName)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			dbId = uuid.NewString()
		}
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
		return
	}
	peerEvent = PeerEvent{}
}

func (s *RoomRecorderService) insertTracksOnInterval(
	onTrack OnTrack,
	trackCh chan Track,
	eofCh chan bool) {

	dbId := s.insertOnTracks(onTrack)
	trackId := onTrack.trackId

	var subFolderName string
	if onTrack.trackRemote.Kind == webrtc.RTPCodecTypeAudio {
		subFolderName = s.audioFolderName
	} else {
		subFolderName = s.videoFolderName
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
				subFolderName,
				trackSavedId,
				len(tracks))
			return
		default:
			if time.Since(lastSavedTime) > s.chopInterval {
				lastSavedTime = time.Now()
				newSavedId := len(tracks)
				go s.insertTracks(
					tracks,
					trackId,
					dbId,
					subFolderName,
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
	insertStmt := `insert into "` + s.roomRecordSchema + `"."onTrack"(
					"id",
					"roomRecordId",
					"roomId",
					"timeElapsed",
					"trackRemote")
					values($1, $2, $3, $4, $5)`
	dbId := uuid.NewString()
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			dbId,
			s.roomRecordId,
			s.roomId,
			onTrack.timeElapsed,
			trackRemote)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			dbId = uuid.NewString()
		}
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
	}
	return dbId
}

func (s *RoomRecorderService) insertTracks(
	tracks []Track,
	trackID, dbId, subFolderName string,
	startId, endId int) {

	if startId >= endId {
		return
	}
	var err error
	insertStmt := `insert into "` + s.roomRecordSchema + `"."trackStream"(
					"id",
					"onTrackId",
					"roomId",
					"timeElapsed",
					"filePath")
					values($1, $2, $3, $4, $5)`
	objName := uuid.NewString()
	filePath := subFolderName + objName
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			objName,
			dbId,
			s.roomId,
			tracks[startId].TimeElapsed,
			filePath)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			objName = uuid.NewString()
			filePath = subFolderName + objName
		}
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
	}

	data := new(bytes.Buffer)
	err = gob.NewEncoder(data).Encode(tracks[startId:endId])
	if err != nil {
		log.Errorf("track encoding error:", err)
		return
	}
	var uploadInfo minio.UploadInfo
	for retry := 0; retry < DB_RETRY; retry++ {
		uploadInfo, err = s.minioClient.PutObject(context.Background(),
			s.bucketName,
			s.folderName+filePath,
			data,
			int64(data.Len()),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err == nil {
			break
		}
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

type ChatPayload struct {
	Msg *Payload `json:"msg,omitempty"`
}

type Payload struct {
	Uid              *string     `json:"uid,omitempty"`
	Name             *string     `json:"name,omitempty"`
	Text             *string     `json:"text,omitempty"`
	Timestamp        *time.Time  `json:"timestamp,omitempty"`
	Base64File       *Attachment `json:"base64File,omitempty"`
	IgnoreByRecorder *bool       `json:"ignoreByRecorder,omitempty"`
}

type Attachment struct {
	Name *string `json:"name,omitempty"`
	Size *int    `json:"size,omitempty"`
	Data *string `json:"data,omitempty"`
}

func (s *RoomRecorderService) insertChats(chat Chat) {
	var err error
	jsonStr, err := json.Marshal(chat.data)
	if err != nil {
		log.Errorf("error decoding chat message %s", err.Error())
		return
	}
	var chatPayload ChatPayload
	err = json.Unmarshal(jsonStr, &chatPayload)
	if err != nil {
		log.Errorf("error decoding chat message %s", err.Error())
		return
	}

	if chatPayload.Msg.IgnoreByRecorder != nil {
		log.Infof("not recording this chat message which has IgnoreByRecorder")
		return
	}
	if chatPayload.Msg.Uid == nil {
		log.Errorf("chat message has no sender id")
		return
	}
	if chatPayload.Msg.Name == nil {
		log.Errorf("chat message has no sender name")
		return
	}
	if chatPayload.Msg.Timestamp == nil {
		timeStamp := time.Now()
		chatPayload.Msg.Timestamp = &timeStamp
	}

	if chatPayload.Msg.Text != nil {
		s.insertChatText(chat, chatPayload)
	} else if chatPayload.Msg.Base64File != nil {
		s.insertChatFile(chat, chatPayload)
	} else {
		jsonStr, _ = json.MarshalIndent(chat.data, "", "    ")
		log.Warnf("chat message is on neither text nor file type:\n%s", jsonStr)
	}
	chat.data = nil
	chat = Chat{}
}

func (s *RoomRecorderService) insertChatText(chat Chat, chatPayload ChatPayload) {
	var err error
	insertStmt := `insert into "` + s.roomRecordSchema + `"."chatMessage"(
					"id",
					"roomRecordId",
					"roomId",
					"timeElapsed",
					"userId",
					"userName",
					"text",
					"timestamp")
					values($1, $2, $3, $4, $5, $6, $7, $8)`
	dbId := uuid.NewString()
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			dbId,
			s.roomRecordId,
			s.roomId,
			chat.timeElapsed,
			*chatPayload.Msg.Uid,
			*chatPayload.Msg.Name,
			*chatPayload.Msg.Text,
			*chatPayload.Msg.Timestamp)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			dbId = uuid.NewString()
		}
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
	}
}

func (s *RoomRecorderService) insertChatFile(chat Chat, chatPayload ChatPayload) {
	var err error
	if chatPayload.Msg.Base64File.Data == nil {
		log.Errorf("chat attachment has no data")
		return
	}
	if chatPayload.Msg.Base64File.Name == nil {
		log.Errorf("chat attachment has no name")
		return
	}
	if chatPayload.Msg.Base64File.Size == nil {
		log.Errorf("chat attachment has no size")
		return
	}

	insertStmt := `insert into "` + s.roomRecordSchema + `"."chatAttachment"(
					"id",
					"roomRecordId",
					"roomId",
					"timeElapsed",
					"userId",
					"userName",
					"fileName",
					"fileSize",
					"filePath",
					"timestamp")
					values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	objName := uuid.NewString()
	filePath := s.attachmentFolderName + objName
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			objName,
			s.roomRecordId,
			s.roomId,
			chat.timeElapsed,
			*chatPayload.Msg.Uid,
			*chatPayload.Msg.Name,
			*chatPayload.Msg.Base64File.Name,
			*chatPayload.Msg.Base64File.Size,
			filePath,
			*chatPayload.Msg.Timestamp)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			objName = uuid.NewString()
			filePath = s.attachmentFolderName + objName
		}
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
		return
	}

	data := bytes.NewReader([]byte(*chatPayload.Msg.Base64File.Data))
	var uploadInfo minio.UploadInfo
	for retry := 0; retry < DB_RETRY; retry++ {
		uploadInfo, err = s.minioClient.PutObject(context.Background(),
			s.bucketName,
			s.folderName+filePath,
			data,
			int64(len(*chatPayload.Msg.Base64File.Data)),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("could not upload file: %s", err)
	}
	log.Infof("successfully uploaded bytes: ", uploadInfo)
}

func (s *RoomRecorderService) testDownload(filePath, filename string) {
	object, err := s.minioClient.GetObject(context.Background(),
		s.bucketName,
		filePath,
		minio.GetObjectOptions{})
	if err != nil {
		log.Errorf("error downloading file: %s", err)
		return
	}
	localFile, err := os.Create(filename)
	if err != nil {
		log.Errorf("error creating file: %s", err)
		return
	}
	downloadInfo, err := io.Copy(localFile, object)
	if err != nil {
		log.Errorf("error copying file: %s", err)
		return
	}
	log.Infof("Successfully downloaded bytes: %d", downloadInfo)
}

func (s *RoomRecorderService) testUpload(filePath, filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Errorf("error opening file: %s", err)
		return
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		log.Errorf("error accessing file: %s", err)
		return
	}

	uploadInfo, err := s.minioClient.PutObject(context.Background(),
		s.bucketName,
		filePath,
		file,
		fileStat.Size(),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Errorf("error uploading file: %s", err)
		return
	}
	log.Infof("Successfully uploaded: %+v", uploadInfo)
}
