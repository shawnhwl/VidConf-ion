package recorder

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	constants "github.com/pion/ion/apps/constants"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v3"
)

// RoomRecorderSession represents a room-recorder session instance
type RoomRecorderSession struct {
	conf     Config
	natsConn *nats.Conn

	postgresDB       *sql.DB
	roomMgmtSchema   string
	roomRecordSchema string

	minioClient *minio.Client
	bucketName  string

	exitCh     chan struct{}
	waitUpload *sync.WaitGroup

	sessionChOnce  sync.Once
	sessionCh      chan struct{}
	sessionId      string
	systemUserId   string
	systemUsername string
	chopInterval   time.Duration

	joinRoomCh     chan struct{}
	isSdkConnected bool

	sdkConnector *sdk.Connector
	roomService  *sdk.Room
	roomRTC      *sdk.RTC
	stunServer   []string
}

func (s *RoomRecorderService) newRoomRecorderSession(sessionId string) (*RoomRecorderSession, error) {
	if strings.HasPrefix(sessionId, s.conf.RoomMgmt.PlaybackIdPrefix) {
		errorString := fmt.Sprintf("'%s' is not a recording session", sessionId)
		log.Errorf(errorString)
		return nil, errors.New(errorString)
	}
	session := &RoomRecorderSession{
		conf:     s.conf,
		natsConn: s.natsConn,

		postgresDB:       s.postgresDB,
		roomMgmtSchema:   s.conf.Postgres.RoomMgmtSchema,
		roomRecordSchema: s.conf.Postgres.RoomRecordSchema,

		minioClient: s.minioClient,
		bucketName:  s.conf.Minio.BucketName,

		exitCh:     s.exitCh,
		waitUpload: s.waitUpload,

		sessionCh:      make(chan struct{}),
		sessionId:      sessionId,
		systemUserId:   s.conf.RoomMgmt.SystemUserIdPrefix + sdk.RandomKey(16),
		systemUsername: s.conf.RoomMgmt.SystemUsername,

		chopInterval:   time.Duration(s.conf.Recorder.ChoppedInSeconds) * time.Second,
		joinRoomCh:     make(chan struct{}, 32),
		isSdkConnected: true,

		sdkConnector: nil,
		roomService:  nil,
		roomRTC:      nil,
		stunServer:   s.conf.Stunserver.Urls,
	}
	go session.checkForRoomError()

	return session, nil
}

func (s *RoomRecorderSession) close() {
	log.Infof("Recording sessionId '%s' Ended", s.sessionId)
	s.sessionChOnce.Do((func() {
		time.Sleep(constants.RETRY_DELAY)
		close(s.sessionCh)
	}))
}

func (s *RoomRecorderSession) checkForRoomError() {
	s.joinRoomCh <- struct{}{}
	for {
		select {
		case <-s.sessionCh:
			return
		case <-s.exitCh:
			return
		case <-s.joinRoomCh:
			if s.isSdkConnected {
				s.isSdkConnected = false
				go s.openRoom()
			}
		}
	}
}

func (s *RoomRecorderSession) closeRoom() {
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

func (s *RoomRecorderSession) getRoomService() error {
	log.Infof("--- Connecting to Room Signal ---")
	log.Infof("attempt gRPC connection to %s", s.conf.Signal.Addr)
	sdkConnector := sdk.NewConnector(s.conf.Signal.Addr)
	if sdkConnector == nil {
		log.Errorf("connection to %s fail", s.conf.Signal.Addr)
		return errors.New("")
	}

	s.roomService = sdk.NewRoom(sdkConnector)
	s.roomRTC = sdk.NewRTC(sdkConnector,
		sdk.RTCConfig{
			WebRTC: sdk.WebRTCTransportConfig{
				Configuration: webrtc.Configuration{
					ICEServers: []webrtc.ICEServer{
						{
							URLs: s.conf.Stunserver.Urls,
						},
					},
				},
			},
		})

	return nil
}

func (s *RoomRecorderSession) openRoom() {
	err := s.getRoomsByRoomid()
	if err != nil {
		s.close()
		return
	}
	s.closeRoom()
	for {
		time.Sleep(constants.RECONNECTION_INTERVAL)
		err = s.getRoomService()
		if err == nil {
			s.isSdkConnected = true
			break
		}
	}
	s.joinRoom()
}

func (s *RoomRecorderSession) joinRoom() {
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
			Sid: s.sessionId,
			Uid: s.systemUserId,
		},
	)
	if err != nil {
		s.joinRoomCh <- struct{}{}
		return
	}

	log.Infof("room.Join ok roomid=%s", s.sessionId)
}

func (s *RoomRecorderSession) onRoomPeerEvent(state sdk.PeerState, peer sdk.PeerInfo) {
}

func (s *RoomRecorderSession) onRoomMessage(from string, to string, data map[string]interface{}) {
}

func (s *RoomRecorderSession) onRoomDisconnect(sid, reason string) {
	log.Infof("onRoomDisconnect sid = %+v, reason = %+v", sid, reason)
	s.joinRoomCh <- struct{}{}
}

func (s *RoomRecorderSession) onRoomError(err error) {
	log.Errorf("onRoomError %s", err)
	s.joinRoomCh <- struct{}{}
}

func (s *RoomRecorderSession) onRoomLeave(success bool, err error) {
}

func (s *RoomRecorderSession) onRoomInfo(info sdk.RoomInfo) {
}

func (s *RoomRecorderSession) onRoomJoin(success bool, info sdk.RoomInfo, err error) {
	log.Infof("onRoomJoin, success = %v, info = %v, err = %s", success, info, err)
	if !success || err != nil {
		s.joinRoomCh <- struct{}{}
		return
	}

	s.roomRTC.OnTrack = s.onRTCTrack
	s.roomRTC.OnTrackEvent = s.onRTCTrackEvent
	s.roomRTC.OnError = s.onRTCError
	s.roomRTC.OnDataChannel = s.onRTCDataChannel
	s.roomRTC.OnSpeaker = s.onRTCSpeaker

	err = s.roomRTC.Join(s.sessionId, s.systemUserId)
	if err != nil {
		s.joinRoomCh <- struct{}{}
		return
	}
	log.Infof("rtc.Join ok roomid=%s", s.sessionId)
}

type TrackEvent struct {
	peerId         string
	trackRemoteIds pq.StringArray
}

type Track struct {
	trackRemoteId string
	mimeType      string
	kind          webrtc.RTPCodecType
}

type TrackStream struct {
	Timestamp time.Time
	Data      []byte
}

func (s *RoomRecorderSession) onRTCTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	log.Infof("onRTCTrack track: %+v", track)

	trackCh := make(chan TrackStream, 128)
	var eofChOnce sync.Once
	eofCh := make(chan struct{})
	go s.insertTrackOnInterval(
		Track{
			track.ID(),
			track.Codec().MimeType,
			track.Kind()},
		trackCh,
		eofCh)

	// Send a PLI on an interval so that the publisher is pushing a keyframe every rtcpPLIInterval
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			rtcpSendErr := s.roomRTC.GetSubTransport().GetPeerConnection().WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())}})
			if rtcpSendErr != nil {
				log.Errorf("%s", rtcpSendErr)
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
				log.Errorf("%s", readErr)
			}
			eofChOnce.Do((func() {
				close(eofCh)
			}))
			return
		}
		if readCnt == 0 {
			continue
		}
		track := make([]byte, readCnt)
		copy(track, buf)
		trackCh <- TrackStream{time.Now(), track}
	}
}

func (s *RoomRecorderSession) onRTCTrackEvent(event sdk.TrackEvent) {
	log.Infof("onRTCTrackEvent: %+v", event)
	if event.State == sdk.TrackEvent_REMOVE {
		return
	}
	trackIds := make(pq.StringArray, 0)
	for _, track := range event.Tracks {
		log.Infof("onRTCTrackEvent: %+v", track)
		trackIds = append(trackIds, track.Id)
	}
	go s.insertTrackEvent(
		TrackEvent{
			event.Uid,
			trackIds})
}

func (s *RoomRecorderSession) onRTCError(err error) {
	log.Errorf("onRTCError: %+v", err)
	s.joinRoomCh <- struct{}{}
}

func (s *RoomRecorderSession) onRTCDataChannel(dc *webrtc.DataChannel) {
}

func (s *RoomRecorderSession) onRTCSpeaker(event []string) {
}

func (s *RoomRecorderSession) insertTrackEvent(trackEvent TrackEvent) {
	var err error
	insertStmt := `INSERT INTO "` + s.roomRecordSchema + `"."trackEvent"(
					"id",
					"roomId",
					"peerId",
					"trackRemoteIds")
					VALUES($1, $2, $3, $4)`
	s.waitUpload.Add(1)
	defer s.waitUpload.Done()
	trackEventId := uuid.NewString()
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			trackEventId,
			s.sessionId,
			trackEvent.peerId,
			trackEvent.trackRemoteIds)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), constants.DUP_PK) {
			trackEventId = uuid.NewString()
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
		return
	}
}

func (s *RoomRecorderSession) insertTrackOnInterval(
	track Track,
	trackCh chan TrackStream,
	eofCh chan struct{}) {

	s.waitUpload.Add(1)
	defer s.waitUpload.Done()
	dbId := s.insertTrack(track)
	trackId := track.trackRemoteId

	var folderName string
	if track.kind == webrtc.RTPCodecTypeAudio {
		folderName = constants.AUDIO_FOLDERNAME
	} else {
		folderName = constants.VIDEO_FOLDERNAME
	}
	track = Track{}

	trackSavedId := 0
	tracks := make([]TrackStream, 0)
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
		case <-s.sessionCh:
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

func (s *RoomRecorderSession) insertTrack(track Track) string {
	var err error
	insertStmt := `INSERT INTO "` + s.roomRecordSchema + `"."track"(
					"id",
					"roomId",
					"trackRemoteId",
					"mimeType")
					VALUES($1, $2, $3, $4)`
	dbId := uuid.NewString()
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			dbId,
			s.sessionId,
			track.trackRemoteId,
			track.mimeType)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), constants.DUP_PK) {
			dbId = uuid.NewString()
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
	}
	return dbId
}

func (s *RoomRecorderSession) insertTracks(
	tracks []TrackStream,
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
					"trackId",
					"roomId",
					"filePath")
					VALUES($1, $2, $3, $4)`
	objName := uuid.NewString()
	filePath := folderName + objName
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			objName,
			dbId,
			s.sessionId,
			filePath)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), constants.DUP_PK) {
			objName = uuid.NewString()
			filePath = folderName + objName
		}
		time.Sleep(constants.RETRY_DELAY)
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
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		uploadInfo, err = s.minioClient.PutObject(context.Background(),
			s.bucketName,
			s.sessionId+filePath,
			&data,
			int64(data.Len()),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not upload file: %s", err)
		return
	}
	for id := startId; id < endId; id++ {
		tracks[id].Data = nil
		tracks[id] = TrackStream{}
	}
	log.Infof("successfully uploaded bytes: ", uploadInfo)
}
