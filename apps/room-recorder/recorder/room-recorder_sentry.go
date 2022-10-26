package recorder

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
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

func (s *RoomRecorder) onTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	log.Infof("onTrack track: %+v", track)
	log.Infof("onTrack receiver: %+v", receiver)

	trackCh := make(chan Track, 128)
	quitCh := make(chan bool)
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
		quitCh)

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
			quitCh <- true
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

func (s *RoomRecorder) onError(err error) {
	log.Errorf("onError: %+v", err)
}

func (s *RoomRecorder) onTrackEvent(event sdk.TrackEvent) {
	log.Infof("onTrackEvent: %+v", event)
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

func (s *RoomRecorder) onJoin(success bool, info sdk.RoomInfo, err error) {
	log.Infof("OnJoin success = %v, info = %v, err = %v", success, info, err)

	s.roomRTC.OnTrack = s.onTrack
	s.roomRTC.OnDataChannel = s.onDataChannel
	s.roomRTC.OnError = s.onError
	s.roomRTC.OnTrackEvent = s.onTrackEvent
	s.roomRTC.OnSpeaker = s.onSpeaker

	err = s.roomRTC.Join(s.roomId, s.systemUid)
	if err != nil {
		log.Panicf("RTC join error: %v", err)
	}
	log.Infof("rtc.Join ok roomid=%v", s.roomId)
}

func (s *RoomRecorder) onPeerEvent(state sdk.PeerState, peer sdk.PeerInfo) {
	log.Infof("OnPeerEvent state = %+v, peer = %+v", state, peer)
	go s.insertPeerEvent(
		PeerEvent{
			time.Since(s.startTime),
			state,
			peer.Uid,
			peer.DisplayName})
}

func (s *RoomRecorder) onMessage(from string, to string, data map[string]interface{}) {
	log.Infof("OnMessage from = %+v, to = %+v, data = %+v", from, to, data)
	go s.insertChats(Chat{time.Since(s.startTime), data})
}

func (s *RoomRecorder) onDisconnect(sid, reason string) {
	log.Infof("OnDisconnect sid = %+v, reason = %+v", sid, reason)
	s.quitCh <- os.Interrupt
}

func (s *RoomRecorder) onRoomError(err error) {
	log.Errorf("onRoomError %v", err)
	time.Sleep(time.Second)
	s.quitCh <- os.Interrupt
}

// TBD onLeave
func (s *RoomRecorder) onLeave(success bool, err error) {
	log.Warnf("Not Implemented onDataChannel: success %v, onLeave %v", success, err)
}

// TBD onDataChannel
func (s *RoomRecorder) onDataChannel(dc *webrtc.DataChannel) {
	log.Warnf("Not Implemented onDataChannel: %+v", dc)
}

// TBD onSpeaker
func (s *RoomRecorder) onSpeaker(event []string) {
	log.Warnf("Not Implemented onSpeaker: %+v", event)
}

func (s *RoomRecorder) joinRoom() {
	var err error
	s.roomRecordId = uuid.NewString()
	s.startTime = time.Now()
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
		log.Panicf("could not insert into database: %s", err)
		return
	}

	s.roomService.OnJoin = s.onJoin
	s.roomService.OnPeerEvent = s.onPeerEvent
	s.roomService.OnMessage = s.onMessage
	s.roomService.OnDisconnect = s.onDisconnect
	s.roomService.OnError = s.onRoomError
	s.roomService.OnLeave = s.onLeave

	// join room
	err = s.roomService.Join(
		sdk.JoinInfo{
			Sid:         s.roomId,
			Uid:         s.systemUid,
			DisplayName: s.systemUsername,
		},
	)
	if err != nil {
		deleteStmt := `delete from "` + s.roomRecordSchema + `"."roomRecord" where "id"=$1`
		for retry := 0; retry < DB_RETRY; retry++ {
			_, err = s.postgresDB.Exec(deleteStmt, s.roomRecordId)
			if err == nil {
				break
			}
		}
		if err != nil {
			log.Errorf("could not delete from database: %s", err)
		}
		log.Panicf("Session join error: %v", err)
		return
	}

	log.Infof("room.Join ok roomid=%v", s.roomId)
	go s.roomRecorderSentry()
}

func (s *RoomRecorder) UpdateRoomRecord() {
	updateStmt := `update "` + s.roomRecordSchema + `"."roomRecord" set "endTime"=$1 where "id"=$2`
	var err error
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(updateStmt, time.Now(), s.roomRecordId)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("could not update database: %s", err)
	}
}

func (s *RoomRecorder) roomRecorderSentry() {
	for {
		time.Sleep(s.chopInterval)
		s.UpdateRoomRecord()
	}
}

func (s *RoomRecorder) insertTrackEvent(trackEvent TrackEvent) {
	var err error
	insertStmt := `insert into "` + s.roomRecordSchema + `"."trackEvent"(
					"id",
					"roomRecordId",
					"roomId",
					"timeElapsed",
					"state",
					"trackEventId")
					values($1, $2, $3, $4, $5)`
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
		log.Panicf("could not insert into database: %s", err)
	}

	insertStmt = `insert into "` + s.roomRecordSchema + `"."trackEvent"(
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
					values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`
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
			log.Panicf("could not insert into database: %s", err)
		}
	}
	for id := range trackEvent.tracks {
		trackEvent.tracks[id] = sdk.TrackInfo{}
	}
	trackEvent = TrackEvent{}
}

func (s *RoomRecorder) insertPeerEvent(peerEvent PeerEvent) {
	var err error
	insertStmt := `insert into "` + s.roomRecordSchema + `"."peerEvent"(
					"id",
					"roomRecordId",
					"roomId",
					"timeElapsed",
					"state",
					"peerId",
					"peerName")
					values($1, $2, $3, $4, $5, $6)`
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
		log.Panicf("could not insert into database: %s", err)
	}
	peerEvent = PeerEvent{}
}

func (s *RoomRecorder) insertTracksOnInterval(
	onTrack OnTrack,
	trackCh chan Track,
	quitCh chan bool) {

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
		case <-quitCh:
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

func (s *RoomRecorder) insertOnTracks(onTrack OnTrack) string {
	var err error
	trackRemote, _ := json.Marshal(onTrack.trackRemote)
	insertStmt := `insert into "` + s.roomRecordSchema + `"."onTrack"(
					"id",
					"roomRecordId",
					"roomId",
					"timeElapsed",
					"trackRemote")
					values($1, $2, $3, $4)`
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
		log.Panicf("could not insert into database: %s", err)
	}
	return dbId
}

func (s *RoomRecorder) insertTracks(
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
					"filepath")
					values($1, $2, $3, $4)`
	objName := uuid.NewString()
	filepath := subFolderName + objName
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			objName,
			dbId,
			s.roomId,
			tracks[startId].TimeElapsed,
			filepath)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			objName = uuid.NewString()
			filepath = subFolderName + objName
		}
	}
	if err != nil {
		log.Panicf("could not insert into database: %s", err)
	}

	data := new(bytes.Buffer)
	err = gob.NewEncoder(data).Encode(tracks[startId:endId])
	if err != nil {
		log.Panicf("track encoding error:", err)
	}
	var uploadInfo minio.UploadInfo
	for retry := 0; retry < DB_RETRY; retry++ {
		uploadInfo, err = s.minioClient.PutObject(context.Background(),
			s.bucketName,
			s.folderName+filepath,
			data,
			int64(data.Len()),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("could not upload file: %s", err)
	}
	for id := startId; id < endId; id++ {
		tracks[id].Data = nil
		tracks[id] = Track{}
	}
	log.Infof("successfully uploaded bytes: ", uploadInfo)

	s.testDownload(s.folderName+filepath, "track"+objName)
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

func (s *RoomRecorder) insertChats(chat Chat) {
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
		log.Errorf("chat message has no timestamp")
		return
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

func (s *RoomRecorder) insertChatText(chat Chat, chatPayload ChatPayload) {
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
					values($1, $2, $3, $4, $5, $6, $7)`
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
		log.Panicf("could not insert into database: %s", err)
	}
}

func (s *RoomRecorder) insertChatFile(chat Chat, chatPayload ChatPayload) {
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
					values($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	objName := uuid.NewString()
	filepath := s.attachmentFolderName + objName
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
			filepath,
			*chatPayload.Msg.Timestamp)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			objName = uuid.NewString()
			filepath = s.attachmentFolderName + objName
		}
	}
	if err != nil {
		log.Panicf("could not insert into database: %s", err)
	}

	data := bytes.NewReader([]byte(*chatPayload.Msg.Base64File.Data))
	var uploadInfo minio.UploadInfo
	for retry := 0; retry < DB_RETRY; retry++ {
		uploadInfo, err = s.minioClient.PutObject(context.Background(),
			s.bucketName,
			s.folderName+filepath,
			data,
			int64(len(*chatPayload.Msg.Base64File.Data)),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("could not upload file: %s", err)
	}
	log.Infof("successfully uploaded bytes: ", uploadInfo)

	s.testDownload(s.folderName+filepath, "chat"+objName+".json")
}

func (s *RoomRecorder) testDownload(filepath, filename string) {
	object, err := s.minioClient.GetObject(context.Background(),
		s.bucketName,
		filepath,
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

func (s *RoomRecorder) testUpload(filepath, filename string) {
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
		filepath,
		file,
		fileStat.Size(),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Errorf("error uploading file: %s", err)
		return
	}
	log.Infof("Successfully uploaded: %+v", uploadInfo)
}
