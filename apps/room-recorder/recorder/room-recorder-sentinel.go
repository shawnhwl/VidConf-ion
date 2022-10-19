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

	onTrackRecord := OnTrackRecord{time.Since(s.startTime),
		track.ID(),
		TrackRemote{
			track.ID(),
			track.StreamID(),
			track.PayloadType(),
			track.Kind(),
			track.SSRC(),
			track.Codec(),
			track.RID()},
		"",
		make([]TrackRecord, 0)}
	s.onTracks = append(s.onTracks, onTrackRecord)
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
	trackId := track.ID()
	s.tracksSavedId[trackId] = 0
	buf := make([]byte, 65536)
	for {
		readCnt, _, readErr := track.Read(buf)
		if readErr != nil {
			if readErr.Error() == "EOF" {
				log.Infof("End of stream")
			} else {
				log.Errorf("%v", readErr)
			}
			return
		}
		if readCnt == 0 {
			continue
		}
		onTrackRecord.tracks = append(onTrackRecord.tracks,
			TrackRecord{time.Since(s.startTime), buf[:readCnt]})
	}
}

func (s *RoomRecorder) onDataChannel(dc *webrtc.DataChannel) {
	// TBD onDataChannel
	log.Warnf("Not Implemented onDataChannel: %+v", dc)
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
	s.trackEvents = append(s.trackEvents,
		TrackEventRecord{time.Since(s.startTime),
			event.State,
			event.Uid,
			tracks})
}

func (s *RoomRecorder) onSpeaker(event []string) {
	// TBD onSpeaker
	log.Warnf("Not Implemented onSpeaker: %+v", event)
}

func (s *RoomRecorder) onJoin(success bool, info sdk.RoomInfo, err error) {
	log.Infof("OnJoin success = %v, info = %v, err = %v", success, info, err)

	s.roomRTC.OnTrack = s.onTrack
	s.roomRTC.OnDataChannel = s.onDataChannel
	s.roomRTC.OnError = s.onError
	s.roomRTC.OnTrackEvent = s.onTrackEvent
	s.roomRTC.OnSpeaker = s.onSpeaker

	err = s.roomRTC.Join(s.roomid, s.systemUid)
	if err != nil {
		log.Panicf("RTC join error: %v", err)
	}
	log.Infof("rtc.Join ok roomid=%v", s.roomid)
}

func (s *RoomRecorder) onPeerEvent(state sdk.PeerState, peer sdk.PeerInfo) {
	log.Infof("OnPeerEvent state = %+v, peer = %+v", state, peer)
	s.peerEvents = append(s.peerEvents,
		PeerEventRecord{time.Since(s.startTime),
			state,
			peer.Uid,
			peer.DisplayName})
}

func (s *RoomRecorder) onMessage(from string, to string, data map[string]interface{}) {
	log.Infof("OnMessage from = %+v, to = %+v, data = %+v", from, to, data)
	s.chats = append(s.chats, ChatRecord{time.Since(s.startTime), data})
}

func (s *RoomRecorder) onDisconnect(sid, reason string) {
	log.Infof("OnDisconnect sid = %+v, reason = %+v", sid, reason)
	s.quitCh <- os.Interrupt
}

func (s *RoomRecorder) joinRoom() {
	s.roomService.OnJoin = s.onJoin
	s.roomService.OnPeerEvent = s.onPeerEvent
	s.roomService.OnMessage = s.onMessage
	s.roomService.OnDisconnect = s.onDisconnect

	// join room
	err := s.roomService.Join(
		sdk.JoinInfo{
			Sid:         s.roomid,
			Uid:         s.systemUid,
			DisplayName: s.systemUsername,
		},
	)
	if err != nil {
		log.Panicf("Session join error: %v", err)
		return
	}

	s.roomRecordId = uuid.NewString()
	s.startTime = time.Now()
	insertStmt := `insert into "roomRecord"("id",
											"roomId",
											"startTime",
											"endTime")
					values($1, $2, $3, $4)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			s.roomRecordId,
			s.roomid,
			s.startTime,
			time.Now())
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
	log.Infof("room.Join ok roomid=%v", s.roomid)
}

func (s *RoomRecorder) RecorderSentinel() {
	log.Infof("RecorderSentinel Started")
	defer log.Infof("RecorderSentinel Ended")
	for {
		time.Sleep(s.chopInterval)
		s.RecordData()
	}
}

func (s *RoomRecorder) RecordData() {
	peerEventsSavedId := len(s.peerEvents)
	trackEventsSavedId := len(s.trackEvents)
	onTracksSavedId := len(s.onTracks)
	chatsSavedId := len(s.chats)
	tracksSavedId := make(map[string]int)
	for _, onTracks := range s.onTracks {
		tracksSavedId[onTracks.trackId] = len(onTracks.tracks)
	}

	go s.updateRoomRecord()
	for id := s.peerEventsSavedId; id < peerEventsSavedId; {
		go s.insertPeerEvent(id)
		id++
		s.peerEventsSavedId = id
	}
	for id := s.trackEventsSavedId; id < trackEventsSavedId; {
		go s.insertTrackEvent(id)
		id++
		s.trackEventsSavedId = id
	}
	for id := s.onTracksSavedId; id < onTracksSavedId; {
		go s.insertOnTracks(id)
		id++
		s.onTracksSavedId = id
	}

	go s.insertChats(s.chatsSavedId, chatsSavedId)
	s.chatsSavedId = chatsSavedId

	for key := range tracksSavedId {
		for id := s.tracksSavedId[key]; id < tracksSavedId[key]; {
			go s.insertTracks(key, s.chatsSavedId, chatsSavedId)
			s.tracksSavedId[key] = tracksSavedId[key]
		}
	}
}

func (s *RoomRecorder) updateRoomRecord() {
	updateStmt := `update "roomRecord" set "endTime"=$1 where "id"=$2`
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

func (s *RoomRecorder) insertPeerEvent(id int) {
	var err error
	insertStmt := `insert into "peerEvent"( "id",
											"roomRecordId",
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
			s.peerEvents[id].timeElapsed,
			s.peerEvents[id].state,
			s.peerEvents[id].peerId,
			s.peerEvents[id].peerName)
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

func (s *RoomRecorder) insertTrackEvent(id int) {
	var err error
	insertStmt := `insert into "trackEvent"("id",
											"roomRecordId",
											"timeElapsed",
											"state",
											"trackEventId")
					values($1, $2, $3, $4, $5)`
	trackEventId := uuid.NewString()
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			trackEventId,
			s.roomRecordId,
			s.trackEvents[id].timeElapsed,
			s.trackEvents[id].state,
			s.trackEvents[id].trackEventId)
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

	insertStmt = `insert into "trackEvent"( "id",
											"trackEventId",
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
	for _, trackInfo := range s.trackEvents[id].tracks {
		dbId := uuid.NewString()
		for retry := 0; retry < DB_RETRY; retry++ {
			_, err = s.postgresDB.Exec(insertStmt,
				dbId,
				trackEventId,
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
}

func (s *RoomRecorder) insertOnTracks(id int) {
	var err error
	trackRemote, _ := json.Marshal(s.onTracks[id].trackRemote)
	insertStmt := `insert into "onTrack"(   "id",
											"roomRecordId",
											"timeElapsed",
											"trackRemote")
					values($1, $2, $3, $4)`
	s.onTracks[id].dbId = uuid.NewString()
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			s.onTracks[id].dbId,
			s.roomRecordId,
			s.onTracks[id].timeElapsed,
			trackRemote)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			s.onTracks[id].dbId = uuid.NewString()
		}
	}
	if err != nil {
		log.Panicf("could not insert into database: %s", err)
	}
}

func (s *RoomRecorder) insertChats(startId, endId int) {
	var err error
	insertStmt := `insert into "chatStream"("id",
											"roomRecordId",
											"timeElapsed",
											"filepath")
					values($1, $2, $3, $4)`
	objName := uuid.NewString()
	filepath := s.folderName + objName
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			objName,
			s.roomRecordId,
			s.chats[startId].TimeElapsed,
			s.bucketName+filepath)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			objName = uuid.NewString()
			filepath = s.folderName + objName
		}
	}
	if err != nil {
		log.Panicf("could not insert into database: %s", err)
	}

	data := new(bytes.Buffer)
	err = json.NewEncoder(data).Encode(s.chats[startId:endId])
	if err != nil {
		log.Panicf("chat encoding error:", err)
	}
	uploadInfo, err := s.minioClient.PutObject(context.Background(),
		s.bucketName,
		filepath,
		data,
		int64(data.Len()),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Panicf("could not upload file: %s", err)
	}
	log.Infof("successfully uploaded bytes: ", uploadInfo)

	s.testDownload(filepath, "chat"+objName+".json")
}

func (s *RoomRecorder) insertTracks(key string, startId, endId int) {
	if startId >= endId {
		return
	}
	var trackId int
	for id := range s.onTracks {
		if s.onTracks[id].trackId == key {
			trackId = id
			break
		}
	}
	var err error
	insertStmt := `insert into "trackStream"(   "id",
												"onTrackId",
												"timeElapsed",
												"filepath")
					values($1, $2, $3, $4)`
	objName := uuid.NewString()
	filepath := s.folderName + objName
	for {
		if s.onTracks[trackId].dbId != "" {
			break
		}
		time.Sleep(time.Second)
	}
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			objName,
			s.onTracks[trackId].dbId,
			s.onTracks[trackId].tracks[startId].TimeElapsed,
			s.bucketName+filepath)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			objName = uuid.NewString()
			filepath = s.folderName + objName
		}
	}
	if err != nil {
		log.Panicf("could not insert into database: %s", err)
	}

	data := new(bytes.Buffer)
	err = gob.NewEncoder(data).Encode(s.onTracks[trackId].tracks[startId:endId])
	if err != nil {
		log.Panicf("track encoding error:", err)
	}
	uploadInfo, err := s.minioClient.PutObject(context.Background(),
		s.bucketName,
		filepath,
		data,
		int64(data.Len()),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Panicf("could not upload file: %s", err)
	}
	log.Infof("successfully uploaded bytes: ", uploadInfo)

	s.testDownload(filepath, "track"+objName)
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
