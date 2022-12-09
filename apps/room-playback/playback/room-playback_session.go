package playback

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
	"github.com/pion/webrtc/v3"
)

type Ctrl struct {
	isPause         bool
	speed10         time.Duration
	playFrom        int
	isChat          bool
	isVideo         bool
	isAudio         bool
	actualRefTime   time.Time
	playbackRefTime time.Time
}

func (s *RoomPlaybackSession) onPlaybackPause(msg *nats.Msg) {
	log.Infof("onSubcribe %s", msg.Subject)
	s.ctrlCh <- Ctrl{
		isPause: true,
	}
	msg.Respond([]byte(""))
}

func (s *RoomPlaybackSession) onPlayback(msg *nats.Msg) {
	data := strings.Split(string(msg.Data), "/")
	speed := data[0]
	playfrom := data[1]
	chat := data[2]
	video := data[3]
	audio := data[4]
	log.Infof("onSubcribe %s %s/%s/%s/%s/%s", msg.Subject, speed, playfrom, chat, video, audio)
	speedf, err := strconv.ParseFloat(speed, 64)
	if err != nil {
		errorString := fmt.Sprintf("non-float speedParam '%s'", speed)
		msg.Respond([]byte(errorString))
		return
	}
	playfromd, err := strconv.Atoi(playfrom)
	if err != nil {
		errorString := fmt.Sprintf("non-integer playfromParam '%s'", playfrom)
		msg.Respond([]byte(errorString))
		return
	}
	chatb, err := strconv.ParseBool(chat)
	if err != nil {
		errorString := fmt.Sprintf("non-boolean chatParam '%s'", chat)
		msg.Respond([]byte(errorString))
		return
	}
	videob, err := strconv.ParseBool(video)
	if err != nil {
		errorString := fmt.Sprintf("non-boolean videoParam '%s'", video)
		msg.Respond([]byte(errorString))
		return
	}
	audiob, err := strconv.ParseBool(audio)
	if err != nil {
		errorString := fmt.Sprintf("non-boolean audioParam '%s'", audio)
		msg.Respond([]byte(errorString))
		return
	}

	s.ctrlCh <- Ctrl{
		isPause:  false,
		speed10:  time.Duration(speedf * 10),
		playFrom: playfromd,
		isChat:   chatb,
		isVideo:  videob,
		isAudio:  audiob,
	}
	msg.Respond([]byte(""))
}

type ChatPayload struct {
	Uid        string      `json:"uid"`
	Name       string      `json:"name"`
	MimeType   string      `json:"mimeType"`
	Text       *string     `json:"text,omitempty"`
	Timestamp  time.Time   `json:"timestamp"`
	Base64File *Attachment `json:"base64File,omitempty"`
}

type Attachment struct {
	Name string `json:"name"`
	Size int    `json:"size"`
	Data string `json:"data"`
}

type ChatPayloads []ChatPayload

func (p ChatPayloads) Len() int {
	return len(p)
}

func (p ChatPayloads) Less(i, j int) bool {
	return p[i].Timestamp.Before(p[j].Timestamp)
}

func (p ChatPayloads) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// RoomPlaybackSession represents a room-playback session instance
type RoomPlaybackSession struct {
	conf     Config
	natsConn *nats.Conn

	postgresDB       *sql.DB
	roomMgmtSchema   string
	roomRecordSchema string

	minioClient *minio.Client
	bucketName  string

	sessionChOnce      sync.Once
	sessionCh          chan struct{}
	sessionId          string
	systemUserIdPrefix string
	systemUsername     string
	ctrlCh             chan Ctrl

	joinRoomCh     chan struct{}
	isSdkConnected bool

	sdkConnector *sdk.Connector
	roomService  *sdk.Room

	roomId        string
	roomStartTime time.Time

	chatPayloads    ChatPayloads
	lenChatPayloads int
	chatId          int

	waitPeer  *sync.WaitGroup
	isRunning bool
	peers     []*PlaybackPeer

	speed10         time.Duration
	isChat          bool
	isVideo         bool
	isAudio         bool
	playbackRefTime time.Time
	actualRefTime   time.Time
}

func (s *RoomPlaybackService) newRoomPlaybackSession(sessionId string) (*RoomPlaybackSession, error) {
	if !strings.HasPrefix(sessionId, s.conf.RoomMgmt.PlaybackIdPrefix) {
		errorString := fmt.Sprintf("'%s' is not a playback session", sessionId)
		log.Errorf(errorString)
		return nil, errors.New(errorString)
	}
	session := &RoomPlaybackSession{
		conf:     s.conf,
		natsConn: s.natsConn,

		postgresDB:       s.postgresDB,
		roomMgmtSchema:   s.conf.Postgres.RoomMgmtSchema,
		roomRecordSchema: s.conf.Postgres.RoomRecordSchema,

		minioClient: s.minioClient,
		bucketName:  s.conf.Minio.BucketName,

		sessionCh:          make(chan struct{}),
		sessionId:          sessionId,
		systemUserIdPrefix: s.conf.RoomMgmt.SystemUserIdPrefix,
		systemUsername:     s.conf.RoomMgmt.SystemUsername,
		ctrlCh:             make(chan Ctrl, 32),

		joinRoomCh:     make(chan struct{}, 32),
		isSdkConnected: true,

		sdkConnector: nil,
		roomService:  nil,

		chatPayloads:    make(ChatPayloads, 0),
		lenChatPayloads: 0,
		chatId:          0,

		waitPeer:  new(sync.WaitGroup),
		isRunning: false,
		peers:     make([]*PlaybackPeer, 0),
	}
	err := session.getRoomByPlaybackId()
	if err != nil {
		return nil, err
	}
	return session, nil
}

func (s *RoomPlaybackSession) run() {
	err := s.preparePlayback()
	if err != nil {
		s.close()
		return
	}
	go s.start()
	if s.conf.Playback.CheckForEmptyRoom {
		go s.checkForEmptyRoom()
	}
	go s.checkForRoomError()
	go s.playbackSentry()

}

func (s *RoomPlaybackSession) close() {
	log.Infof("Playback sessionId '%s' Ended", s.sessionId)
	s.sessionChOnce.Do((func() {
		time.Sleep(constants.RETRY_DELAY)
		close(s.sessionCh)
	}))
}

func (s *RoomPlaybackSession) start() {
	log.Infof("--- Starting NATS Subscription ---")
	onPlaybackPauseSub, err := s.natsConn.Subscribe(constants.PAUSEPLAYBACK_TOPIC+s.sessionId, s.onPlaybackPause)
	if err != nil {
		log.Errorf("NATS Subscription error: %s", err)
		s.sessionCh <- struct{}{}
		return
	}
	defer onPlaybackPauseSub.Unsubscribe()
	onPlaybackSub, err := s.natsConn.Subscribe(constants.PLAYBACK_TOPIC+s.sessionId, s.onPlayback)
	if err != nil {
		log.Errorf("NATS Subscription error: %s", err)
		s.sessionCh <- struct{}{}
		return
	}
	defer onPlaybackSub.Unsubscribe()
	<-s.sessionCh
}

func (s *RoomPlaybackSession) checkForEmptyRoom() {
	time.Sleep(constants.ROOMEMPTYWAIT_INTERVAL)
	for {
		select {
		case <-s.sessionCh:
			return
		default:
			time.Sleep(constants.ROOMEMPTY_INTERVAL)
			if s.roomService != nil {
				peers := s.roomService.GetPeers(s.sessionId)
				peerCount := 0
				for _, peer := range peers {
					if strings.HasPrefix(peer.Uid, s.systemUserIdPrefix) {
						continue
					}
					if strings.HasPrefix(peer.Uid, constants.PLAYBACK_PREFIX) {
						continue
					}
					peerCount++
				}
				if peerCount == 0 {
					log.Warnf("Deleting playbackId '%s' since no viewers left", s.sessionId)
					err := s.natsPublish(constants.DELETEPLAYBACK_TOPIC+s.sessionId, nil)
					if err == nil {
						return
					}
				}
			}
		}
	}
}

func (s *RoomPlaybackSession) checkForRoomError() {
	s.joinRoomCh <- struct{}{}
	for {
		select {
		case <-s.sessionCh:
			return
		case <-s.joinRoomCh:
			if s.isSdkConnected {
				s.isSdkConnected = false
				go s.openRoom()
			}
		}
	}
}

func (s *RoomPlaybackSession) playbackSentry() {
	s.waitPeer.Wait()
	s.speed10 = constants.PLAYBACK_SPEED10
	s.isChat = true
	s.isVideo = true
	s.isAudio = true
	s.playbackRefTime = s.roomStartTime
	s.actualRefTime = time.Now().Add(time.Second)
	for id := range s.peers {
		s.peers[id].ctrlCh <- Ctrl{
			isPause:         false,
			speed10:         s.speed10,
			isVideo:         s.isVideo,
			isAudio:         s.isAudio,
			playbackRefTime: s.playbackRefTime,
			actualRefTime:   s.actualRefTime,
		}
	}
	s.chatId = 0
	s.isRunning = true

	for {
		select {
		case <-s.sessionCh:
			return
		case ctrl := <-s.ctrlCh:
			s.playbackCtrl(ctrl)
		default:
			time.Sleep(time.Nanosecond)
			s.playbackChat()
		}
	}
}

func getRoomService(conf Config) (*sdk.Connector, *sdk.Room, *sdk.RTC, error) {
	log.Infof("--- Connecting to Room Signal ---")
	log.Infof("attempt gRPC connection to %s", conf.Signal.Addr)
	sdkConnector := sdk.NewConnector(conf.Signal.Addr)
	if sdkConnector == nil {
		log.Errorf("connection to %s fail", conf.Signal.Addr)
		return nil, nil, nil, errors.New("")
	}
	roomService := sdk.NewRoom(sdkConnector)
	roomRTC := sdk.NewRTC(sdkConnector,
		sdk.RTCConfig{
			WebRTC: sdk.WebRTCTransportConfig{
				Configuration: webrtc.Configuration{
					ICEServers: []webrtc.ICEServer{
						{
							URLs: conf.Stunserver.Urls,
						},
					},
				},
			},
		})

	return sdkConnector, roomService, roomRTC, nil
}

func (s *RoomPlaybackSession) preparePlayback() error {
	log.Infof("preparePlayback started")

	var err error
	var queryStmt string
	// find peers media
	peerNames := make(map[string]string)
	var peerRows *sql.Rows
	queryStmt = `SELECT 	"peerId",
							"peerName"
					FROM "` + s.roomRecordSchema + `"."peerEvent"
					WHERE "roomId"=$1 AND "state"=$2`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		peerRows, err = s.postgresDB.Query(queryStmt, s.roomId, sdk.PeerState_JOIN)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		return err
	}
	defer peerRows.Close()
	for peerRows.Next() {
		var peerId string
		var peerName string
		err := peerRows.Scan(&peerId, &peerName)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			return err
		}
		peerNames[peerId] = peerName
	}
	log.Infof("peers found: %d", len(peerNames))
	for peerId := range peerNames {
		newPeer := s.NewPlaybackPeer(peerId, peerNames[peerId], "")
		if newPeer != nil {
			s.peers = append(s.peers, newPeer)
		}
	}

	// find screen share media
	orphanRemoteIds := make(map[string]struct{})
	var trackRows *sql.Rows
	queryStmt = `SELECT "trackRemoteId"
					FROM "` + s.roomRecordSchema + `"."track" WHERE "roomId"=$1`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		trackRows, err = s.postgresDB.Query(queryStmt, s.roomId)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		return err
	}
	defer trackRows.Close()
	for trackRows.Next() {
		var trackRemoteId string
		err := trackRows.Scan(&trackRemoteId)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			return err
		}
		orphanRemoteIds[trackRemoteId] = struct{}{}
	}
	var trackEventRows *sql.Rows
	queryStmt = `SELECT "trackRemoteIds"
					FROM "` + s.roomRecordSchema + `"."trackEvent" WHERE "roomId"=$1`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		trackEventRows, err = s.postgresDB.Query(queryStmt, s.roomId)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		return err
	}
	defer trackEventRows.Close()
	for trackEventRows.Next() {
		var trackRemoteIds pq.StringArray
		err := trackEventRows.Scan(&trackRemoteIds)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			return err
		}
		for _, trackRemoteId := range trackRemoteIds {
			delete(orphanRemoteIds, trackRemoteId)
		}
	}
	for orphanRemoteId := range orphanRemoteIds {
		newPeer := s.NewPlaybackPeer(uuid.NewString(), "Screen-Share", orphanRemoteId)
		if newPeer != nil {
			s.peers = append(s.peers, newPeer)
		}
	}

	s.chatPayloads, err = s.getChats(s.roomId)
	if err != nil {
		return err
	}

	s.lenChatPayloads = len(s.chatPayloads)
	log.Infof("preparePlayback completed")
	return nil
}

func (s *RoomPlaybackSession) getChats(roomId string) (ChatPayloads, error) {
	var err error
	chats := make(ChatPayloads, 0)
	var chatrows *sql.Rows
	queryStmt := `SELECT "userId",
						 "userName",
						 "mimeType",
						 "timestamp",
						 "text",
						 "fileName",
						 "fileSize",
						 "filePath"
					 FROM "` + s.roomRecordSchema + `"."chat" WHERE "roomId"=$1`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		chatrows, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		return nil, err
	}
	defer chatrows.Close()
	for chatrows.Next() {
		var chat ChatPayload
		var userId string
		var name string
		var text string
		var fileinfo Attachment
		var filePath string
		err := chatrows.Scan(&userId,
			&name,
			&chat.MimeType,
			&chat.Timestamp,
			&text,
			&fileinfo.Name,
			&fileinfo.Size,
			&filePath)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			return nil, err
		}
		if text != "" {
			chat.Text = &text
		}
		if filePath != "" {
			object, err := s.minioClient.GetObject(context.Background(),
				s.bucketName,
				roomId+filePath,
				minio.GetObjectOptions{})
			if err != nil {
				log.Errorf("could not download attachment: %s", err)
				continue
			}
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(object)
			if err != nil {
				log.Errorf("could not process download: %s", err)
				continue
			}
			fileinfo.Data = buf.String()
			chat.Base64File = &fileinfo
		}
		chat.Uid = constants.PLAYBACK_PREFIX + userId
		chat.Name = constants.PLAYBACK_PREFIX + name
		chats = append(chats, chat)
	}

	sort.Sort(chats)
	return chats, nil
}

func (s *RoomPlaybackSession) pausePlayback(ctrl Ctrl) {
	if s.isRunning {
		s.playbackRefTime = s.playbackRefTime.Add(time.Since(s.actualRefTime) * s.speed10 / 10)
		ctrl.isPause = true
		for id := range s.peers {
			s.peers[id].ctrlCh <- ctrl
		}
		s.isRunning = false
	}
}

func (s *RoomPlaybackSession) playbackCtrl(ctrl Ctrl) {
	if ctrl.isPause {
		s.pausePlayback(ctrl)
		return
	}
	if s.isRunning {
		if s.speed10 == ctrl.speed10 &&
			s.isChat == ctrl.isChat &&
			s.isVideo == ctrl.isVideo &&
			s.isAudio == ctrl.isAudio &&
			ctrl.playFrom == -1 {
			return
		}
		s.pausePlayback(ctrl)
	}
	if ctrl.playFrom >= 0 {
		s.playbackRefTime = s.roomStartTime.Add(time.Duration(ctrl.playFrom) * time.Second)
		if ctrl.isChat {
			s.batchSendChat()
		}
	} else if !s.isChat && ctrl.isChat {
		s.batchSendChat()
	}
	s.speed10 = ctrl.speed10
	s.isChat = ctrl.isChat
	s.isVideo = ctrl.isVideo
	s.isAudio = ctrl.isAudio
	s.actualRefTime = time.Now().Add(time.Second)
	for id := range s.peers {
		s.peers[id].ctrlCh <- Ctrl{
			isPause:         false,
			speed10:         s.speed10,
			isVideo:         s.isVideo,
			isAudio:         s.isAudio,
			playbackRefTime: s.playbackRefTime,
			actualRefTime:   s.actualRefTime,
		}
	}
	s.isRunning = true
}

func (s *RoomPlaybackSession) batchSendChat() {
	chatId := 0
	for ; chatId < s.lenChatPayloads; chatId++ {
		if s.chatPayloads[chatId].Timestamp.After(s.playbackRefTime) {
			break
		}
		time.Sleep(50 * time.Millisecond)
		s.sendChat(s.chatPayloads[chatId])
	}
	s.chatId = chatId
}

func (s *RoomPlaybackSession) sendChat(chatPayload ChatPayload) {
	var err error
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		err := s.roomService.SendMessage(s.sessionId,
			s.systemUserIdPrefix,
			"all",
			map[string]interface{}{"msg": chatPayload})
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Errorf("Error playback chat to playbackId '%s' : %s", s.sessionId, err)
		return
	}
}

func (s *RoomPlaybackSession) playbackChat() {
	if !s.isRunning || !s.isChat || s.chatId >= s.lenChatPayloads {
		return
	}

	if s.speed10*time.Since(s.actualRefTime) >
		constants.PLAYBACK_SPEED10*s.chatPayloads[s.chatId].Timestamp.Sub(s.playbackRefTime) {
		s.sendChat(s.chatPayloads[s.chatId])
		s.chatId++
	}
}

func (s *RoomPlaybackSession) natsPublish(topic string, data []byte) error {
	log.Infof("publishing to topic '%s'", topic)
	var resp *nats.Msg
	var err error
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		resp, err = s.natsConn.Request(topic, data, constants.RETRY_DELAY)
		if err == nil && string(resp.Data) == "" {
			break
		}
	}
	if err != nil {
		log.Errorf("error publishing topic '%s': %s", topic, err)
		return err
	}
	if string(resp.Data) != "" {
		log.Errorf("error publishing topic '%s': %s", topic, string(resp.Data))
		return errors.New(string(resp.Data))
	}
	return nil
}

func (s *RoomPlaybackSession) closeRoom() {
	if s.roomService != nil {
		s.roomService = nil
	}
	if s.sdkConnector != nil {
		s.sdkConnector = nil
	}
}

func (s *RoomPlaybackSession) openRoom() {
	err := s.getRoomByPlaybackId()
	if err != nil {
		s.close()
		return
	}
	s.closeRoom()
	for {
		time.Sleep(constants.RECONNECTION_INTERVAL)
		s.sdkConnector, s.roomService, _, err = getRoomService(s.conf)
		if err == nil {
			s.isSdkConnected = true
			break
		}
	}
	s.joinRoom()
}

func (s *RoomPlaybackSession) joinRoom() {
	log.Infof("--- Joining Room ---")
	var err error
	s.roomService.OnJoin = s.onRoomJoin
	s.roomService.OnPeerEvent = s.onRoomPeerEvent
	s.roomService.OnMessage = s.onRoomMessage
	s.roomService.OnDisconnect = s.onRoomDisconnect
	s.roomService.OnError = s.onRoomError
	s.roomService.OnLeave = s.onRoomLeave
	s.roomService.OnRoomInfo = s.onRoomInfo

	err = s.roomService.Join(
		sdk.JoinInfo{
			Sid: s.sessionId,
			Uid: s.systemUserIdPrefix + sdk.RandomKey(16),
		},
	)
	if err != nil {
		s.joinRoomCh <- struct{}{}
		return
	}

	log.Infof("room.Join ok roomid=%s", s.sessionId)
}

func (s *RoomPlaybackSession) onRoomJoin(success bool, info sdk.RoomInfo, err error) {
	log.Infof("onRoomJoin success = %v, info = %v, err = %s", success, info, err)
}

func (s *RoomPlaybackSession) onRoomPeerEvent(state sdk.PeerState, peer sdk.PeerInfo) {
}

func (s *RoomPlaybackSession) onRoomMessage(from string, to string, data map[string]interface{}) {
}

func (s *RoomPlaybackSession) onRoomDisconnect(sid, reason string) {
	log.Infof("onRoomDisconnect sid = %+v, reason = %+v", sid, reason)
	s.joinRoomCh <- struct{}{}
}

func (s *RoomPlaybackSession) onRoomError(err error) {
	log.Errorf("onRoomError %s", err)
	s.joinRoomCh <- struct{}{}
}

func (s *RoomPlaybackSession) onRoomLeave(success bool, err error) {
}

func (s *RoomPlaybackSession) onRoomInfo(info sdk.RoomInfo) {
}
