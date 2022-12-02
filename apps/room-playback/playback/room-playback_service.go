package playback

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	minioService "github.com/pion/ion/apps/minio"
	postgresService "github.com/pion/ion/apps/postgres"
	"github.com/pion/webrtc/v3"
)

const (
	DELETEPLAYBACK_TOPIC string = "playbackDelete."
	PAUSEPLAYBACK_TOPIC  string = "playbackPause."
	PLAYBACK_TOPIC       string = "playback."

	NOT_READY string = "service is not yet ready"

	RETRY_COUNT  int           = 3
	RETRY_DELAY  time.Duration = 5 * time.Second
	NOT_FOUND_PK string        = "no rows in result set"

	RECONNECTION_INTERVAL  time.Duration = 2 * time.Second
	ROOMEMPTY_INTERVAL     time.Duration = time.Minute
	ROOMEMPTYWAIT_INTERVAL time.Duration = 10 * time.Minute

	PLAYBACK_PREFIX     string = "[RECORDED]"
	LEN_PLAYBACK_PREFIX int    = len(PLAYBACK_PREFIX)

	PLAYBACK_SPEED10 time.Duration = 10

	MIME_VIDEO string = "VIDEO"
	MIME_AUDIO string = "AUDIO"
	MIME_VP8   string = webrtc.MimeTypeVP8
)

func (s *RoomPlaybackService) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomPlaybackService) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func (s *RoomPlaybackService) onPlaybackPause(msg *nats.Msg) {
	log.Infof("onSubcribe %s", msg.Subject)
	if s.timeReady == "" {
		msg.Respond([]byte(NOT_READY))
		return
	}

	s.ctrlCh <- Ctrl{
		isPause: true,
	}
	msg.Respond([]byte(""))
}

// err = s.natsPublish(PLAYBACK_TOPIC+playbackId, []byte(speed+"/"+playfrom+"/"+chat+"/"+video+"/"+audio))

func (s *RoomPlaybackService) onPlayback(msg *nats.Msg) {
	data := strings.Split(string(msg.Data), "/")
	speed := data[0]
	playfrom := data[1]
	chat := data[2]
	video := data[3]
	audio := data[4]
	log.Infof("onSubcribe %s %s/%s/%s/%s/%s", msg.Subject, speed, playfrom, chat, video, audio)
	if s.timeReady == "" {
		msg.Respond([]byte(NOT_READY))
		return
	}

	speedf, err := strconv.ParseFloat(speed, 64)
	if err != nil {
		errorsttring := fmt.Sprintf("non-float speedParam '%s'", speed)
		msg.Respond([]byte(errorsttring))
		return
	}
	playfromd, err := strconv.Atoi(playfrom)
	if err != nil {
		errorsttring := fmt.Sprintf("non-integer playfromParam '%s'", playfrom)
		msg.Respond([]byte(errorsttring))
		return
	}
	chatb, err := strconv.ParseBool(chat)
	if err != nil {
		errorsttring := fmt.Sprintf("non-boolean chatParam '%s'", chat)
		msg.Respond([]byte(errorsttring))
		return
	}
	videob, err := strconv.ParseBool(video)
	if err != nil {
		errorsttring := fmt.Sprintf("non-boolean videoParam '%s'", video)
		msg.Respond([]byte(errorsttring))
		return
	}
	audiob, err := strconv.ParseBool(audio)
	if err != nil {
		errorsttring := fmt.Sprintf("non-boolean audioParam '%s'", audio)
		msg.Respond([]byte(errorsttring))
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

// RoomPlaybackService represents a room-playback instance
type RoomPlaybackService struct {
	conf     Config
	natsConn *nats.Conn
	quitCh   chan os.Signal

	joinRoomCh     chan struct{}
	isSdkConnected bool
	ctrlCh         chan Ctrl

	timeLive  string
	timeReady string

	sdkConnector *sdk.Connector
	roomService  *sdk.Room

	postgresDB       *sql.DB
	roomMgmtSchema   string
	roomRecordSchema string

	minioClient *minio.Client
	bucketName  string

	sessionId          string
	systemUserIdPrefix string
	systemUsername     string

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

func NewRoomPlaybackService(config Config, natsConn *nats.Conn, quitCh chan os.Signal) *RoomPlaybackService {
	if !strings.HasPrefix(config.RoomMgmt.SessionId, config.RoomMgmt.PlaybackIdPrefix) {
		log.Infof("is not a playback session, exiting")
		os.Exit(0)
	}
	timeLive := time.Now().Format(time.RFC3339)
	minioClient := minioService.GetMinioClient(config.Minio)
	postgresDB := postgresService.GetPostgresDB(config.Postgres)

	s := &RoomPlaybackService{
		conf:     config,
		natsConn: natsConn,
		quitCh:   quitCh,

		joinRoomCh:     make(chan struct{}, 32),
		isSdkConnected: true,
		ctrlCh:         make(chan Ctrl, 32),

		timeLive:  timeLive,
		timeReady: "",

		sdkConnector: nil,
		roomService:  nil,

		postgresDB:       postgresDB,
		roomMgmtSchema:   config.Postgres.RoomMgmtSchema,
		roomRecordSchema: config.Postgres.RoomRecordSchema,

		minioClient: minioClient,
		bucketName:  config.Minio.BucketName,

		sessionId:          config.RoomMgmt.SessionId,
		systemUserIdPrefix: config.RoomMgmt.SystemUserIdPrefix,
		systemUsername:     config.RoomMgmt.SystemUsername,

		chatPayloads: make(ChatPayloads, 0),
		chatId:       0,

		waitPeer:  new(sync.WaitGroup),
		isRunning: false,
		peers:     make([]*PlaybackPeer, 0),
	}
	go s.start()
	s.preparePlayback()
	go s.checkForRoomError()
	for {
		time.Sleep(time.Second)
		if s.timeReady != "" {
			break
		}
	}
	go s.playbackSentry()
	if config.Playback.CheckForEmptyRoom {
		go s.checkForEmptyRoom()
	}

	return s
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
	roomRTC := sdk.NewRTC(sdkConnector)

	return sdkConnector, roomService, roomRTC, nil
}

func (s *RoomPlaybackService) start() {
	log.Infof("--- Starting NATS Subscription ---")
	onPlaybackPauseSub, err := s.natsConn.Subscribe(PAUSEPLAYBACK_TOPIC+s.sessionId, s.onPlaybackPause)
	if err != nil {
		log.Errorf("NATS Subscription error: %s", err)
		os.Exit(1)
	}
	defer onPlaybackPauseSub.Unsubscribe()
	onPlaybackSub, err := s.natsConn.Subscribe(PLAYBACK_TOPIC+s.sessionId, s.onPlayback)
	if err != nil {
		log.Errorf("NATS Subscription error: %s", err)
		os.Exit(1)
	}
	defer onPlaybackSub.Unsubscribe()

	log.Infof("--- Starting monitoring-API Server ---")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/liveness", s.getLiveness)
	router.GET("/readiness", s.getReadiness)

	log.Infof("HTTP service starting at %s", s.conf.Playback.Addr)
	err = router.Run(s.conf.Playback.Addr)
	if err != nil {
		log.Errorf("http listener error: %s", err)
		os.Exit(1)
	}
}

func (s *RoomPlaybackService) preparePlayback() {
	log.Infof("preparePlayback started")
	err := s.getRoomByPlaybackId(s.sessionId)
	if err != nil {
		os.Exit(1)
	}

	var queryStmt string
	// find peers media
	peerNames := make(map[string]string)
	var peerRows *sql.Rows
	queryStmt = `SELECT 	"peerId",
							"peerName"
					FROM "` + s.roomRecordSchema + `"."peerEvent"
					WHERE "roomId"=$1 AND "state"=$2`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		peerRows, err = s.postgresDB.Query(queryStmt, s.roomId, sdk.PeerState_JOIN)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		os.Exit(1)
	}
	defer peerRows.Close()
	for peerRows.Next() {
		var peerId string
		var peerName string
		err := peerRows.Scan(&peerId, &peerName)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
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
	for retry := 0; retry < RETRY_COUNT; retry++ {
		trackRows, err = s.postgresDB.Query(queryStmt, s.roomId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		os.Exit(1)
	}
	defer trackRows.Close()
	for trackRows.Next() {
		var trackRemoteId string
		err := trackRows.Scan(&trackRemoteId)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
		}
		orphanRemoteIds[trackRemoteId] = struct{}{}
	}
	var trackEventRows *sql.Rows
	queryStmt = `SELECT "trackRemoteIds"
					FROM "` + s.roomRecordSchema + `"."trackEvent" WHERE "roomId"=$1`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		trackEventRows, err = s.postgresDB.Query(queryStmt, s.roomId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		os.Exit(1)
	}
	defer trackEventRows.Close()
	for trackEventRows.Next() {
		var trackRemoteIds pq.StringArray
		err := trackEventRows.Scan(&trackRemoteIds)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
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

	s.chatPayloads = s.getChats(s.roomId)
	s.lenChatPayloads = len(s.chatPayloads)
	log.Infof("preparePlayback completed")
}

type ChatPayload struct {
	Uid        string      `json:"uid"`
	Name       string      `json:"name"`
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

func (s *RoomPlaybackService) getChats(roomId string) ChatPayloads {
	var err error
	chats := make(ChatPayloads, 0)
	var chatrows *sql.Rows
	queryStmt := `SELECT "userId",
						 "userName",
						 "timestamp",
						 "text",
						 "fileName",
						 "fileSize",
						 "filePath"
					 FROM "` + s.roomRecordSchema + `"."chat" WHERE "roomId"=$1`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		chatrows, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		os.Exit(1)
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
			&chat.Timestamp,
			&text,
			&fileinfo.Name,
			&fileinfo.Size,
			&filePath)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
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
		chat.Uid = PLAYBACK_PREFIX + userId
		chat.Name = PLAYBACK_PREFIX + name
		chats = append(chats, chat)
	}

	sort.Sort(chats)
	return chats
}
