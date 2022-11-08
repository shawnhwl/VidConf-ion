package playback

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	minioService "github.com/pion/ion/apps/minio"
	postgresService "github.com/pion/ion/apps/postgres"
)

const (
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

func (s *RoomPlaybackService) postPlay(c *gin.Context) {
	speed := c.Param("speed")
	log.Infof("POST /%s", speed)
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}

	speedf, err := strconv.ParseFloat(speed, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "non-float speed"})
		return
	}
	s.ctrlCh <- Ctrl{
		isPaused:     false,
		skipInterval: -1,
		speed10:      time.Duration(speedf * 10),
	}
	c.Status(http.StatusOK)
}

func (s *RoomPlaybackService) postPlayFrom(c *gin.Context) {
	speed := c.Param("speed")
	secondsFromStart := c.Param("secondsfromstart")
	log.Infof("POST /%s/%s", speed, secondsFromStart)
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}

	speedf, err := strconv.ParseFloat(speed, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "non-float speed"})
		return
	}
	skipInterval, err := strconv.Atoi(secondsFromStart)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "non-integer skip interval"})
		return
	}

	s.ctrlCh <- Ctrl{
		isPaused:     false,
		skipInterval: skipInterval,
		speed10:      time.Duration(speedf * 10),
	}
	c.Status(http.StatusOK)
}

func (s *RoomPlaybackService) postPause(c *gin.Context) {
	log.Infof("POST /pause")
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}

	s.ctrlCh <- Ctrl{
		isPaused: true,
	}
	c.Status(http.StatusOK)
}

type Ctrl struct {
	isPaused        bool
	skipInterval    int
	speed10         time.Duration
	actualRefTime   time.Time
	playbackRefTime time.Time
}

// RoomPlaybackService represents a room-playback instance
type RoomPlaybackService struct {
	conf   Config
	quitCh chan os.Signal

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

	playbackId      string
	systemUserId    string
	lenSystemUserId int
	systemUsername  string
	roomId          string
	roomStartTime   time.Time

	chatPayloads    ChatPayloads
	lenChatPayloads int
	chatId          int

	waitPeer  *sync.WaitGroup
	isRunning bool
	peers     []*PlaybackPeer

	speed10         time.Duration
	playbackRefTime time.Time
	actualRefTime   time.Time
}

func NewRoomPlaybackService(config Config, quitCh chan os.Signal) *RoomPlaybackService {
	timeLive := time.Now().Format(time.RFC3339)
	minioClient := minioService.GetMinioClient(config.Minio)
	postgresDB := postgresService.GetPostgresDB(config.Postgres)

	s := &RoomPlaybackService{
		conf:   config,
		quitCh: quitCh,

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

		playbackId:      config.Playback.PlaybackId,
		systemUserId:    config.Playback.SystemUserId,
		lenSystemUserId: len(config.Playback.SystemUserId),
		systemUsername:  config.Playback.SystemUsername,

		chatPayloads: make(ChatPayloads, 0),
		chatId:       0,

		waitPeer:  new(sync.WaitGroup),
		isRunning: false,
		peers:     make([]*PlaybackPeer, 0),
	}
	go s.start()
	s.preparePlayback()
	go s.checkForRoomError()
	s.joinRoomCh <- struct{}{}
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
	roomRTC, err := sdk.NewRTC(sdkConnector)
	if err != nil {
		log.Errorf("rtc connector fail: %s", err)
		return nil, nil, nil, errors.New("")
	}
	return sdkConnector, roomService, roomRTC, nil
}

func (s *RoomPlaybackService) start() {
	log.Infof("--- Starting monitoring-API Server ---")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/liveness", s.getLiveness)
	router.GET("/readiness", s.getReadiness)
	router.POST("/:speed", s.postPlay)
	router.POST("/:speed/:secondsfromstart", s.postPlayFrom)
	router.POST("/pause", s.postPause)

	log.Infof("HTTP service starting at %s", s.conf.Playback.Addr)
	log.Errorf("%s", router.Run(s.conf.Playback.Addr))
	os.Exit(1)
}

func (s *RoomPlaybackService) preparePlayback() {
	log.Infof("preparePlayback started")
	err := s.getRoomByPlaybackId(s.playbackId)
	if err != nil {
		os.Exit(1)
	}

	var queryStmt string
	// find peers media
	peerNames := make(map[string]string)
	var peerRows *sql.Rows
	queryStmt = `SELECT 	"peerId",
							"peerName"
					FROM "` + s.roomRecordSchema + `"."peerEvent" WHERE "roomId"=$1`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		peerRows, err = s.postgresDB.Query(queryStmt, s.roomId)
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
	for peerId := range peerNames {
		s.peers = append(s.peers, s.NewPlaybackPeer(peerId, peerNames[peerId], ""))
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
		s.peers = append(s.peers, s.NewPlaybackPeer(uuid.NewString(), "", orphanRemoteId))
	}

	s.chatPayloads = s.getChats(s.roomId)
	s.lenChatPayloads = len(s.chatPayloads)
	s.waitPeer.Wait()
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
						 "timestamp"
						 "text",
						 "fileName",
						 "fileSize",
						 "filePath",
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
