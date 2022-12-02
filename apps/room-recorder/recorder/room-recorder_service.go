package recorder

import (
	"database/sql"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	minioService "github.com/pion/ion/apps/minio"
	postgresService "github.com/pion/ion/apps/postgres"
)

const (
	NOT_READY string = "service is not yet ready"
)

func (s *RoomRecorderService) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomRecorderService) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

// RoomRecorderService represents a room-recorder instance
type RoomRecorderService struct {
	conf           Config
	quitCh         chan os.Signal
	joinRoomCh     chan struct{}
	isSdkConnected bool
	exitCh         chan struct{}
	waitUpload     *sync.WaitGroup

	timeLive  string
	timeReady string

	sdkConnector *sdk.Connector
	roomService  *sdk.Room
	roomRTC      *sdk.RTC

	postgresDB       *sql.DB
	roomMgmtSchema   string
	roomRecordSchema string

	minioClient *minio.Client
	bucketName  string

	sessionId      string
	systemUserId   string
	systemUsername string

	chopInterval time.Duration
}

func NewRoomRecorderService(config Config, quitCh chan os.Signal) *RoomRecorderService {
	if strings.HasPrefix(config.RoomMgmt.SessionId, config.RoomMgmt.PlaybackIdPrefix) {
		log.Infof("is not a recording session, exiting")
		os.Exit(0)
	}
	timeLive := time.Now().Format(time.RFC3339)
	minioClient := minioService.GetMinioClient(config.Minio)
	postgresDB := postgresService.GetPostgresDB(config.Postgres)
	s := &RoomRecorderService{
		conf:           config,
		quitCh:         quitCh,
		joinRoomCh:     make(chan struct{}, 32),
		isSdkConnected: true,
		exitCh:         make(chan struct{}),
		waitUpload:     new(sync.WaitGroup),

		timeLive:  timeLive,
		timeReady: "",

		sdkConnector: nil,
		roomService:  nil,
		roomRTC:      nil,

		postgresDB:       postgresDB,
		roomMgmtSchema:   config.Postgres.RoomMgmtSchema,
		roomRecordSchema: config.Postgres.RoomRecordSchema,

		minioClient: minioClient,
		bucketName:  config.Minio.BucketName,

		sessionId:      config.RoomMgmt.SessionId,
		systemUserId:   config.RoomMgmt.SystemUserIdPrefix + sdk.RandomKey(16),
		systemUsername: config.RoomMgmt.SystemUsername,

		chopInterval: time.Duration(config.Recorder.ChoppedInSeconds) * time.Second,
	}
	go s.start()
	s.getRoomsByRoomid()
	go s.checkForRoomError()
	s.joinRoomCh <- struct{}{}

	return s
}

func (s *RoomRecorderService) start() {
	log.Infof("--- Starting monitoring-API Server ---")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/liveness", s.getLiveness)
	router.GET("/readiness", s.getReadiness)
	log.Infof("HTTP service starting at %s", s.conf.Recorder.Addr)
	log.Errorf("%s", router.Run(s.conf.Recorder.Addr))
	os.Exit(1)
}
