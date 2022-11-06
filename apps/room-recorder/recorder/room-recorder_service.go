package recorder

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
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

	roomId         string
	chopInterval   time.Duration
	systemUid      string
	systemUsername string
}

func NewRoomRecorderService(config Config, quitCh chan os.Signal) *RoomRecorderService {
	timeLive := time.Now().Format(time.RFC3339)
	minioClient := getMinioClient(config)
	postgresDB := getPostgresDB(config)
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

		roomId:         config.Recorder.RoomId,
		chopInterval:   time.Duration(config.Recorder.ChoppedInSeconds) * time.Second,
		systemUid:      config.Recorder.SystemUserId,
		systemUsername: config.Recorder.SystemUsername,
	}
	go s.start()
	s.getRoomInfo()
	go s.checkForRoomError()
	s.joinRoomCh <- struct{}{}

	return s
}

func getMinioClient(config Config) *minio.Client {
	log.Infof("--- Connecting to MinIO ---")
	minioClient, err := minio.New(config.Minio.Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(config.Minio.AccessKeyID,
			config.Minio.SecretAccessKey, ""),
		Secure: config.Minio.UseSSL,
	})
	if err != nil {
		log.Errorf("Unable to connect to filestore: %v\n", err)
		os.Exit(1)
	}
	err = minioClient.MakeBucket(context.Background(),
		config.Minio.BucketName,
		minio.MakeBucketOptions{})
	if err != nil {
		exists, errBucketExists := minioClient.BucketExists(context.Background(),
			config.Minio.BucketName)
		if errBucketExists != nil || !exists {
			log.Errorf("Unable to create bucket: %v\n", err)
			os.Exit(1)
		}
	}

	return minioClient
}

func getPostgresDB(config Config) *sql.DB {
	log.Infof("--- Connecting to PostgreSql ---")
	addrSplit := strings.Split(config.Postgres.Addr, ":")
	port, err := strconv.Atoi(addrSplit[1])
	if err != nil {
		log.Errorf("invalid port number: %s\n", addrSplit[1])
		os.Exit(1)
	}
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		addrSplit[0],
		port,
		config.Postgres.User,
		config.Postgres.Password,
		config.Postgres.Database)
	var postgresDB *sql.DB
	// postgresDB.Open
	for retry := 0; retry < RETRY_COUNT; retry++ {
		postgresDB, err = sql.Open("postgres", psqlconn)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	// postgresDB.Ping
	for retry := 0; retry < RETRY_COUNT; retry++ {
		err = postgresDB.Ping()
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to ping database: %v\n", err)
		os.Exit(1)
	}
	// create RoomMgmtSchema schema
	createStmt := `CREATE SCHEMA IF NOT EXISTS "` + config.Postgres.RoomMgmtSchema + `"`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "room"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomMgmtSchema + `"."room"(
					"id"             UUID PRIMARY KEY,
					"name"           TEXT NOT NULL,
					"status"         TEXT NOT NULL,
					"httpEndpoint"	 TEXT NOT NULL,
					"signalEndpoint" TEXT NOT NULL,
					"startTime"      TIMESTAMPTZ NOT NULL,
					"endTime"        TIMESTAMPTZ NOT NULL,
					"allowedUserId"  TEXT ARRAY NOT NULL,
					"earlyEndReason" TEXT NOT NULL,
					"createdBy"      TEXT NOT NULL,
					"createdAt"      TIMESTAMPTZ NOT NULL,
					"updatedBy"      TEXT NOT NULL,
					"updatedAt"      TIMESTAMPTZ NOT NULL)`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}

	// create RoomRecordSchema schema
	createStmt = `CREATE SCHEMA IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "room"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."room"(
					"id"        UUID PRIMARY KEY,
					"name"      TEXT NOT NULL,
					"startTime" TIMESTAMPTZ NOT NULL,
					"endTime"   TIMESTAMPTZ NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("id") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."trackEvent"(
					"id"             UUID PRIMARY KEY,
					"roomId"         UUID NOT NULL,
					"peerId"         TEXT NOT NULL,
					"trackRemoteIds" TEXT ARRAY NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "track"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."track"(
					"id"            UUID PRIMARY KEY,
					"roomId"        UUID NOT NULL,
					"trackRemoteId" TEXT NOT NULL,
					"mimeType"      TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "trackStream"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."trackStream"(
					"id"       UUID PRIMARY KEY,
					"trackId"  UUID NOT NULL,
					"roomId"   UUID NOT NULL,
					"filePath" TEXT NOT NULL,
					CONSTRAINT fk_track FOREIGN KEY("trackId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."track"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}

	return postgresDB
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
