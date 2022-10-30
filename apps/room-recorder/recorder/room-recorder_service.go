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
	"github.com/pion/webrtc/v3"
)

type PeerEvent struct {
	timeElapsed time.Duration
	state       sdk.PeerState
	peerId      string
	peerName    string
}

type TrackEvent struct {
	timeElapsed time.Duration
	state       sdk.TrackEvent_State
	peerId      string
	tracks      []sdk.TrackInfo
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

type Chat struct {
	timeElapsed time.Duration
	data        map[string]interface{}
}

type OnTrack struct {
	timeElapsed time.Duration
	trackId     string
	trackRemote TrackRemote
}

type Track struct {
	TimeElapsed time.Duration
	Data        []byte
}

// RoomRecorderService represents a room-recorder instance
type RoomRecorderService struct {
	conf           Config
	quitCh         chan os.Signal
	joinRoomCh     chan bool
	isSdkConnected bool
	runningCh      chan bool
	waitUpload     *sync.WaitGroup

	timeLive  string
	timeReady string

	sdkConnector *sdk.Connector
	roomService  *sdk.Room
	roomRTC      *sdk.RTC

	postgresDB       *sql.DB
	roomMgmtSchema   string
	roomRecordSchema string

	minioClient          *minio.Client
	bucketName           string
	folderName           string
	attachmentFolderName string
	videoFolderName      string
	audioFolderName      string

	roomId         string
	chopInterval   time.Duration
	systemUid      string
	systemUsername string

	roomRecordId string
	startTime    time.Time
}

func (s *RoomRecorderService) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomRecorderService) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, "Not Ready yet")
		return
	}
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
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
	for retry := 0; retry < DB_RETRY; retry++ {
		postgresDB, err = sql.Open("postgres", psqlconn)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	// postgresDB.Ping
	for retry := 0; retry < DB_RETRY; retry++ {
		err = postgresDB.Ping()
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to ping database: %v\n", err)
		os.Exit(1)
	}
	// create RoomMgmtSchema schema
	createStmt := `CREATE SCHEMA IF NOT EXISTS "` + config.Postgres.RoomMgmtSchema + `"`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
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
					"startTime"      TIMESTAMP NOT NULL,
					"endTime"        TIMESTAMP NOT NULL,
					"allowedUserId"  TEXT ARRAY NOT NULL,
					"earlyEndReason" TEXT NOT NULL,
					"createdBy"      TEXT NOT NULL,
					"createdAt"      TIMESTAMP NOT NULL,
					"updatedBy"      TEXT NOT NULL,
					"updatedAt"      TIMESTAMP NOT NULL)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}

	// create RoomRecordSchema schema
	createStmt = `CREATE SCHEMA IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "roomRecord"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."roomRecord"(
					"id"         UUID PRIMARY KEY,
					"roomId"     UUID NOT NULL,
					"startTime"  TIMESTAMP NOT NULL,
					"endTime"    TIMESTAMP NOT NULL,
					"folderPath" TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "peerEvent"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."peerEvent"(
					"id"           UUID PRIMARY KEY,
					"roomRecordId" UUID NOT NULL,
					"roomId"       UUID NOT NULL,
					"timeElapsed"  BIGINT NOT NULL,
					"state"        INT NOT NULL,
					"peerId"       TEXT NOT NULL,
					"peerName"     TEXT NOT NULL,
					CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."roomRecord"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "trackEvent"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."trackEvent"(
					"id"           UUID PRIMARY KEY,
					"roomRecordId" UUID NOT NULL,
					"roomId"       UUID NOT NULL,
					"timeElapsed"  BIGINT NOT NULL,
					"state"        INT NOT NULL,
					"peerId"       TEXT NOT NULL,
					CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."roomRecord"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "trackInfo"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."trackInfo"(
					"id"           UUID PRIMARY KEY,
					"trackEventId" UUID NOT NULL,
					"roomId"       UUID NOT NULL,
					"trackId"      TEXT NOT NULL,
					"kind"         TEXT NOT NULL,
					"muted"        BOOL NOT NULL,
					"type"         INT NOT NULL,
					"streamId"     TEXT NOT NULL,
					"label"        TEXT NOT NULL,
					"subscribe"    BOOL NOT NULL,
					"layer"        TEXT NOT NULL,
					"direction"    TEXT NOT NULL,
					"width"        INT NOT NULL,
					"height"       INT NOT NULL,
					"frameRate"    INT NOT NULL,
					CONSTRAINT fk_trackEvent FOREIGN KEY("trackEventId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."trackEvent"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "onTrack"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."onTrack"(
					"id"           UUID PRIMARY KEY,
					"roomRecordId" UUID NOT NULL,
					"roomId"       UUID NOT NULL,
					"timeElapsed"  BIGINT NOT NULL,
					"trackRemote"  JSON NOT NULL,
					CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."roomRecord"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "trackStream"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."trackStream"(
					"id"          UUID PRIMARY KEY,
					"onTrackId"   UUID NOT NULL,
					"roomId"      UUID NOT NULL,
					"timeElapsed" BIGINT NOT NULL,
					"filePath"    TEXT NOT NULL,
					CONSTRAINT fk_onTrack FOREIGN KEY("onTrackId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."onTrack"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "chatMessage"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."chatMessage"(
					"id"           UUID PRIMARY KEY,
					"roomRecordId" UUID NOT NULL,
					"roomId"       UUID NOT NULL,
					"timeElapsed"  BIGINT NOT NULL,
					"userId"       TEXT NOT NULL,
					"userName"     TEXT NOT NULL,
					"text"         TEXT NOT NULL,
					"timestamp"    TIMESTAMP NOT NULL,
					CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."roomRecord"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "chatAttachment"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."chatAttachment"(
					"id"           UUID PRIMARY KEY,
					"roomRecordId" UUID NOT NULL,
					"roomId"       UUID NOT NULL,
					"timeElapsed"  BIGINT NOT NULL,
					"userId"       TEXT NOT NULL,
					"userName"     TEXT NOT NULL,
					"fileName"     TEXT NOT NULL,
					"fileSize"     INT NOT NULL,
					"filePath"     TEXT NOT NULL,
					"timestamp"    TIMESTAMP NOT NULL,
					CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."roomRecord"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}

	return postgresDB
}

func NewRoomRecorderService(config Config, quitCh chan os.Signal) *RoomRecorderService {
	timeLive := time.Now().Format(time.RFC3339)
	minioClient := getMinioClient(config)
	postgresDB := getPostgresDB(config)
	folderName := config.Minio.FolderName
	if folderName == "" {
		folderName = "/"
	} else {
		if folderName[0] != '/' {
			folderName = "/" + folderName
		}
	}
	attachmentFolderName := config.Minio.AttachmentFolderName
	if attachmentFolderName == "" {
		attachmentFolderName = "/"
	} else {
		if attachmentFolderName[0] != '/' {
			attachmentFolderName = "/" + attachmentFolderName
		}
		if attachmentFolderName[len(attachmentFolderName)-1] != '/' {
			attachmentFolderName += "/"
		}
	}
	videoFolderName := config.Minio.VideoFolderName
	if videoFolderName == "" {
		videoFolderName = "/"
	} else {
		if videoFolderName[0] != '/' {
			videoFolderName = "/" + videoFolderName
		}
		if videoFolderName[len(videoFolderName)-1] != '/' {
			videoFolderName += "/"
		}
	}
	audioFolderName := config.Minio.AudioFolderName
	if audioFolderName == "" {
		audioFolderName = "/"
	} else {
		if audioFolderName[0] != '/' {
			audioFolderName = "/" + audioFolderName
		}
		if audioFolderName[len(audioFolderName)-1] != '/' {
			audioFolderName += "/"
		}
	}
	s := &RoomRecorderService{
		conf:           config,
		quitCh:         quitCh,
		joinRoomCh:     make(chan bool, 32),
		isSdkConnected: true,
		runningCh:      make(chan bool),
		waitUpload:     new(sync.WaitGroup),

		timeLive:  timeLive,
		timeReady: "",

		sdkConnector: nil,
		roomService:  nil,
		roomRTC:      nil,

		postgresDB:       postgresDB,
		roomMgmtSchema:   config.Postgres.RoomMgmtSchema,
		roomRecordSchema: config.Postgres.RoomRecordSchema,

		minioClient:          minioClient,
		bucketName:           config.Minio.BucketName,
		folderName:           folderName,
		attachmentFolderName: attachmentFolderName,
		videoFolderName:      videoFolderName,
		audioFolderName:      audioFolderName,

		roomId:         config.Recorder.Roomid,
		chopInterval:   time.Duration(config.Recorder.ChoppedInSeconds) * time.Second,
		systemUid:      config.Recorder.SystemUid,
		systemUsername: config.Recorder.SystemUsername,

		roomRecordId: "",
		startTime:    time.Now(),
	}
	s.getRoomInfo()
	go s.start()
	go s.roomRecorderSentry()
	go s.checkForRoomError()
	s.joinRoomCh <- true

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
