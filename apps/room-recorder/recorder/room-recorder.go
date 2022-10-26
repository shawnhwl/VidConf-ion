package recorder

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	"github.com/pion/webrtc/v3"
	"github.com/spf13/viper"
)

type LogConf struct {
	Level string `mapstructure:"level"`
}

type PostgresConf struct {
	Addr             string `mapstructure:"addr"`
	User             string `mapstructure:"user"`
	Password         string `mapstructure:"password"`
	Database         string `mapstructure:"database"`
	RoomMgmtSchema   string `mapstructure:"roomMgmtSchema"`
	RoomRecordSchema string `mapstructure:"roomRecordSchema"`
}

type MinioConf struct {
	Endpoint             string `mapstructure:"endpoint"`
	UseSSL               bool   `mapstructure:"useSSL"`
	AccessKeyID          string `mapstructure:"username"`
	SecretAccessKey      string `mapstructure:"password"`
	BucketName           string `mapstructure:"bucketName"`
	FolderName           string `mapstructure:"folderName"`
	AttachmentFolderName string `mapstructure:"attachmentFolderName"`
	VideoFolderName      string `mapstructure:"videoFolderName"`
	AudioFolderName      string `mapstructure:"audioFolderName"`
}

type SignalConf struct {
	Addr string `mapstructure:"addr"`
}

type RecorderConf struct {
	Addr             string `mapstructure:"addr"`
	Roomid           string `mapstructure:"roomid"`
	ChoppedInSeconds int    `mapstructure:"choppedInSeconds"`
	SystemUid        string `mapstructure:"system_userid"`
	SystemUsername   string `mapstructure:"system_username"`
}

type Config struct {
	Log      LogConf      `mapstructure:"log"`
	Postgres PostgresConf `mapstructure:"postgres"`
	Minio    MinioConf    `mapstructure:"minio"`
	Signal   SignalConf   `mapstructure:"signal"`
	Recorder RecorderConf `mapstructure:"recorder"`
}

func unmarshal(rawVal interface{}) error {
	if err := viper.Unmarshal(rawVal); err != nil {
		return err
	}
	return nil
}

func (c *Config) Load(file string) error {
	_, err := os.Stat(file)
	if err != nil {
		return err
	}

	viper.SetConfigFile(file)
	viper.SetConfigType("toml")

	err = viper.ReadInConfig()
	if err != nil {
		log.Errorf("config file %s read failed. %v\n", file, err)
		return err
	}

	err = unmarshal(c)
	if err != nil {
		return err
	}
	if err != nil {
		log.Errorf("config file %s loaded failed. %v\n", file, err)
		return err
	}

	log.Infof("config %s load ok!", file)
	return nil
}

type PeerEvent struct {
	timeElapsed time.Duration
	state       sdk.PeerState
	peerId      string
	peerName    string
}

type TrackEvent struct {
	timeElapsed  time.Duration
	state        sdk.TrackEvent_State
	trackEventId string
	tracks       []sdk.TrackInfo
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

// RoomRecorder represents a room-recorder instance
type RoomRecorder struct {
	conf   Config
	quitCh chan os.Signal

	timeLive    string
	timeReady   string
	roomService *sdk.Room
	roomRTC     *sdk.RTC

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

func (s *RoomRecorder) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomRecorder) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func NewRoomRecorder(config Config, quitCh chan os.Signal) *RoomRecorder {
	timeLive := time.Now().Format(time.RFC3339)

	log.Infof("--- Connecting to MinIO ---")
	minioClient, err := minio.New(config.Minio.Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(config.Minio.AccessKeyID,
			config.Minio.SecretAccessKey, ""),
		Secure: config.Minio.UseSSL,
	})
	if err != nil {
		log.Panicf("Unable to connect to filestore: %v\n", err)
	}
	err = minioClient.MakeBucket(context.Background(),
		config.Minio.BucketName,
		minio.MakeBucketOptions{})
	if err != nil {
		exists, errBucketExists := minioClient.BucketExists(context.Background(),
			config.Minio.BucketName)
		if errBucketExists != nil || !exists {
			log.Panicf("Unable to create bucket: %v\n", err)
		}
	}

	log.Infof("--- Connecting to PostgreSql ---")
	addrSplit := strings.Split(config.Postgres.Addr, ":")
	port, err := strconv.Atoi(addrSplit[1])
	if err != nil {
		log.Panicf("invalid port number: %s\n", addrSplit[1])
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
		log.Panicf("Unable to connect to database: %v\n", err)
	}
	// postgresDB.Ping
	for retry := 0; retry < DB_RETRY; retry++ {
		err = postgresDB.Ping()
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to ping database: %v\n", err)
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
		log.Panicf("Unable to execute sql statement: %v\n", err)
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
		log.Panicf("Unable to execute sql statement: %v\n", err)
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
		log.Panicf("Unable to execute sql statement: %v\n", err)
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
		log.Panicf("Unable to execute sql statement: %v\n", err)
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
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}
	// create table "trackEvent"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."trackEvent"(
					"id"           UUID PRIMARY KEY,
					"roomRecordId" UUID NOT NULL,
					"roomId"       UUID NOT NULL,
					"timeElapsed"  BIGINT NOT NULL,
					"state"        INT NOT NULL,
					"trackEventId" TEXT NOT NULL,
					CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."roomRecord"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
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
		log.Panicf("Unable to execute sql statement: %v\n", err)
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
		log.Panicf("Unable to execute sql statement: %v\n", err)
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
		log.Panicf("Unable to execute sql statement: %v\n", err)
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
		log.Panicf("Unable to execute sql statement: %v\n", err)
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
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}

	log.Infof("--- Connecting to Room Signal ---")
	log.Infof("attempt gRPC connection to %s", config.Signal.Addr)
	sdk_connector := sdk.NewConnector(config.Signal.Addr)
	if sdk_connector == nil {
		log.Panicf("connection to %s fail", config.Signal.Addr)
	}
	sdk_roomService := sdk.NewRoom(sdk_connector)
	sdk_rtc, err := sdk.NewRTC(sdk_connector)
	if err != nil {
		log.Panicf("rtc connector fail: %s", err)
	}
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
	s := &RoomRecorder{
		conf:        config,
		quitCh:      quitCh,
		timeLive:    timeLive,
		roomService: sdk_roomService,
		roomRTC:     sdk_rtc,

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
	}

	s.testUpload(s.folderName+"myobject", "main.go")
	s.testDownload(s.folderName+"myobject", "local.go1")

	return s
}

func (s *RoomRecorder) Start() {
	defer s.postgresDB.Close()
	defer s.roomService.Close()
	defer s.roomRTC.Close()

	err := s.getRoomsByRoomid(s.roomId)
	if err != nil {
		log.Panicf("Join room fail:%s", err.Error())
	}
	log.Infof("--- Joining Room ---")
	s.joinRoom()
	log.Infof("RoomRecorder Started")
	defer log.Infof("RoomRecorder Ended")

	s.timeReady = time.Now().Format(time.RFC3339)
	log.Infof("--- Starting monitoring-API Server ---")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/liveness", s.getLiveness)
	router.GET("/readiness", s.getReadiness)
	log.Infof("HTTP service starting at %s", s.conf.Recorder.Addr)
	log.Panicf("%s", router.Run(s.conf.Recorder.Addr))
}
