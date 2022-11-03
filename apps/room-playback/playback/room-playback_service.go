package playback

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
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
	_ "github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
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
	log.Infof("POST /play")
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}

	s.ctrlCh <- "play"
	c.Status(http.StatusOK)
}

func (s *RoomPlaybackService) postPlayFrom(c *gin.Context) {
	secondsFromStart := c.Param("secondsfromstart")
	log.Infof("POST /playFrom/%s", secondsFromStart)
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}
	_, err := strconv.Atoi(secondsFromStart)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "non-integer skip interval"})
		return
	}

	s.ctrlCh <- secondsFromStart
	c.Status(http.StatusOK)
}

func (s *RoomPlaybackService) postPause(c *gin.Context) {
	log.Infof("POST /pause")
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}

	s.ctrlCh <- "pause"
	c.Status(http.StatusOK)
}

// RoomPlaybackService represents a room-playback instance
type RoomPlaybackService struct {
	conf   Config
	quitCh chan os.Signal

	joinRoomCh     chan struct{}
	isSdkConnected bool
	ctrlCh         chan string

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

	pauseTime    time.Time
	playTime     time.Time
	chatPayloads ChatPayloads
	chatId       int

	waitPeer  *sync.WaitGroup
	isRunning bool
	peers     []*PlaybackPeer
}

func NewRoomPlaybackService(config Config, quitCh chan os.Signal) *RoomPlaybackService {
	timeLive := time.Now().Format(time.RFC3339)
	minioClient := getMinioClient(config)
	postgresDB := getPostgresDB(config)
	s := &RoomPlaybackService{
		conf:   config,
		quitCh: quitCh,

		joinRoomCh:     make(chan struct{}, 32),
		isSdkConnected: true,
		ctrlCh:         make(chan string, 32),

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
	go s.playbackSentry()
	go s.checkForEmptyRoom()

	return s
}

type TestTrack struct {
	Timestamp time.Time
	Data      []byte
}

func testMinioClient(minioClient *minio.Client, config Config) {
	testString := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	ptestString := &testString
	testStringData := bytes.NewReader([]byte(*ptestString))
	var err error
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = minioClient.PutObject(context.Background(),
			config.Minio.BucketName,
			"/test/testString",
			testStringData,
			int64(len(*ptestString)),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not upload attachment: %s", err)
		os.Exit(1)
	}

	var object *minio.Object
	for retry := 0; retry < RETRY_COUNT; retry++ {
		object, err = minioClient.GetObject(context.Background(),
			config.Minio.BucketName,
			"/test/testString",
			minio.GetObjectOptions{})
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not download attachment: %s", err)
		os.Exit(1)
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(object)
	if err != nil {
		log.Errorf("could not process downloaded attachment: %s", err)
		os.Exit(1)
	}
	if testString != buf.String() {
		log.Errorf("downloaded base64 string is different: %s", buf.String())
		os.Exit(1)
	}

	testTracks := make([]TestTrack, 0)
	for id := 0; id < 100; id++ {
		timeNow := time.Now()
		testTracks = append(testTracks, TestTrack{timeNow, []byte(timeNow.String())})
	}
	for id := 0; id < 100; id += 5 {
		var testTrackData bytes.Buffer
		err := gob.NewEncoder(&testTrackData).Encode(testTracks[id : id+5])
		if err != nil {
			log.Errorf("track encoding error:", err)
			os.Exit(1)
		}

		var track []TestTrack
		buf := bytes.NewBuffer(testTrackData.Bytes())
		err = gob.NewDecoder(buf).Decode(&track)
		if err != nil {
			log.Errorf("track decoding error:", err)
			os.Exit(1)
		}

		if string(testTracks[id].Data) != string(track[0].Data) {
			log.Errorf("codec tracks.Data is different: %s vs %s", string(testTracks[id].Data), string(track[0].Data))
			os.Exit(1)
		}

		for retry := 0; retry < RETRY_COUNT; retry++ {
			_, err = minioClient.PutObject(context.Background(),
				config.Minio.BucketName,
				"/test/testTrack"+strconv.Itoa(id),
				&testTrackData,
				int64(testTrackData.Len()),
				minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err == nil {
				break
			}
			time.Sleep(RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not upload file: %s", err)
			os.Exit(1)
		}
	}

	recvTracks := make([]TestTrack, 0)
	for id := 0; id < 100; id += 5 {
		for retry := 0; retry < RETRY_COUNT; retry++ {
			object, err = minioClient.GetObject(context.Background(),
				config.Minio.BucketName,
				"/test/testTrack"+strconv.Itoa(id),
				minio.GetObjectOptions{})
			if err == nil {
				break
			}
			time.Sleep(RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not download attachment: %s", err)
			os.Exit(1)
		}

		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(object)
		if err != nil {
			log.Errorf("could not process download: %s", err)
			continue
		}
		var tracks []TestTrack
		gob.NewDecoder(buf).Decode(&tracks)
		recvTracks = append(recvTracks, tracks...)
	}

	for id := 0; id < 100; id++ {
		if string(testTracks[id].Data) != string(recvTracks[id].Data) {
			log.Errorf("downloaded tracks.data is different: %s vs %s", string(testTracks[id].Data), string(recvTracks[id].Data))
			os.Exit(1)
		}
	}

	log.Infof("testMinioClient OK")
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

	testMinioClient(minioClient, config)
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
	// create table "peerEvent"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."peerEvent"(
					"id"           UUID PRIMARY KEY,
					"roomId"       UUID NOT NULL,
					"timestamp"    TIMESTAMPTZ NOT NULL,
					"state"        INT NOT NULL,
					"peerId"       TEXT NOT NULL,
					"peerName"     TEXT NOT NULL,
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
	// create table "trackEvent"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."trackEvent"(
					"id"           UUID PRIMARY KEY,
					"roomId"       UUID NOT NULL,
					"timestamp"    TIMESTAMPTZ NOT NULL,
					"state"        INT NOT NULL,
					"peerId"       TEXT NOT NULL,
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
	// create table "onTrack"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."onTrack"(
					"id"           UUID PRIMARY KEY,
					"roomId"       UUID NOT NULL,
					"trackId"      TEXT NOT NULL,
					"timestamp"    TIMESTAMPTZ NOT NULL,
					"trackRemote"  JSON NOT NULL,
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
					"id"          UUID PRIMARY KEY,
					"onTrackId"   UUID NOT NULL,
					"roomId"      UUID NOT NULL,
					"timestamp"   TIMESTAMPTZ NOT NULL,
					"filePath"    TEXT NOT NULL,
					CONSTRAINT fk_onTrack FOREIGN KEY("onTrackId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."onTrack"("id") ON DELETE CASCADE,
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
	// create table "chatMessage"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."chatMessage"(
					"id"           UUID PRIMARY KEY,
					"roomId"       UUID NOT NULL,
					"timestamp"    TIMESTAMPTZ NOT NULL,
					"userId"       TEXT NOT NULL,
					"userName"     TEXT NOT NULL,
					"text"         TEXT NOT NULL,
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
	// create table "chatAttachment"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."chatAttachment"(
					"id"           UUID PRIMARY KEY,
					"roomId"       UUID NOT NULL,
					"timestamp"    TIMESTAMPTZ NOT NULL,
					"userId"       TEXT NOT NULL,
					"userName"     TEXT NOT NULL,
					"fileName"     TEXT NOT NULL,
					"fileSize"     INT NOT NULL,
					"filePath"     TEXT NOT NULL,
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
	router.POST("/play", s.postPlay)
	router.POST("/play/:secondsfromstart", s.postPlayFrom)
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

	peerIds := make(map[string]string)
	var rows *sql.Rows
	queryStmt := `SELECT 	"peerId",
							"peerName"
					FROM "` + s.roomRecordSchema + `"."peerEvent" WHERE "roomId"=$1`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		rows, err = s.postgresDB.Query(queryStmt, s.roomId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		os.Exit(1)
	}
	defer rows.Close()
	for rows.Next() {
		var peerId string
		var peerName string
		err := rows.Scan(&peerId, &peerName)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
		}
		peerIds[peerId] = peerName
	}

	for peerId := range peerIds {
		s.peers = append(s.peers, s.NewPlaybackPeer(peerId, peerIds[peerId]))
	}
	s.chatPayloads = s.getChats(s.roomId)
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
						 "text",
						 "timestamp"
					FROM "` + s.roomRecordSchema + `"."chatMessage" WHERE "roomId"=$1`
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
		var text string
		err := chatrows.Scan(&chat.Uid,
			&chat.Name,
			&text,
			&chat.Timestamp)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
		}
		chat.Text = &text
		chats = append(chats, chat)
	}

	attachments := make(ChatPayloads, 0)
	var attachmentrows *sql.Rows
	queryStmt = `SELECT "userId",
						"userName",
						"fileName",
						"fileSize",
						"filePath",
						"timestamp"
					FROM "` + s.roomRecordSchema + `"."chatAttachment" WHERE "roomId"=$1`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		attachmentrows, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		os.Exit(1)
	}
	defer attachmentrows.Close()
	for attachmentrows.Next() {
		var attachment ChatPayload
		var fileinfo Attachment
		var filePath string
		err := chatrows.Scan(&attachment.Uid,
			&attachment.Name,
			&fileinfo.Name,
			&fileinfo.Size,
			&filePath,
			&attachment.Timestamp)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
		}
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
		attachment.Base64File = &fileinfo
		attachments = append(attachments, attachment)
	}

	chats = append(chats, attachments...)
	sort.Sort(chats)
	return chats
}
