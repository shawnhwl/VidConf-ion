package server

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	log "github.com/pion/ion-log"
)

const (
	NOT_READY         string = "service is not yet ready"
	ANNOUNCEMENT_SENT string = "Sent"
	ROOM_BOOKED       string = "Booked"
	ROOM_STARTED      string = "Started"
	ROOM_ENDED        string = "Ended"
	FROM_END          string = "end"

	NOT_FOUND_PK string = "no rows in result set"

	RETRY_COUNT int           = 3
	RETRY_DELAY time.Duration = 5 * time.Second

	RECONNECTION_INTERVAL time.Duration = 10 * time.Second
)

type StartRooms struct {
	timeTick time.Time
	roomname string
}

type Terminations struct {
	timeTick time.Time
	roomname string
	reason   string
}

type Announcements struct {
	timeTick time.Time
	message  string
	userId   pq.StringArray
}

type AnnounceKey struct {
	roomId     string
	announceId string
}

type RoomSentryService struct {
	conf Config

	onRoomChanges    chan string
	onPlaybackCreate chan string
	onPlaybackDelete chan string

	timeLive  string
	timeReady string

	postgresDB     *sql.DB
	roomMgmtSchema string

	pollInterval   time.Duration
	systemUid      string
	systemUsername string
	endpoints      []string

	roomStarts       map[string]StartRooms
	roomStartKeys    []string
	roomEnds         map[string]Terminations
	roomEndKeys      []string
	announcements    map[AnnounceKey]Announcements
	announcementKeys []AnnounceKey
}

func (s *RoomSentryService) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomSentryService) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func (s *RoomSentryService) getRoomid(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("POST rooms/%s", roomId)
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}

	s.onRoomChanges <- roomId
	c.Status(http.StatusOK)
}

func (s *RoomSentryService) getPlaybackid(c *gin.Context) {
	playbackid := c.Param("playbackid")
	log.Infof("POST playback/%s", playbackid)
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}

	s.onPlaybackCreate <- playbackid
	c.Status(http.StatusOK)
}

func (s *RoomSentryService) deletePlaybackid(c *gin.Context) {
	playbackid := c.Param("playbackid")
	log.Infof("POST delete/playback/%s", playbackid)
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}

	s.onPlaybackDelete <- playbackid
	c.Status(http.StatusOK)
}

func NewRoomMgmtSentryService(config Config) *RoomSentryService {
	timeLive := time.Now().Format(time.RFC3339)
	postgresDB := getPostgresDB(config)

	s := &RoomSentryService{
		conf: config,

		onRoomChanges:    make(chan string, 2048),
		onPlaybackCreate: make(chan string, 2048),
		onPlaybackDelete: make(chan string, 2048),

		timeLive:  timeLive,
		timeReady: "",

		postgresDB:     postgresDB,
		roomMgmtSchema: config.Postgres.RoomMgmtSchema,

		pollInterval:   time.Duration(config.RoomMgmtSentry.PollInSeconds) * time.Second,
		systemUid:      config.RoomMgmtSentry.SystemUid,
		systemUsername: config.RoomMgmtSentry.SystemUsername,
		endpoints:      config.RoomMgmtSentry.Endpoints,

		roomStarts:       make(map[string]StartRooms),
		roomStartKeys:    make([]string, 0),
		roomEnds:         make(map[string]Terminations),
		roomEndKeys:      make([]string, 0),
		announcements:    make(map[AnnounceKey]Announcements),
		announcementKeys: make([]AnnounceKey, 0),
	}

	go s.start()
	go s.checkForServiceCall()
	<-s.onRoomChanges
	s.timeReady = time.Now().Format(time.RFC3339)

	return s
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
					"startTime"      TIMESTAMP NOT NULL,
					"endTime"        TIMESTAMP NOT NULL,
					"allowedUserId"  TEXT ARRAY NOT NULL,
					"earlyEndReason" TEXT NOT NULL,
					"createdBy"      TEXT NOT NULL,
					"createdAt"      TIMESTAMP NOT NULL,
					"updatedBy"      TEXT NOT NULL,
					"updatedAt"      TIMESTAMP NOT NULL)`
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
	// create table "announcement"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomMgmtSchema + `"."announcement"(
					"id"                    UUID PRIMARY KEY,
					"roomId"                UUID NOT NULL,
					"status"                TEXT NOT NULL,
					"message"               TEXT NOT NULL,
					"relativeFrom"          TEXT NOT NULL,
					"relativeTimeInSeconds" INT NOT NULL,
					"userId"                TEXT ARRAY NOT NULL,
					"createdAt"             TIMESTAMP NOT NULL,
					"createdBy"             TEXT NOT NULL,
					"updatedAt"             TIMESTAMP NOT NULL,
					"updatedBy"             TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id") ON DELETE CASCADE)`
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
		"startTime" TIMESTAMP NOT NULL,
		"endTime"   TIMESTAMP NOT NULL,
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
	// create table "playback"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomMgmtSchema + `"."playback"(
					"id"       UUID PRIMARY KEY,
					"roomId"   UUID NOT NULL,
					"name"     TEXT NOT NULL,
					"endpoint" TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."room"("id"))`
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

func (s *RoomSentryService) start() {
	log.Infof("--- Starting HTTP-API Server ---")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/liveness", s.getLiveness)
	router.GET("/readiness", s.getReadiness)
	router.POST("/rooms/:roomid", s.getRoomid)
	router.POST("/playback/:playbackid", s.getPlaybackid)
	router.POST("/delete/playback/:playbackid", s.deletePlaybackid)

	log.Infof("HTTP service starting at %s", s.conf.RoomMgmtSentry.Addr)
	log.Errorf("%s", router.Run(s.conf.RoomMgmtSentry.Addr))
	os.Exit(1)
}
