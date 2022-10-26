package server

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
)

const (
	ANNOUNCEMENT_SENT string = "Sent"
	ROOM_BOOKED       string = "Booked"
	ROOM_STARTED      string = "Started"
	ROOM_ENDED        string = "Ended"
	FROM_END          string = "end"

	RETRY_COUNT int = 3
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

type RoomMgmtSentryService struct {
	conf Config

	timeLive    string
	timeReady   string
	roomService *sdk.Room

	postgresDB     *sql.DB
	roomMgmtSchema string

	onChanges      chan string
	pollInterval   time.Duration
	systemUid      string
	systemUsername string

	roomStarts       map[string]StartRooms
	roomStartKeys    []string
	roomEnds         map[string]Terminations
	roomEndKeys      []string
	announcements    map[AnnounceKey]Announcements
	announcementKeys []AnnounceKey
}

func (s *RoomMgmtSentryService) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomMgmtSentryService) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, "Not Ready yet")
		return
	}
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func (s *RoomMgmtSentryService) getRoomid(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("GET rooms/%s", roomId)

	s.onChanges <- roomId
	c.Status(http.StatusOK)
}

func NewRoomMgmtSentryService(config Config) *RoomMgmtSentryService {
	timeLive := time.Now().Format(time.RFC3339)

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
	for retry := 0; retry < RETRY_COUNT; retry++ {
		postgresDB, err = sql.Open("postgres", psqlconn)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to connect to database: %v\n", err)
	}
	// postgresDB.Ping
	for retry := 0; retry < RETRY_COUNT; retry++ {
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
	for retry := 0; retry < RETRY_COUNT; retry++ {
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
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
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
	roomService := sdk.NewRoom(sdk_connector)

	s := &RoomMgmtSentryService{
		conf: config,

		timeLive:    timeLive,
		timeReady:   "",
		roomService: roomService,

		postgresDB:     postgresDB,
		roomMgmtSchema: config.Postgres.RoomMgmtSchema,

		onChanges:      make(chan string, 2048),
		pollInterval:   time.Duration(config.RoomMgmtSentry.PollInSeconds) * time.Second,
		systemUid:      config.RoomMgmtSentry.SystemUid,
		systemUsername: config.RoomMgmtSentry.SystemUsername,

		roomStarts:       make(map[string]StartRooms),
		roomStartKeys:    make([]string, 0),
		roomEnds:         make(map[string]Terminations),
		roomEndKeys:      make([]string, 0),
		announcements:    make(map[AnnounceKey]Announcements),
		announcementKeys: make([]AnnounceKey, 0),
	}
	go s.start()
	go s.sentry()
	<-s.onChanges
	return s
}

func (s *RoomMgmtSentryService) start() {
	defer s.postgresDB.Close()
	defer s.roomService.Close()
	defer close(s.onChanges)

	log.Infof("--- Starting HTTP-API Server ---")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/liveness", s.getLiveness)
	router.GET("/readiness", s.getReadiness)
	router.GET("/rooms/:roomid", s.getRoomid)

	log.Infof("HTTP service starting at %s", s.conf.RoomMgmtSentry.Addr)
	log.Panicf("%s", router.Run(s.conf.RoomMgmtSentry.Addr))
}
