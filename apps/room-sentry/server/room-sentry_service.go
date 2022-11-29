package server

import (
	"database/sql"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	log "github.com/pion/ion-log"
	postgresService "github.com/pion/ion/apps/postgres"
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

	postgresDB       *sql.DB
	roomMgmtSchema   string
	roomRecordSchema string

	pollInterval   time.Duration
	systemUserId   string
	systemUsername string
	httpEndpoints  []string

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
	postgresDB := postgresService.GetPostgresDB(config.Postgres)

	s := &RoomSentryService{
		conf: config,

		onRoomChanges:    make(chan string, 2048),
		onPlaybackCreate: make(chan string, 2048),
		onPlaybackDelete: make(chan string, 2048),

		timeLive:  timeLive,
		timeReady: "",

		postgresDB:       postgresDB,
		roomMgmtSchema:   config.Postgres.RoomMgmtSchema,
		roomRecordSchema: config.Postgres.RoomRecordSchema,

		pollInterval:   time.Duration(config.RoomSentry.PollInSeconds) * time.Second,
		systemUserId:   config.RoomSentry.SystemUserId,
		systemUsername: config.RoomSentry.SystemUsername,
		httpEndpoints:  config.RoomSentry.HttpEndpoints,

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

	log.Infof("HTTP service starting at %s", s.conf.RoomSentry.Addr)
	log.Errorf("%s", router.Run(s.conf.RoomSentry.Addr))
	os.Exit(1)
}
