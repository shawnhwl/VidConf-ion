package server

import (
	"database/sql"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	postgresService "github.com/pion/ion/apps/postgres"
)

const (
	UPDATEROOM_TOPIC     string = "roomUpdates."
	CREATEPLAYBACK_TOPIC string = "playbackCreate."
	DELETEPLAYBACK_TOPIC string = "playbackDelete."

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
	conf     Config
	natsConn *nats.Conn

	onRoomUpdatesCh    chan string
	onPlaybackCreateCh chan string
	onPlaybackDeleteCh chan string

	timeLive  string
	timeReady string

	postgresDB       *sql.DB
	roomMgmtSchema   string
	roomRecordSchema string

	systemUserIdPrefix string
	systemUsername     string

	pollInterval time.Duration

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

func (s *RoomSentryService) onRoomUpdates(msg *nats.Msg) {
	roomId := msg.Subject[len(UPDATEROOM_TOPIC):]
	log.Infof("onSubcribe %s%s", UPDATEROOM_TOPIC, roomId)
	if s.timeReady == "" {
		msg.Respond([]byte(NOT_READY))
		return
	}

	s.onRoomUpdatesCh <- roomId
	msg.Respond([]byte(""))
}

func (s *RoomSentryService) onPlaybackCreate(msg *nats.Msg) {
	playbackid := msg.Subject[len(CREATEPLAYBACK_TOPIC):]
	log.Infof("onSubcribe %s%s", CREATEPLAYBACK_TOPIC, playbackid)
	if s.timeReady == "" {
		msg.Respond([]byte(NOT_READY))
		return
	}

	s.onPlaybackCreateCh <- playbackid
	msg.Respond([]byte(""))
}

func (s *RoomSentryService) onPlaybackDelete(msg *nats.Msg) {
	playbackid := msg.Subject[len(DELETEPLAYBACK_TOPIC):]
	log.Infof("onSubcribe %s%s", DELETEPLAYBACK_TOPIC, playbackid)
	if s.timeReady == "" {
		msg.Respond([]byte(NOT_READY))
		return
	}

	s.onPlaybackDeleteCh <- playbackid
	msg.Respond([]byte(""))
}

func NewRoomMgmtSentryService(config Config, natsConn *nats.Conn) *RoomSentryService {
	timeLive := time.Now().Format(time.RFC3339)
	postgresDB := postgresService.GetPostgresDB(config.Postgres)

	s := &RoomSentryService{
		conf:     config,
		natsConn: natsConn,

		onRoomUpdatesCh:    make(chan string, 2048),
		onPlaybackCreateCh: make(chan string, 2048),
		onPlaybackDeleteCh: make(chan string, 2048),

		timeLive:  timeLive,
		timeReady: "",

		postgresDB:       postgresDB,
		roomMgmtSchema:   config.Postgres.RoomMgmtSchema,
		roomRecordSchema: config.Postgres.RoomRecordSchema,

		systemUserIdPrefix: config.RoomMgmt.SystemUserIdPrefix,
		systemUsername:     config.RoomMgmt.SystemUsername,

		pollInterval: time.Duration(config.RoomSentry.PollInSeconds) * time.Second,

		roomStarts:       make(map[string]StartRooms),
		roomStartKeys:    make([]string, 0),
		roomEnds:         make(map[string]Terminations),
		roomEndKeys:      make([]string, 0),
		announcements:    make(map[AnnounceKey]Announcements),
		announcementKeys: make([]AnnounceKey, 0),
	}

	go s.start()
	go s.checkForServiceCall()
	<-s.onRoomUpdatesCh
	s.timeReady = time.Now().Format(time.RFC3339)

	return s
}

func (s *RoomSentryService) start() {
	log.Infof("--- Starting NATS Subscription ---")
	onRoomUpdatesSub, err := s.natsConn.Subscribe(UPDATEROOM_TOPIC+"*", s.onRoomUpdates)
	if err != nil {
		log.Errorf("NATS Subscription error: %s", err)
		os.Exit(1)
	}
	defer onRoomUpdatesSub.Unsubscribe()
	onPlaybackCreateSub, err := s.natsConn.Subscribe(CREATEPLAYBACK_TOPIC+"*", s.onPlaybackCreate)
	if err != nil {
		log.Errorf("NATS Subscription error: %s", err)
		os.Exit(1)
	}
	defer onPlaybackCreateSub.Unsubscribe()
	onPlaybackDeleteSub, err := s.natsConn.Subscribe(DELETEPLAYBACK_TOPIC+"*", s.onPlaybackDelete)
	if err != nil {
		log.Errorf("NATS Subscription error: %s", err)
		os.Exit(1)
	}
	defer onPlaybackDeleteSub.Unsubscribe()

	log.Infof("--- Starting HTTP-API Server ---")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/liveness", s.getLiveness)
	router.GET("/readiness", s.getReadiness)
	log.Infof("HTTP service starting at %s", s.conf.RoomSentry.Addr)
	err = router.Run(s.conf.RoomSentry.Addr)
	if err != nil {
		log.Errorf("http listener error: %s", err)
		os.Exit(1)
	}
}
