package sentry

import (
	"database/sql"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	constants "github.com/pion/ion/apps/constants"
	postgresService "github.com/pion/ion/apps/postgres"
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

	onRoomUpdatesCh chan string

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
		c.String(http.StatusInternalServerError, constants.ErrNotReady.Error())
		return
	}
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func (s *RoomSentryService) onRoomUpdates(msg *nats.Msg) {
	roomId := msg.Subject[len(constants.UPDATEROOM_TOPIC):]
	log.Infof("onSubcribe %s%s", constants.UPDATEROOM_TOPIC, roomId)
	if s.timeReady == "" {
		msg.Respond([]byte(constants.ErrNotReady.Error()))
		return
	}

	s.onRoomUpdatesCh <- roomId
	msg.Respond([]byte(""))
}

func (s *RoomSentryService) onPlaybackDelete(msg *nats.Msg) {
	playbackId := msg.Subject[len(constants.DELETEPLAYBACK_TOPIC):]
	log.Infof("onSubcribe %s%s", constants.DELETEPLAYBACK_TOPIC, playbackId)
	if s.timeReady == "" {
		msg.Respond([]byte(constants.ErrNotReady.Error()))
		return
	}

	err := s.deletePlayback(playbackId)
	if err != nil {
		msg.Respond([]byte(err.Error()))
		return
	}

	msg.Respond([]byte(""))
}

func NewRoomMgmtSentryService(config Config, natsConn *nats.Conn) *RoomSentryService {
	timeLive := time.Now().Format(time.RFC3339)
	postgresDB := postgresService.GetPostgresDB(config.Postgres)

	s := &RoomSentryService{
		conf:     config,
		natsConn: natsConn,

		onRoomUpdatesCh: make(chan string, 2048),

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
	onRoomUpdatesSub, err := s.natsConn.Subscribe(constants.UPDATEROOM_TOPIC+"*", s.onRoomUpdates)
	if err != nil {
		log.Errorf("NATS Subscription error: %s", err)
		os.Exit(1)
	}
	defer onRoomUpdatesSub.Unsubscribe()
	onPlaybackDeleteSub, err := s.natsConn.Subscribe(constants.DELETEPLAYBACK_TOPIC+"*", s.onPlaybackDelete)
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

func (s *RoomSentryService) natsPublish(topic string, data []byte) error {
	log.Infof("publishing to topic '%s'", topic)
	var resp *nats.Msg
	var err error
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		resp, err = s.natsConn.Request(topic, data, constants.RETRY_DELAY)
		if err == nil && string(resp.Data) == "" {
			break
		}
	}
	if err != nil {
		log.Errorf("error publishing topic '%s': %s", topic, err)
		return err
	}
	if string(resp.Data) != "" {
		log.Errorf("error publishing topic '%s': %s", topic, string(resp.Data))
		return errors.New(string(resp.Data))
	}
	return nil
}
