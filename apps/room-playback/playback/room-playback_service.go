package playback

import (
	"database/sql"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	minio "github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	constants "github.com/pion/ion/apps/constants"
	minioService "github.com/pion/ion/apps/minio"
	postgresService "github.com/pion/ion/apps/postgres"
)

func (s *RoomPlaybackService) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomPlaybackService) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func (s *RoomPlaybackService) onStartPlayback(msg *nats.Msg) {
	log.Infof("QueueSubscribe %s", msg.Subject)
	sessionId := string(msg.Data)
	session, err := s.newRoomPlaybackSession(sessionId)
	if err != nil {
		msg.Respond([]byte(err.Error()))
		return
	}
	go session.run()
	s.sessions.Store(sessionId, session)
	msg.Respond([]byte(""))
}

func (s *RoomPlaybackService) onEndPlayback(msg *nats.Msg) {
	log.Infof("onSubcribe %s", msg.Subject)
	sessionId := msg.Subject[len(constants.ENDPLAYBACK_TOPIC):]
	session, loaded := s.sessions.LoadAndDelete(sessionId)
	if !loaded {
		return
	}
	go session.(*RoomPlaybackSession).close()
	msg.Respond([]byte(""))
}

// RoomPlaybackService represents a room-playback service instance
type RoomPlaybackService struct {
	conf     Config
	natsConn *nats.Conn

	timeLive  string
	timeReady string

	postgresDB  *sql.DB
	minioClient *minio.Client

	sessions sync.Map
}

func NewRoomPlaybackService(config Config, natsConn *nats.Conn) *RoomPlaybackService {
	timeLive := time.Now().Format(time.RFC3339)
	minioClient := minioService.GetMinioClient(config.Minio)
	postgresDB := postgresService.GetPostgresDB(config.Postgres)
	s := &RoomPlaybackService{
		conf:     config,
		natsConn: natsConn,

		timeLive:  timeLive,
		timeReady: "",

		postgresDB:  postgresDB,
		minioClient: minioClient,

		sessions: sync.Map{},
	}
	go s.start()
	return s
}

func (s *RoomPlaybackService) start() {
	log.Infof("--- Starting NATS Subscription ---")
	onStartRecordingSub, err :=
		s.natsConn.QueueSubscribe(constants.STARTPLAYBACK_TOPIC,
			constants.STARTRECORDING_TOPIC,
			s.onStartPlayback)
	if err != nil {
		log.Errorf("NATS QueueSubscription error: %s", err)
		os.Exit(1)
	}
	defer onStartRecordingSub.Unsubscribe()
	onEndRecordingSub, err :=
		s.natsConn.Subscribe(constants.ENDPLAYBACK_TOPIC+"*",
			s.onEndPlayback)
	if err != nil {
		log.Errorf("NATS Subscription error: %s", err)
		os.Exit(1)
	}
	defer onEndRecordingSub.Unsubscribe()

	log.Infof("--- Starting monitoring-API Server ---")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/liveness", s.getLiveness)
	router.GET("/readiness", s.getReadiness)

	s.timeReady = time.Now().Format(time.RFC3339)
	log.Infof("HTTP service starting at %s", s.conf.Playback.Addr)
	err = router.Run(s.conf.Playback.Addr)
	if err != nil {
		log.Errorf("http listener error: %s", err)
		os.Exit(1)
	}
}
