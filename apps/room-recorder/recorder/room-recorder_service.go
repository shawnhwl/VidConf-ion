package recorder

import (
	"database/sql"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	constants "github.com/pion/ion/apps/constants"
	minioService "github.com/pion/ion/apps/minio"
	postgresService "github.com/pion/ion/apps/postgres"
)

func (s *RoomRecorderService) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomRecorderService) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func (s *RoomRecorderService) onStartRecording(msg *nats.Msg) {
	log.Infof("onQueueSubscribe %s", msg.Subject)
	sessionId := string(msg.Data)
	session, err := s.newRoomRecorderSession(sessionId)
	if err != nil {
		msg.Respond([]byte(err.Error()))
		return
	}
	s.sessions.Store(sessionId, session)
	msg.Respond([]byte(""))
}

func (s *RoomRecorderService) onEndRecording(msg *nats.Msg) {
	log.Infof("onSubcribe %s", msg.Subject)
	sessionId := msg.Subject[len(constants.ENDRECORDING_TOPIC):]
	session, loaded := s.sessions.LoadAndDelete(sessionId)
	if !loaded {
		return
	}
	go session.(*RoomRecorderSession).close()
	msg.Respond([]byte(""))
}

// RoomRecorderService represents a room-recorder service instance
type RoomRecorderService struct {
	conf     Config
	natsConn *nats.Conn

	timeLive  string
	timeReady string

	postgresDB  *sql.DB
	minioClient *minio.Client

	exitChOnce sync.Once
	exitCh     chan struct{}
	waitUpload *sync.WaitGroup
	sessions   sync.Map
}

func NewRoomRecorderService(config Config, natsConn *nats.Conn) *RoomRecorderService {
	timeLive := time.Now().Format(time.RFC3339)
	minioClient := minioService.GetMinioClient(config.Minio)
	postgresDB := postgresService.GetPostgresDB(config.Postgres)
	s := &RoomRecorderService{
		conf:     config,
		natsConn: natsConn,

		timeLive:  timeLive,
		timeReady: "",

		postgresDB:  postgresDB,
		minioClient: minioClient,

		exitCh:     make(chan struct{}),
		waitUpload: new(sync.WaitGroup),
		sessions:   sync.Map{},
	}
	go s.start()
	return s
}

func (s *RoomRecorderService) start() {
	log.Infof("--- Starting NATS Subscription ---")
	onStartRecordingSub, err :=
		s.natsConn.QueueSubscribe(constants.STARTRECORDING_TOPIC,
			constants.STARTRECORDING_TOPIC,
			s.onStartRecording)
	if err != nil {
		log.Errorf("NATS QueueSubscription error: %s", err)
		os.Exit(1)
	}
	defer onStartRecordingSub.Unsubscribe()
	onEndRecordingSub, err :=
		s.natsConn.Subscribe(constants.ENDRECORDING_TOPIC+"*",
			s.onEndRecording)
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
	log.Infof("HTTP service starting at %s", s.conf.Recorder.Addr)
	err = router.Run(s.conf.Recorder.Addr)
	if err != nil {
		log.Errorf("http listener error: %s", err)
		os.Exit(1)
	}
}
