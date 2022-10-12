package recorder

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	"github.com/pion/webrtc/v3"
	"github.com/spf13/viper"
)

type LogConf struct {
	Level string `mapstructure:"level"`
}

type PostgresConf struct {
	Addr     string `mapstructure:"addr"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
}

type RecorderConf struct {
	Addr             string `mapstructure:"addr"`
	Cert             string `mapstructure:"cert"`
	Key              string `mapstructure:"key"`
	Roomid           string `mapstructure:"roomid"`
	ChoppedInSeconds int    `mapstructure:"choppedInSeconds"`
	SystemUid        string `mapstructure:"system_userid"`
	SystemUsername   string `mapstructure:"system_username"`
}

type SignalConf struct {
	Addr string `mapstructure:"addr"`
}

type Config struct {
	Log      LogConf      `mapstructure:"log"`
	Postgres PostgresConf `mapstructure:"postgres"`
	Recorder RecorderConf `mapstructure:"recorder"`
	Signal   SignalConf   `mapstructure:"signal"`
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

type PeerEventRecord struct {
	timeElapsed time.Duration
	state       sdk.PeerState
	peerId      string
	peerName    string
}

type TrackEventRecord struct {
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

type OnTrackRecord struct {
	timeElapsed time.Duration
	trackRemote TrackRemote
}

type TrackRecord struct {
	timeElapsed time.Duration
	data        []byte
}

type ChatRecord struct {
	timeElapsed time.Duration
	data        map[string]interface{}
}

// RoomRecorder represents a room-recorder node
type RoomRecorder struct {
	conf   Config
	quitCh chan os.Signal

	timeLive       string
	timeReady      string
	roomService    *sdk.Room
	roomRTC        *sdk.RTC
	postgresDB     *sql.DB
	roomid         string
	chopInterval   time.Duration
	systemUid      string
	systemUsername string

	roomRecordId       string
	startTime          time.Time
	peerEventsSavedId  int
	peerEvents         []PeerEventRecord
	trackEventsSavedId int
	trackEvents        []TrackEventRecord
	onTracksSavedId    int
	onTracks           []OnTrackRecord
	chatsSavedId       int
	chats              []ChatRecord
	tracksSavedId      map[string]int
	tracks             map[string][]TrackRecord
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
	log.Infof("psqlconn: %s", psqlconn)
	var postgresDB *sql.DB
	for retry := 0; retry < DB_RETRY; retry++ {
		postgresDB, err = sql.Open("postgres", psqlconn)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to connect to database: %v\n", err)
	}
	for retry := 0; retry < DB_RETRY; retry++ {
		err = postgresDB.Ping()
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to ping database: %v\n", err)
	}
	createStmt := `CREATE TABLE IF NOT EXISTS
					"room"( "id"             UUID PRIMARY KEY,
							"name"           TEXT,
							"status"         TEXT NOT NULL,
							"startTime"      TIMESTAMP NOT NULL,
							"endTime"        TIMESTAMP NOT NULL,
							"allowedUserId"  TEXT ARRAY,
							"earlyEndReason" TEXT,
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
	createStmt = `CREATE TABLE IF NOT EXISTS
					"announcement"( "id"                    UUID PRIMARY KEY,
									"roomId"                UUID NOT NULL,
									"status"                TEXT NOT NULL,
									"message"               TEXT NOT NULL,
									"relativeFrom"          TEXT NOT NULL,
									"relativeTimeInSeconds" INT NOT NULL,
									"userId"                TEXT ARRAY,
									"createdAt"             TIMESTAMP NOT NULL,
									"createdBy"             TEXT NOT NULL,
									"updatedAt"             TIMESTAMP NOT NULL,
									"updatedBy"             TEXT NOT NULL,
									CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}

	createStmt = `CREATE TABLE IF NOT EXISTS
					"roomRecord"(	"id"        UUID PRIMARY KEY,
									"roomId"    UUID NOT NULL,
									"startTime" TIMESTAMP NOT NULL,
									"endTime"   TIMESTAMP NOT NULL,
									CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}
	createStmt = `CREATE TABLE IF NOT EXISTS
					"peerEvent"("id"           UUID PRIMARY KEY,
								"roomRecordId" UUID NOT NULL,
								"timeElapsed"  INTERVAL NOT NULL,
								"state"        INT NOT NULL,
								"peerId"       TEXT NOT NULL,
								"peerName"     TEXT NOT NULL,
								CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "roomRecord"(id) ON DELETE CASCADE)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}
	createStmt = `CREATE TABLE IF NOT EXISTS
					"trackEvent"(   "id"           UUID PRIMARY KEY,
									"roomRecordId" UUID NOT NULL,
									"timeElapsed"  INTERVAL NOT NULL,
									"state"        INT NOT NULL,
									"trackEventId" TEXT NOT NULL,
									CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "roomRecord"("id") ON DELETE CASCADE)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}
	createStmt = `CREATE TABLE IF NOT EXISTS
					"trackInfo"("id"           UUID PRIMARY KEY,
								"trackEventId" UUID NOT NULL,
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
								CONSTRAINT fk_trackEvent FOREIGN KEY("trackEventId") REFERENCES "trackEvent"("id") ON DELETE CASCADE)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}
	createStmt = `CREATE TABLE IF NOT EXISTS
					"onTrack"(  "id" UUID PRIMARY KEY,
								"roomRecordId" UUID NOT NULL,
								"timeElapsed" INTERVAL NOT NULL,
								"trackRemote" JSON NOT NULL,
								CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "roomRecord"("id") ON DELETE CASCADE)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}
	createStmt = `CREATE TABLE IF NOT EXISTS
					"trackStream"(  "id" UUID PRIMARY KEY,
									"onTrackId" UUID NOT NULL,
									"timeElapsed" INTERVAL NOT NULL,
									"filepath" TEXT NOT NULL,
									CONSTRAINT fk_onTrack FOREIGN KEY("onTrackId") REFERENCES "onTrack"("id") ON DELETE CASCADE)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}
	createStmt = `CREATE TABLE IF NOT EXISTS
					"chatStream"(   "id" UUID PRIMARY KEY,
									"roomRecordId" UUID NOT NULL,
									"timeElapsed" INTERVAL NOT NULL,
									"filepath" TEXT NOT NULL,
									CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "roomRecord"("id") ON DELETE CASCADE)`
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

	s := &RoomRecorder{
		conf:           config,
		quitCh:         quitCh,
		timeLive:       timeLive,
		roomService:    sdk_roomService,
		roomRTC:        sdk_rtc,
		postgresDB:     postgresDB,
		roomid:         config.Recorder.Roomid,
		chopInterval:   time.Duration(config.Recorder.ChoppedInSeconds) * time.Second,
		systemUid:      config.Recorder.SystemUid,
		systemUsername: config.Recorder.SystemUsername,

		peerEventsSavedId:  0,
		peerEvents:         make([]PeerEventRecord, 0),
		trackEventsSavedId: 0,
		trackEvents:        make([]TrackEventRecord, 0),
		onTracksSavedId:    0,
		onTracks:           make([]OnTrackRecord, 0),
		chatsSavedId:       0,
		chats:              make([]ChatRecord, 0),
		tracksSavedId:      make(map[string]int),
		tracks:             make(map[string][]TrackRecord),
	}
	return s
}

func (s *RoomRecorder) Start() {
	defer s.postgresDB.Close()
	defer s.roomService.Close()
	defer s.roomRTC.Close()

	err := s.getRoomsByRoomid(s.roomid)
	if err != nil {
		log.Panicf("Join room fail:%s", err.Error())
	}
	log.Infof("--- Joining Room ---")
	s.joinRoom()
	go s.RecorderSentinel()

	s.timeReady = time.Now().Format(time.RFC3339)
	log.Infof("--- Starting monitoring-API Server ---")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/liveness", s.getLiveness)
	router.GET("/readiness", s.getReadiness)
	if s.conf.Recorder.Cert != "" && s.conf.Recorder.Key != "" {
		log.Infof("HTTP service starting at %s", s.conf.Recorder.Addr)
		log.Panicf("%s", router.RunTLS(s.conf.Recorder.Addr, s.conf.Recorder.Cert, s.conf.Recorder.Key))
	} else {
		log.Infof("HTTP service starting at %s", s.conf.Recorder.Addr)
		log.Panicf("%s", router.Run(s.conf.Recorder.Addr))
	}
}
