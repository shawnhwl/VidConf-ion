package recorder

import (
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	"github.com/pion/ion/pkg/db"
	"github.com/spf13/viper"
)

const (
	KICKED string = "kicked"
)

type LogConf struct {
	Level string `mapstructure:"level"`
}

type RecorderConf struct {
	Addr             string `mapstructure:"address"`
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
	Redis    db.Config    `mapstructure:"redis"`
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
	timestamp time.Time
	state     sdk.PeerState
	peer      sdk.PeerInfo
}

type ChatRecord struct {
	timestamp time.Time
	data      map[string]interface{}
}

// RoomRecord represents a room-recorder node
type RoomRecord struct {
	conf   Config
	quitCh chan os.Signal

	timeLive       string
	timeReady      string
	roomService    *sdk.Room
	roomRTC        *sdk.RTC
	redisDB        *db.Redis
	roomid         string
	chopInterval   time.Duration
	systemUid      string
	systemUsername string

	chats      []ChatRecord
	peerevents []PeerEventRecord
}

func (s *RoomRecord) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomRecord) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func NewRoomRecorder(config Config, quitCh chan os.Signal) {
	timeLive := time.Now().Format(time.RFC3339)

	log.Infof("--- Testing Redis Connectionr ---")
	redis_db := db.NewRedis(config.Redis)
	if redis_db == nil {
		log.Panicf("connection to %s fail", config.Redis.Addrs)
	}
	defer redis_db.Close()

	log.Infof("--- Connecting to Room Signal ---")
	log.Infof("attempt gRPC connection to %s", config.Signal.Addr)
	sdk_connector := sdk.NewConnector(config.Signal.Addr)
	if sdk_connector == nil {
		log.Panicf("connection to %s fail", config.Signal.Addr)
	}
	sdk_roomService := sdk.NewRoom(sdk_connector)
	defer sdk_roomService.Close()
	sdk_rtc, err := sdk.NewRTC(sdk_connector)
	if err != nil {
		log.Panicf("rtc connector fail: %s", err)
	}
	defer sdk_rtc.Close()

	s := &RoomRecord{
		conf:           config,
		quitCh:         quitCh,
		timeLive:       timeLive,
		roomService:    sdk_roomService,
		roomRTC:        sdk_rtc,
		redisDB:        redis_db,
		roomid:         config.Recorder.Roomid,
		chopInterval:   time.Duration(config.Recorder.ChoppedInSeconds) * time.Second,
		systemUid:      config.Recorder.SystemUid,
		systemUsername: config.Recorder.SystemUsername,
		chats:          make([]ChatRecord, 0),
		peerevents:     make([]PeerEventRecord, 0),
	}
	log.Infof("config.Recorder.Roomid %s", config.Recorder.Roomid)
	log.Infof("s.roomid", s.roomid)
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
