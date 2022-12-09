package sentry

import (
	"os"

	log "github.com/pion/ion-log"
	postgresService "github.com/pion/ion/apps/postgres"
	"github.com/pion/ion/pkg/util"
	"github.com/spf13/viper"
)

type LogConf struct {
	Level string `mapstructure:"level"`
}

type NatsConf struct {
	URL string `mapstructure:"url"`
}

type SignalConf struct {
	Addr string `mapstructure:"addr"`
}

type RoomMgmtConf struct {
	SystemUserIdPrefix string `mapstructure:"systemUserIdPrefix"`
	SystemUsername     string `mapstructure:"systemUsername"`
}

type RoomSentryConf struct {
	PollInSeconds int    `mapstructure:"pollInSeconds"`
	Addr          string `mapstructure:"address"`
}

type Config struct {
	Log        LogConf                      `mapstructure:"log"`
	Nats       NatsConf                     `mapstructure:"nats"`
	Postgres   postgresService.PostgresConf `mapstructure:"postgres"`
	Signal     SignalConf                   `mapstructure:"signal"`
	RoomMgmt   RoomMgmtConf                 `mapstructure:"roommgmt"`
	RoomSentry RoomSentryConf               `mapstructure:"roomsentry"`
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
		log.Errorf("config file %s read failed. %s\n", file, err)
		return err
	}

	err = unmarshal(c)
	if err != nil {
		return err
	}
	if err != nil {
		log.Errorf("config file %s loaded failed. %s\n", file, err)
		return err
	}

	log.Infof("config %s load ok!", file)
	return nil
}

// RoomSentry represents a room-sentry node
type RoomSentry struct {
	// HTTP room-sentry service
	RoomSentryService
}

// New create a RoomSentry node instance
func New() *RoomSentry {
	return &RoomSentry{}
}

// Start RoomSentry node
func (r *RoomSentry) Start(conf Config) error {
	var err error

	log.Infof("conf.Nats.URL===%+v", conf.Nats.URL)
	natsConn, err := util.NewNatsConn(conf.Nats.URL)
	if err != nil {
		log.Errorf("new nats conn error %s", err)
		return err
	}

	r.RoomSentryService = *NewRoomMgmtSentryService(conf, natsConn)

	return nil
}
