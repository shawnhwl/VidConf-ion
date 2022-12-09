package mgmt

import (
	"os"

	log "github.com/pion/ion-log"
	minioService "github.com/pion/ion/apps/minio"
	postgresService "github.com/pion/ion/apps/postgres"
	"github.com/pion/ion/pkg/db"
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
	Addr               string `mapstructure:"address"`
	SystemUserIdPrefix string `mapstructure:"systemUserIdPrefix"`
	PlaybackIdPrefix   string `mapstructure:"playbackIdPrefix"`
}

type Config struct {
	Log      LogConf                      `mapstructure:"log"`
	Nats     NatsConf                     `mapstructure:"nats"`
	Redis    db.Config                    `mapstructure:"redis"`
	Postgres postgresService.PostgresConf `mapstructure:"postgres"`
	Minio    minioService.MinioConf       `mapstructure:"minio"`
	Signal   SignalConf                   `mapstructure:"signal"`
	RoomMgmt RoomMgmtConf                 `mapstructure:"roommgmt"`
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

// RoomMgmt represents a room-mgmt instance
type RoomMgmt struct {
	// HTTP room-mgmt service
	RoomMgmtService
}

// New create a RoomMgmt node instance
func New() *RoomMgmt {
	return &RoomMgmt{}
}

// Start RoomMgmt node
func (r *RoomMgmt) Start(conf Config) error {
	var err error

	log.Infof("r.conf.Nats.URL===%+v", conf.Nats.URL)
	natsConn, err := util.NewNatsConn(conf.Nats.URL)
	if err != nil {
		log.Errorf("new nats conn error %s", err)
		return err
	}

	r.RoomMgmtService = *NewRoomMgmtService(conf, natsConn)

	return nil
}
