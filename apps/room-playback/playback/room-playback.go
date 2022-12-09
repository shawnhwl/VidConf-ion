package playback

import (
	"os"

	log "github.com/pion/ion-log"
	minioService "github.com/pion/ion/apps/minio"
	postgresService "github.com/pion/ion/apps/postgres"
	"github.com/pion/ion/pkg/util"
	"github.com/spf13/viper"
)

type LogConf struct {
	Level string `mapstructure:"level"`
}

type StunConf struct {
	Urls []string `mapstructure:"urls"`
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
	PlaybackIdPrefix   string `mapstructure:"playbackIdPrefix"`
}

type PlaybackConf struct {
	Addr              string `mapstructure:"addr"`
	CheckForEmptyRoom bool   `mapstructure:"checkForEmptyRoom"`
}

type Config struct {
	Log        LogConf                      `mapstructure:"log"`
	Stunserver StunConf                     `mapstructure:"stunserver"`
	Nats       NatsConf                     `mapstructure:"nats"`
	Postgres   postgresService.PostgresConf `mapstructure:"postgres"`
	Minio      minioService.MinioConf       `mapstructure:"minio"`
	Signal     SignalConf                   `mapstructure:"signal"`
	RoomMgmt   RoomMgmtConf                 `mapstructure:"roommgmt"`
	Playback   PlaybackConf                 `mapstructure:"playback"`
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

// RoomPlayback represents a room-playback instance
type RoomPlayback struct {
	// HTTP room-playback service
	RoomPlaybackService
}

// New create a RoomPlayback node instance
func New() *RoomPlayback {
	return &RoomPlayback{}
}

// Start RoomPlayback node
func (r *RoomPlayback) Start(conf Config) error {
	var err error

	log.Infof("conf.Nats.URL===%+v", conf.Nats.URL)
	natsConn, err := util.NewNatsConn(conf.Nats.URL)
	if err != nil {
		log.Errorf("new nats conn error %s", err)
		return err
	}

	r.RoomPlaybackService = *NewRoomPlaybackService(conf, natsConn)

	return nil
}
