package playback

import (
	"os"

	natsDiscoveryClient "github.com/cloudwebrtc/nats-discovery/pkg/client"
	"github.com/cloudwebrtc/nats-discovery/pkg/discovery"
	natsRPC "github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	minioService "github.com/pion/ion/apps/minio"
	postgresService "github.com/pion/ion/apps/postgres"
	"github.com/pion/ion/pkg/ion"
	"github.com/pion/ion/pkg/proto"
	"github.com/pion/ion/pkg/runner"
	"github.com/pion/ion/pkg/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc/reflection"
)

type GlobalConf struct {
	Dc string `mapstructure:"dc"`
}

type LogConf struct {
	Level string `mapstructure:"level"`
}

type NatsConf struct {
	URL string `mapstructure:"url"`
}

type SignalConf struct {
	Addr string `mapstructure:"addr"`
}

type RoomSentryConf struct {
	Url string `mapstructure:"url"`
}

type PlaybackConf struct {
	Addr              string `mapstructure:"addr"`
	PlaybackId        string `mapstructure:"playbackId"`
	CheckForEmptyRoom bool   `mapstructure:"checkForEmptyRoom"`
	SystemUserId      string `mapstructure:"systemUserId"`
	SystemUsername    string `mapstructure:"systemUsername"`
}

type Config struct {
	Global     GlobalConf                   `mapstructure:"global"`
	Log        LogConf                      `mapstructure:"log"`
	Nats       NatsConf                     `mapstructure:"nats"`
	Postgres   postgresService.PostgresConf `mapstructure:"postgres"`
	Minio      minioService.MinioConf       `mapstructure:"minio"`
	Signal     SignalConf                   `mapstructure:"signal"`
	RoomSentry RoomSentryConf               `mapstructure:"roomsentry"`
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

// RoomPlayback represents a room-playback node
type RoomPlayback struct {
	// for standalone running
	runner.Service

	// HTTP room-playback service
	RoomPlaybackService

	// for distributed node running
	ion.Node
	natsConn         *nats.Conn
	natsDiscoveryCli *natsDiscoveryClient.Client

	// config
	conf Config
}

// New create a RoomPlayback node instance
func New() *RoomPlayback {
	api := &RoomPlayback{
		Node: ion.NewNode("room-playback-" + util.RandomString(6)),
	}
	return api
}

// Start RoomPlayback node
func (r *RoomPlayback) Start(conf Config, quitCh chan os.Signal) error {
	var err error

	log.Infof("r.conf.Nats.URL===%+v", r.conf.Nats.URL)
	err = r.Node.Start(conf.Nats.URL)
	if err != nil {
		r.Close()
		return err
	}

	ndc, err := natsDiscoveryClient.NewClient(r.Node.NatsConn())
	if err != nil {
		log.Errorf("failed to create discovery client: %v", err)
		ndc.Close()
		return err
	}

	r.natsDiscoveryCli = ndc
	r.natsConn = r.Node.NatsConn()
	r.RoomPlaybackService = *NewRoomPlaybackService(conf, quitCh)

	// Register reflection service on nats-rpc server.
	reflection.Register(r.Node.ServiceRegistrar().(*natsRPC.Server))

	node := discovery.Node{
		DC:      conf.Global.Dc,
		Service: proto.ServiceROOMPLAYBACK,
		NID:     r.Node.NID,
		RPC: discovery.RPC{
			Protocol: discovery.NGRPC,
			Addr:     conf.Nats.URL,
			//Params:   map[string]string{"username": "foo", "password": "bar"},
		},
	}

	go func() {
		err := r.Node.KeepAlive(node)
		if err != nil {
			log.Errorf("sfu.Node.KeepAlive(%v) error %v", r.Node.NID, err)
		}
	}()

	//Watch ALL nodes.
	go func() {
		err := r.Node.Watch(proto.ServiceALL)
		if err != nil {
			log.Errorf("Node.Watch(proto.ServiceALL) error %v", err)
		}
	}()

	return nil
}

// Close all
func (s *RoomPlayback) Close() {
	s.Node.Close()
}
