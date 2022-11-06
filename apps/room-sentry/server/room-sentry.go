package server

import (
	"os"

	natsDiscoveryClient "github.com/cloudwebrtc/nats-discovery/pkg/client"
	"github.com/cloudwebrtc/nats-discovery/pkg/discovery"
	natsRPC "github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
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

type PostgresConf struct {
	Addr             string `mapstructure:"addr"`
	User             string `mapstructure:"user"`
	Password         string `mapstructure:"password"`
	Database         string `mapstructure:"database"`
	RoomMgmtSchema   string `mapstructure:"roomMgmtSchema"`
	RoomRecordSchema string `mapstructure:"roomRecordSchema"`
}

type SignalConf struct {
	Addr string `mapstructure:"addr"`
}

type RoomSentryConf struct {
	PollInSeconds  int      `mapstructure:"pollInSeconds"`
	Addr           string   `mapstructure:"address"`
	SystemUserId   string   `mapstructure:"systemUserId"`
	SystemUsername string   `mapstructure:"systemUsername"`
	HttpEndpoints  []string `mapstructure:"httpEndpoints"`
}

type Config struct {
	Global     GlobalConf     `mapstructure:"global"`
	Log        LogConf        `mapstructure:"log"`
	Nats       NatsConf       `mapstructure:"nats"`
	Postgres   PostgresConf   `mapstructure:"postgres"`
	Signal     SignalConf     `mapstructure:"signal"`
	RoomSentry RoomSentryConf `mapstructure:"roomsentry"`
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

// RoomSentry represents a room-sentry node
type RoomSentry struct {
	// for standalone running
	runner.Service

	// HTTP room-sentry service
	RoomSentryService

	// for distributed node running
	ion.Node
	natsConn         *nats.Conn
	natsDiscoveryCli *natsDiscoveryClient.Client

	// config
	conf Config
}

// New create a RoomSentry node instance
func New() *RoomSentry {
	api := &RoomSentry{
		Node: ion.NewNode("room-sentry-" + util.RandomString(6)),
	}
	return api
}

// Start RoomSentry node
func (r *RoomSentry) Start(conf Config) error {
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
	r.RoomSentryService = *NewRoomMgmtSentryService(conf)

	// Register reflection service on nats-rpc server.
	reflection.Register(r.Node.ServiceRegistrar().(*natsRPC.Server))

	node := discovery.Node{
		DC:      conf.Global.Dc,
		Service: proto.ServiceROOMSENTRY,
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
func (s *RoomSentry) Close() {
	s.Node.Close()
}
