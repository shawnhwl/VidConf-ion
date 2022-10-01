package server

import (
	"os"

	natsDiscoveryClient "github.com/cloudwebrtc/nats-discovery/pkg/client"
	"github.com/cloudwebrtc/nats-discovery/pkg/discovery"
	natsRPC "github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	"github.com/pion/ion/pkg/db"
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

type RoomMgmtConf struct {
	PollInSeconds  int    `mapstructure:"pollInSeconds"`
	Addr           string `mapstructure:"address"`
	Cert           string `mapstructure:"cert"`
	Key            string `mapstructure:"key"`
	SystemUid      string `mapstructure:"system_userid"`
	SystemUsername string `mapstructure:"system_username"`
}

type WebAppConf struct {
	Url string `mapstructure:"url"`
}

type SignalConf struct {
	Addr string `mapstructure:"addr"`
}

type Config struct {
	Global   GlobalConf   `mapstructure:"global"`
	Log      LogConf      `mapstructure:"log"`
	Nats     NatsConf     `mapstructure:"nats"`
	Redis    db.Config    `mapstructure:"redis"`
	RoomMgmt RoomMgmtConf `mapstructure:"roommgmt"`
	WebApp   WebAppConf   `mapstructure:"webapp"`
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

// RoomMgmt represents a room-mgmt node
type RoomMgmt struct {
	// for standalone running
	runner.Service

	// HTTP room-mgmt service
	RoomMgmtService

	// for distributed node running
	ion.Node
	natsConn         *nats.Conn
	natsDiscoveryCli *natsDiscoveryClient.Client

	// config
	conf Config
}

// New create a RoomMgmt node instance
func New() *RoomMgmt {
	api := &RoomMgmt{
		Node: ion.NewNode("room-mgmt-" + util.RandomString(6)),
	}
	return api
}

// Start RoomMgmt node
func (r *RoomMgmt) Start(conf Config) error {
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
	r.RoomMgmtService = *NewRoomMgmtService(conf)

	// Register reflection service on nats-rpc server.
	reflection.Register(r.Node.ServiceRegistrar().(*natsRPC.Server))

	node := discovery.Node{
		DC:      conf.Global.Dc,
		Service: proto.ServiceROOMMGMT,
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
func (s *RoomMgmt) Close() {
	s.Node.Close()
}
