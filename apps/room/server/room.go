package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"

	natsDiscoveryClient "github.com/cloudwebrtc/nats-discovery/pkg/client"
	"github.com/cloudwebrtc/nats-discovery/pkg/discovery"
	natsRPC "github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/cloudwebrtc/nats-grpc/pkg/rpc/reflection"
	"github.com/google/uuid"
	minio "github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"

	constants "github.com/pion/ion/apps/constants"
	minioService "github.com/pion/ion/apps/minio"
	postgresService "github.com/pion/ion/apps/postgres"
	room "github.com/pion/ion/apps/room/proto"
	"github.com/pion/ion/pkg/db"
	"github.com/pion/ion/pkg/ion"
	"github.com/pion/ion/pkg/proto"
	"github.com/pion/ion/pkg/runner"
	"github.com/pion/ion/pkg/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type global struct {
	Dc string `mapstructure:"dc"`
}

type logConf struct {
	Level string `mapstructure:"level"`
}

type natsConf struct {
	URL string `mapstructure:"url"`
}

type RoomMgmtConf struct {
	ReservedUsernames  []string `mapstructure:"reserved_usernames"`
	SystemUserIdPrefix string   `mapstructure:"systemUserIdPrefix"`
	PlaybackIdPrefix   string   `mapstructure:"playbackIdPrefix"`
}

// Config for room node
type Config struct {
	runner.ConfigBase
	Global   global                       `mapstructure:"global"`
	Log      logConf                      `mapstructure:"log"`
	Nats     natsConf                     `mapstructure:"nats"`
	Redis    db.Config                    `mapstructure:"redis"`
	Postgres postgresService.PostgresConf `mapstructure:"postgres"`
	Minio    minioService.MinioConf       `mapstructure:"minio"`
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

// Room represents a Room which manage peers
type Room struct {
	sync.RWMutex
	sid    string
	peers  map[string]*Peer
	info   *room.Room
	update time.Time

	redis *db.Redis

	postgresDB       *sql.DB
	roomRecordSchema string

	minioClient *minio.Client
	bucketName  string

	systemUserIdPrefix string
}

type RoomServer struct {
	// for standalone running
	runner.Service

	// grpc room service
	RoomService
	RoomSignalService

	// for distributed node running
	ion.Node
	natsConn         *nats.Conn
	natsDiscoveryCli *natsDiscoveryClient.Client

	// config
	conf Config
}

// New create a room node instance
func New() *RoomServer {
	return &RoomServer{
		Node: ion.NewNode("room-" + util.RandomString(6)),
	}
}

// Load load config file
func (r *RoomServer) Load(confFile string) error {
	err := r.conf.Load(confFile)
	if err != nil {
		log.Errorf("config load error: %v", err)
		return err
	}
	return nil
}

// ConfigBase used for runner
func (r *RoomServer) ConfigBase() runner.ConfigBase {
	return &r.conf
}

// StartGRPC for standalone bin
func (r *RoomServer) StartGRPC(registrar grpc.ServiceRegistrar) error {
	var err error

	ndc, err := natsDiscoveryClient.NewClient(nil)
	if err != nil {
		log.Errorf("failed to create discovery client: %v", err)
		ndc.Close()
		return err
	}

	r.natsDiscoveryCli = ndc
	r.natsConn = nil
	r.RoomService = *NewRoomService(r.conf)
	log.Infof("NewRoomService r.conf.Redis=%+v r.redis=%+v", r.conf.Redis, r.redis)
	log.Infof("NewRoomService r.conf.Postgres=%+v r.postgres=%+v", r.conf.Postgres, r.postgresDB)
	r.RoomSignalService = *NewRoomSignalService(&r.RoomService)

	room.RegisterRoomServiceServer(registrar, &r.RoomService)
	room.RegisterRoomSignalServer(registrar, &r.RoomSignalService)

	return nil
}

// Start for distributed node
func (r *RoomServer) Start() error {
	var err error

	log.Infof("r.conf.Nats.URL===%+v", r.conf.Nats.URL)
	err = r.Node.Start(r.conf.Nats.URL)
	if err != nil {
		r.Close()
		return err
	}

	ndc, err := natsDiscoveryClient.NewClient(r.NatsConn())
	if err != nil {
		log.Errorf("failed to create discovery client: %v", err)
		ndc.Close()
		return err
	}

	r.natsDiscoveryCli = ndc
	r.natsConn = r.NatsConn()
	r.RoomService = *NewRoomService(r.conf)
	log.Infof("NewRoomService r.conf.Redis=%+v r.redis=%+v", r.conf.Redis, r.redis)
	log.Infof("NewRoomService r.conf.Postgres=%+v r.postgres=%+v", r.conf.Postgres, r.postgresDB)
	r.RoomSignalService = *NewRoomSignalService(&r.RoomService)

	if err != nil {
		r.Close()
		return err
	}

	room.RegisterRoomServiceServer(r.Node.ServiceRegistrar(), &r.RoomService)
	room.RegisterRoomSignalServer(r.Node.ServiceRegistrar(), &r.RoomSignalService)
	// Register reflection service on nats-rpc server.
	reflection.Register(r.Node.ServiceRegistrar().(*natsRPC.Server))

	node := discovery.Node{
		DC:      r.conf.Global.Dc,
		Service: proto.ServiceROOM,
		NID:     r.Node.NID,
		RPC: discovery.RPC{
			Protocol: discovery.NGRPC,
			Addr:     r.conf.Nats.URL,
		},
	}

	go func() {
		err := r.Node.KeepAlive(node)
		if err != nil {
			log.Errorf("Room.Node.KeepAlive(%v) error %v", r.Node.NID, err)
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

func (s *RoomServer) Close() {
	s.RoomService.Close()
	s.Node.Close()
}

// newRoom creates a new room instance
func newRoom(sid, systemUid string,
	redis *db.Redis,
	postgresDB *sql.DB,
	roomRecordSchema string,
	minioClient *minio.Client,
	bucketName string) *Room {
	r := &Room{
		sid:    sid,
		peers:  make(map[string]*Peer),
		update: time.Now(),

		redis: redis,

		postgresDB:       postgresDB,
		roomRecordSchema: roomRecordSchema,

		minioClient: minioClient,
		bucketName:  bucketName,

		systemUserIdPrefix: systemUid,
	}
	return r
}

// Room name
func (r *Room) Name() string {
	return r.info.Name
}

// SID room id
func (r *Room) SID() string {
	return r.sid
}

// addPeer add a peer to room
func (r *Room) addPeer(p *Peer) {
	event := &room.PeerEvent{
		Peer:  p.info,
		State: room.PeerState_JOIN,
	}

	r.broadcastPeerEvent(event)

	r.Lock()
	p.room = r
	r.peers[p.info.Uid] = p
	r.update = time.Now()
	r.Unlock()
}

// func (r *Room) roomLocked() bool {
// 	r.RLock()
// 	defer r.RUnlock()
// 	r.update = time.Now()
// 	return r.info.Lock
// }

// getPeer get a peer by peer id
func (r *Room) getPeer(uid string) *Peer {
	r.RLock()
	defer r.RUnlock()
	return r.peers[uid]
}

// getPeers get peers in the room
func (r *Room) getPeers() []*Peer {
	r.RLock()
	defer r.RUnlock()
	p := make([]*Peer, 0, len(r.peers))
	for _, peer := range r.peers {
		p = append(p, peer)
	}
	return p
}

// delPeer delete a peer in the room
func (r *Room) delPeer(p *Peer) int {
	uid := p.info.Uid
	r.Lock()
	r.update = time.Now()
	found := r.peers[uid] == p
	if !found {
		r.Unlock()
		return -1
	}

	delete(r.peers, uid)
	peerCount := len(r.peers)
	r.Unlock()

	event := &room.PeerEvent{
		Peer:  p.info,
		State: room.PeerState_LEAVE,
	}

	key := util.GetRedisPeerKey(p.info.Sid, uid)
	err := r.redis.Del(key)
	if err != nil {
		log.Errorf("err=%v", err)
	}

	r.broadcastPeerEvent(event)

	return peerCount
}

// count return count of peers in room
func (r *Room) count() int {
	r.RLock()
	defer r.RUnlock()
	return len(r.peers)
}

func (r *Room) broadcastRoomEvent(uid string, event *room.Reply) {
	log.Infof("broadcastRoomEvent=%+v", event)
	peers := r.getPeers()
	r.update = time.Now()
	for _, p := range peers {
		if p.UID() == uid {
			continue
		}

		if err := p.send(event); err != nil {
			log.Errorf("send data to peer(%s) error: %v", p.info.Uid, err)
		}
	}
}

func (r *Room) broadcastPeerEvent(event *room.PeerEvent) {
	if strings.HasPrefix(event.Peer.Uid, r.systemUserIdPrefix) {
		return
	}

	go r.insertPeerEvent(
		PeerEvent{
			time.Now(),
			event.State,
			event.Peer.Uid,
			event.Peer.DisplayName})

	log.Infof("broadcastPeerEvent=%+v", event)
	peers := r.getPeers()
	r.update = time.Now()
	for _, p := range peers {
		if p.info.Uid == event.Peer.Uid {
			continue
		}
		if err := p.sendPeerEvent(event); err != nil {
			log.Errorf("send data to peer(%s) error: %v", p.info.Uid, err)
		}
	}
}

func (r *Room) sendMessage(msg *room.Message) {
	log.Infof("msg=%+v", msg)
	r.update = time.Now()
	from := msg.From
	to := msg.To
	dtype := msg.Type
	data := msg.Payload
	log.Debugf("Room.onMessage %v => %v, type: %v, data: %v", from, to, dtype, data)

	isParticipant := strings.HasPrefix(from, r.systemUserIdPrefix)
	peers := r.getPeers()
	for _, p := range peers {
		if isParticipant {
			break
		}
		if from == p.info.Uid {
			isParticipant = true
		}
	}
	if !isParticipant {
		log.Warnf("sender not found in room, maybe the peer was kicked")
		return
	}

	go r.insertChat(data)

	if to == "all" {
		r.broadcastRoomEvent(
			from,
			&room.Reply{
				Payload: &room.Reply_Message{
					Message: msg,
				},
			},
		)
		return
	}

	for _, p := range peers {
		isRecipient := to == p.info.Uid
		if strings.HasPrefix(p.info.Uid, r.systemUserIdPrefix) {
			isRecipient = true
		}
		if isRecipient {
			if err := p.sendMessage(msg); err != nil {
				log.Errorf("send msg to peer(%s) error: %v", p.info.Uid, err)
			}
		}
	}
}

type PeerEvent struct {
	timestamp time.Time
	state     room.PeerState
	peerId    string
	peerName  string
}

func (r *Room) insertPeerEvent(peerEvent PeerEvent) {
	if r.postgresDB == nil &&
		r.roomRecordSchema == "" &&
		r.minioClient == nil &&
		r.bucketName == "" {
		return
	}
	var err error
	insertStmt := `INSERT INTO "` + r.roomRecordSchema + `"."peerEvent"(
					"id",
					"roomId",
					"timestamp",
					"state",
					"peerId",
					"peerName")
					VALUES($1, $2, $3, $4, $5, $6)`
	dbId := uuid.NewString()
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = r.postgresDB.Exec(insertStmt,
			dbId,
			r.sid,
			peerEvent.timestamp,
			peerEvent.state,
			peerEvent.peerId,
			peerEvent.peerName)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), constants.DUP_PK) {
			dbId = uuid.NewString()
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
		return
	}
	peerEvent = PeerEvent{}
}

type ChatPayload struct {
	Msg *Payload `json:"msg,omitempty"`
}

type Payload struct {
	Uid              *string     `json:"uid,omitempty"`
	Name             *string     `json:"name,omitempty"`
	MimeType         *string     `json:"mimeType,omitempty"`
	Text             *string     `json:"text,omitempty"`
	Timestamp        *time.Time  `json:"timestamp,omitempty"`
	Base64File       *Attachment `json:"base64File,omitempty"`
	IgnoreByRecorder *bool       `json:"ignoreByRecorder,omitempty"`
}

type Attachment struct {
	Name *string `json:"name,omitempty"`
	Size *int    `json:"size,omitempty"`
	Data *string `json:"data,omitempty"`
}

func (r *Room) insertChat(data []byte) {
	if r.postgresDB == nil &&
		r.roomRecordSchema == "" &&
		r.minioClient == nil &&
		r.bucketName == "" {
		return
	}
	var err error
	var chatPayload ChatPayload
	err = json.Unmarshal(data, &chatPayload)
	if err != nil {
		log.Errorf("error decoding chat message %s", err)
		return
	}

	if chatPayload.Msg.IgnoreByRecorder != nil {
		log.Infof("not recording this chat message which has IgnoreByRecorder")
		return
	}
	if chatPayload.Msg.Uid == nil {
		log.Errorf("chat message has no sender id")
		return
	}
	if chatPayload.Msg.Name == nil {
		log.Errorf("chat message has no sender name")
		return
	}
	if chatPayload.Msg.Text == nil && chatPayload.Msg.Base64File == nil {
		jsonStr, _ := json.MarshalIndent(chatPayload, "", "    ")
		log.Warnf("chat message is on neither text nor file type:\n%s", jsonStr)
		return
	}
	if chatPayload.Msg.Timestamp == nil {
		timeStamp := time.Now()
		chatPayload.Msg.Timestamp = &timeStamp
	}
	if chatPayload.Msg.Text == nil {
		text := ""
		chatPayload.Msg.Text = &text
	}
	if chatPayload.Msg.MimeType == nil {
		mimeType := ""
		chatPayload.Msg.MimeType = &mimeType
	}

	r.storeChat(chatPayload)
	data = nil
}

func (r *Room) storeChat(chatPayload ChatPayload) {
	var err error

	fileName := ""
	fileSize := 0
	filePath := ""
	if chatPayload.Msg.Base64File != nil {
		if chatPayload.Msg.Base64File.Data == nil {
			log.Errorf("chat attachment has no data")
			return
		}
		if chatPayload.Msg.Base64File.Name == nil {
			log.Errorf("chat attachment has no name")
			return
		}
		if chatPayload.Msg.Base64File.Size == nil {
			log.Errorf("chat attachment has no size")
			return
		}
		if r.minioClient == nil {
			log.Errorf("error connecting to attachment storage")
			return
		}
		fileName = *chatPayload.Msg.Base64File.Name
		fileSize = *chatPayload.Msg.Base64File.Size
	}
	if r.postgresDB == nil {
		log.Errorf("error connecting to database")
		return
	}
	insertStmt := `INSERT INTO "` + r.roomRecordSchema + `"."chat"(
					"id",
					"roomId",
					"timestamp",
					"mimeType",
					"userId",
					"userName",
					"text",
					"fileName",
					"fileSize",
					"filePath")
					VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	objName := uuid.NewString()
	if chatPayload.Msg.Base64File != nil {
		filePath = constants.ATTACHMENT_FOLDERNAME + objName
	}
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = r.postgresDB.Exec(insertStmt,
			objName,
			r.sid,
			*chatPayload.Msg.Timestamp,
			*chatPayload.Msg.MimeType,
			*chatPayload.Msg.Uid,
			*chatPayload.Msg.Name,
			*chatPayload.Msg.Text,
			fileName,
			fileSize,
			filePath)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), constants.DUP_PK) {
			objName = uuid.NewString()
			if chatPayload.Msg.Base64File != nil {
				filePath = constants.ATTACHMENT_FOLDERNAME + objName
			}
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
		return
	}

	if chatPayload.Msg.Base64File != nil {
		data := bytes.NewReader([]byte(*chatPayload.Msg.Base64File.Data))
		var uploadInfo minio.UploadInfo
		for retry := 0; retry < constants.RETRY_COUNT; retry++ {
			uploadInfo, err = r.minioClient.PutObject(context.Background(),
				r.bucketName,
				r.sid+filePath,
				data,
				int64(len(*chatPayload.Msg.Base64File.Data)),
				minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err == nil {
				break
			}
			time.Sleep(constants.RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not upload attachment: %s", err)
		}
		log.Infof("successfully uploaded bytes: ", uploadInfo)
	}
	log.Infof("insert chat completed")
}
