package playback

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	minio "github.com/minio/minio-go/v7"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	room "github.com/pion/ion/apps/room/proto"
	"github.com/pion/webrtc/v3"
)

type PeerEvent struct {
	timestamp time.Time
	state     room.PeerState
}

type PeerEvents []PeerEvent

func (p PeerEvents) Len() int {
	return len(p)
}

func (p PeerEvents) Less(i, j int) bool {
	return p[i].timestamp.Before(p[j].timestamp)
}

func (p PeerEvents) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type TrackEvent struct {
	timestamp time.Time
	state     room.PeerState
	tracks    []sdk.TrackInfo
}

type TrackEvents []TrackEvent

func (p TrackEvents) Len() int {
	return len(p)
}

func (p TrackEvents) Less(i, j int) bool {
	return p[i].timestamp.Before(p[j].timestamp)
}

func (p TrackEvents) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
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

type OnTrack struct {
	id          string
	timestamp   time.Time
	trackId     string
	trackRemote TrackRemote
}

type Track struct {
	Timestamp time.Time
	Data      []byte
}

type Tracks []Track

func (p Tracks) Len() int {
	return len(p)
}

func (p Tracks) Less(i, j int) bool {
	return p[i].Timestamp.Before(p[j].Timestamp)
}

func (p Tracks) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// PlaybackPeer represents a room-playback peer instance
type PlaybackPeer struct {
	conf     Config
	waitPeer *sync.WaitGroup

	postgresDB       *sql.DB
	roomMgmtSchema   string
	roomRecordSchema string

	minioClient *minio.Client
	bucketName  string

	roomId        string
	playbackId    string
	peerId        string
	peerName      string
	roomStartTime time.Time

	joinRoomCh     chan struct{}
	isSdkConnected bool

	sdkConnector *sdk.Connector
	roomService  *sdk.Room
	roomRTC      *sdk.RTC

	peerEvents   PeerEvents
	peerEventId  int
	trackEvents  TrackEvents
	trackEventId int
	onTracks     map[string]OnTrack
	tracks       map[string]Tracks
	trackId      map[string]int
}

func (s *RoomPlaybackService) NewPlaybackPeer(peerId, peerName string) *PlaybackPeer {
	p := &PlaybackPeer{
		conf:     s.conf,
		waitPeer: s.waitPeer,

		postgresDB:       s.postgresDB,
		roomMgmtSchema:   s.roomMgmtSchema,
		roomRecordSchema: s.roomRecordSchema,

		minioClient: s.minioClient,
		bucketName:  s.bucketName,

		roomId:        s.roomId,
		playbackId:    s.playbackId,
		peerId:        peerId,
		peerName:      peerName,
		roomStartTime: s.roomStartTime,

		joinRoomCh:     make(chan struct{}, 32),
		isSdkConnected: true,

		peerEvents:   make(PeerEvents, 0),
		peerEventId:  0,
		trackEvents:  make(TrackEvents, 0),
		trackEventId: 0,
		onTracks:     make(map[string]OnTrack),
		tracks:       make(map[string]Tracks),
		trackId:      make(map[string]int),
	}
	go p.preparePlaybackPeer()
	return p
}

func (p *PlaybackPeer) preparePlaybackPeer() {
	p.waitPeer.Add(1)
	defer p.waitPeer.Done()

	var err error
	var peerEventRows *sql.Rows
	queryStmt := `SELECT "timestamp", "state"
					FROM "` + p.roomRecordSchema + `"."peerEvent"
					WHERE "roomId"=$1 AND "peerId"=$2`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		peerEventRows, err = p.postgresDB.Query(queryStmt, p.roomId, p.peerId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		os.Exit(1)
	}
	defer peerEventRows.Close()
	for peerEventRows.Next() {
		var peerEvent PeerEvent
		err := peerEventRows.Scan(&peerEvent.timestamp, &peerEvent.state)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
		}
		p.peerEvents = append(p.peerEvents, peerEvent)
	}
	sort.Sort(p.peerEvents)

	var trackEventRows *sql.Rows
	queryStmt = `SELECT "id", "timestamp", "state"
					FROM "` + p.roomRecordSchema + `"."trackEvent"
					WHERE "roomId"=$1 AND "peerId"=$2`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		trackEventRows, err = p.postgresDB.Query(queryStmt, p.roomId, p.peerId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		os.Exit(1)
	}
	defer trackEventRows.Close()
	for trackEventRows.Next() {
		var trackEventId string
		var trackEvent TrackEvent
		err := trackEventRows.Scan(&trackEventId,
			&trackEvent.timestamp,
			&trackEvent.state)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
		}
		trackEvent.tracks = make([]sdk.TrackInfo, 0)
		var trackInfoRows *sql.Rows
		queryStmt = `SELECT "trackId",
							"kind",
							"muted",
							"type",
							"streamId",
							"label",
							"subscribe",
							"layer",
							"direction",
							"width",
							"height",
							"frameRate"
						FROM "` + p.roomRecordSchema + `"."trackInfo"
						WHERE "roomId"=$1 AND "trackEventId"=$2`
		for retry := 0; retry < RETRY_COUNT; retry++ {
			trackInfoRows, err = p.postgresDB.Query(queryStmt,
				p.roomId,
				trackEventId)
			if err == nil {
				break
			}
			time.Sleep(RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
		}
		defer trackInfoRows.Close()
		for trackInfoRows.Next() {
			var track sdk.TrackInfo
			err := trackInfoRows.Scan(&track.Id,
				&track.Kind,
				&track.Muted,
				&track.Type,
				&track.StreamId,
				&track.Label,
				&track.Subscribe,
				&track.Layer,
				&track.Direction,
				&track.Width,
				&track.Height,
				&track.FrameRate)
			if err != nil {
				log.Errorf("could not query database: %s", err)
				os.Exit(1)
			}
			trackEvent.tracks = append(trackEvent.tracks, track)
		}
		p.trackEvents = append(p.trackEvents, trackEvent)
	}
	sort.Sort(p.trackEvents)

	for _, trackEvent := range p.trackEvents {
		for _, track := range trackEvent.tracks {
			var onTrackRows *sql.Rows
			queryStmt = `SELECT "id",
								"trackId",
								"timestamp",
								"trackRemote"
							FROM "` + p.roomRecordSchema + `"."onTrack"
							WHERE "roomId"=$1 AND "trackId"=$2`
			for retry := 0; retry < RETRY_COUNT; retry++ {
				onTrackRows, err = p.postgresDB.Query(queryStmt, p.roomId, track.Id)
				if err == nil {
					break
				}
				time.Sleep(RETRY_DELAY)
			}
			if err != nil {
				log.Errorf("could not query database: %s", err)
				os.Exit(1)
			}
			defer onTrackRows.Close()
			for onTrackRows.Next() {
				var onTrack OnTrack
				var trackRemote string
				err := onTrackRows.Scan(&onTrack.id,
					&onTrack.trackId,
					&onTrack.timestamp,
					&trackRemote)
				if err != nil {
					log.Errorf("could not query database: %s", err)
					os.Exit(1)
				}
				json.Unmarshal([]byte(trackRemote), &onTrack.trackRemote)
				p.onTracks[track.Id] = onTrack
			}
		}
	}

	for key := range p.onTracks {
		var trackRows *sql.Rows
		queryStmt = `SELECT "filePath"
						FROM "` + p.roomRecordSchema + `"."trackStream"
						WHERE "roomId"=$1 AND "onTrackId"=$2`
		for retry := 0; retry < RETRY_COUNT; retry++ {
			trackRows, err = p.postgresDB.Query(queryStmt,
				p.roomId,
				p.onTracks[key].id)
			if err == nil {
				break
			}
			time.Sleep(RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not query database: %s", err)
			os.Exit(1)
		}
		defer trackRows.Close()
		tracks := make(Tracks, 0)
		for trackRows.Next() {
			var filePath string
			err := trackRows.Scan(&filePath)
			if err != nil {
				log.Errorf("could not query database: %s", err)
				os.Exit(1)
			}
			object, err := p.minioClient.GetObject(context.Background(),
				p.bucketName,
				p.roomId+filePath,
				minio.GetObjectOptions{})
			if err != nil {
				log.Errorf("could not download attachment: %s", err)
				continue
			}
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(object)
			if err != nil {
				log.Errorf("could not process download: %s", err)
				continue
			}
			var trackData Tracks
			gob.NewDecoder(buf).Decode(&trackData)
			tracks = append(tracks, trackData...)
		}
		sort.Sort(tracks)
		p.tracks[key] = tracks
	}

	log.Infof("preparePlaybackPeer peerId '%s' completed", p.peerId)
}

func (p *PlaybackPeer) Start(exitCh chan struct{}) {
	p.joinRoomCh <- struct{}{}
	for {
		select {
		case <-exitCh:
			return
		case <-p.joinRoomCh:
			if p.isSdkConnected {
				p.isSdkConnected = false
				go p.openRoom()
			}
		default:
			time.Sleep(time.Microsecond)
			// send stream
		}
	}
}

func (p *PlaybackPeer) sendTrack(track *webrtc.TrackLocalStaticRTP, key string) {
	trackData := p.tracks[key]
	lastTick := time.Time{}
	for id := range trackData {
		if id != 0 {
			for {
				if 4*time.Since(lastTick) > trackData[id].Timestamp.Sub(trackData[id-1].Timestamp) {
					break
				}
				time.Sleep(time.Nanosecond)
			}
		}
		_, err := track.Write(trackData[id].Data)
		if err != nil {
			log.Errorf("send err: %s", err.Error())
		}
		lastTick = time.Now()
	}
}

func (p *PlaybackPeer) closeRoom() {
	if p.roomRTC != nil {
		p.roomRTC = nil
	}
	if p.roomService != nil {
		p.roomService = nil
	}
	if p.sdkConnector != nil {
		p.sdkConnector = nil
	}
}

func (p *PlaybackPeer) openRoom() {
	p.closeRoom()
	var err error
	for {
		time.Sleep(RECONNECTION_INTERVAL)
		p.sdkConnector, p.roomService, p.roomRTC, err = getRoomService(p.conf)
		if err == nil {
			p.isSdkConnected = true
			break
		}
	}
	p.joinRoom()
}

func (p *PlaybackPeer) joinRoom() {
	log.Infof("--- Joining Room ---")
	var err error
	p.roomService.OnJoin = p.onRoomJoin
	p.roomService.OnPeerEvent = p.onRoomPeerEvent
	p.roomService.OnMessage = p.onRoomMessage
	p.roomService.OnDisconnect = p.onRoomDisconnect
	p.roomService.OnError = p.onRoomError
	p.roomService.OnLeave = p.onRoomLeave
	p.roomService.OnRoomInfo = p.onRoomInfo

	// join room
	err = p.roomService.Join(
		sdk.JoinInfo{
			Sid:         p.playbackId,
			Uid:         PLAYBACK_PREFIX + p.peerId,
			DisplayName: PLAYBACK_PREFIX + p.peerName,
		},
	)
	if err != nil {
		p.joinRoomCh <- struct{}{}
		return
	}

	log.Infof("room.Join ok roomid=%v", p.roomId)
}

func (p *PlaybackPeer) onRoomJoin(success bool, info sdk.RoomInfo, err error) {
	log.Infof("onRoomJoin success = %v, info = %v, err = %v", success, info, err)

	p.roomRTC.OnTrack = p.onRTCTrack
	p.roomRTC.OnDataChannel = p.onRTCDataChannel
	p.roomRTC.OnError = p.onRTCError
	p.roomRTC.OnTrackEvent = p.onRTCTrackEvent
	p.roomRTC.OnSpeaker = p.onRTCSpeaker

	err = p.roomRTC.Join(p.playbackId, PLAYBACK_PREFIX+p.peerId)
	if err != nil {
		p.joinRoomCh <- struct{}{}
		return
	}

	for key := range p.onTracks {
		track, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: p.onTracks[key].trackRemote.Codec.MimeType},
			uuid.NewString(),
			p.peerId)
		if err != nil {
			log.Errorf("error creating TrackLocal: %s", err.Error())
			continue
		}
		_, err = p.roomRTC.Publish(track)
		if err != nil {
			log.Errorf("error creating TrackLocal: %s", err.Error())
		}
		go p.sendTrack(track, key)
	}
	log.Infof("rtc.Join ok roomid=%v", p.roomId)
}

func (p *PlaybackPeer) onRTCTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
}

func (p *PlaybackPeer) onRTCError(err error) {
	log.Errorf("onRTCError: %+v", err)
	p.joinRoomCh <- struct{}{}
}

func (p *PlaybackPeer) onRTCTrackEvent(event sdk.TrackEvent) {
}

func (p *PlaybackPeer) onRoomPeerEvent(state sdk.PeerState, peer sdk.PeerInfo) {
}

func (p *PlaybackPeer) onRoomMessage(from string, to string, data map[string]interface{}) {
}

func (p *PlaybackPeer) onRoomDisconnect(sid, reason string) {
	log.Infof("onRoomDisconnect sid = %+v, reason = %+v", sid, reason)
	p.joinRoomCh <- struct{}{}
}

func (p *PlaybackPeer) onRoomError(err error) {
	log.Errorf("onRoomError %v", err)
	p.joinRoomCh <- struct{}{}
}

func (p *PlaybackPeer) onRoomLeave(success bool, err error) {
}

func (p *PlaybackPeer) onRoomInfo(info sdk.RoomInfo) {
}

func (p *PlaybackPeer) onRTCDataChannel(dc *webrtc.DataChannel) {
}

func (p *PlaybackPeer) onRTCSpeaker(event []string) {
}
