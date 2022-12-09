package playback

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	constants "github.com/pion/ion/apps/constants"
	"github.com/pion/webrtc/v3"
)

type TrackStream struct {
	Timestamp time.Time
	Data      []byte
}

type TrackStreams []TrackStream

func (p TrackStreams) Len() int {
	return len(p)
}

func (p TrackStreams) Less(i, j int) bool {
	return p[i].Timestamp.Before(p[j].Timestamp)
}

func (p TrackStreams) Swap(i, j int) {
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

	sessionCh      chan struct{}
	roomId         string
	sessionId      string
	peerId         string
	peerName       string
	orphanRemoteId string

	joinRoomCh     chan struct{}
	isSdkConnected bool

	sdkConnector *sdk.Connector
	roomService  *sdk.Room
	roomRTC      *sdk.RTC
	isRoomJoined bool

	trackStreams    map[string]TrackStreams
	lenTrackStreams map[string]int
	mimeTypes       map[string]string
	trackLocals     map[string]*webrtc.TrackLocalStaticRTP
	trackCh         []chan Ctrl

	ctrlCh chan Ctrl
}

func (s *RoomPlaybackSession) NewPlaybackPeer(peerId, peerName, orphanRemoteId string) *PlaybackPeer {
	p := &PlaybackPeer{
		conf:     s.conf,
		waitPeer: s.waitPeer,

		postgresDB:       s.postgresDB,
		roomMgmtSchema:   s.roomMgmtSchema,
		roomRecordSchema: s.roomRecordSchema,

		minioClient: s.minioClient,
		bucketName:  s.bucketName,

		sessionCh:      s.sessionCh,
		roomId:         s.roomId,
		sessionId:      s.sessionId,
		peerId:         peerId,
		peerName:       peerName,
		orphanRemoteId: orphanRemoteId,

		joinRoomCh:     make(chan struct{}, 32),
		isSdkConnected: true,
		roomService:    nil,
		roomRTC:        nil,
		isRoomJoined:   false,

		trackStreams:    make(map[string]TrackStreams),
		lenTrackStreams: make(map[string]int),
		mimeTypes:       make(map[string]string),
		trackLocals:     make(map[string]*webrtc.TrackLocalStaticRTP),
		trackCh:         make([]chan Ctrl, 0),

		ctrlCh: make(chan Ctrl, 32),
	}
	p.waitPeer.Add(1)
	defer p.waitPeer.Done()

	err := p.preparePlaybackPeer()
	if err != nil {
		return nil
	}
	go p.start()
	for {
		time.Sleep(10 * time.Second)
		if p.isRoomJoined {
			break
		}
		log.Warnf("%s waiting....", p.peerName)
	}
	return p
}

func (p *PlaybackPeer) preparePlaybackPeer() error {
	if p.orphanRemoteId != "" {
		return p.preparePlaybackOrphan()
	}

	log.Infof("preparePlaybackPeer peerId '%s'", p.peerId)
	var err error
	var trackEventRow *sql.Row
	queryStmt := `SELECT "trackRemoteIds"
					FROM "` + p.roomRecordSchema + `"."trackEvent"
					WHERE "roomId"=$1 AND "peerId"=$2`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		trackEventRow = p.postgresDB.QueryRow(queryStmt, p.roomId, p.peerId)
		if trackEventRow.Err() == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if trackEventRow.Err() != nil {
		log.Errorf("could not query database: %s", trackEventRow.Err())
		return trackEventRow.Err()
	}
	var trackRemoteIds pq.StringArray
	err = trackEventRow.Scan(&trackRemoteIds)
	if err != nil {
		log.Errorf("could not query database: %s", err)
		return err
	}
	hasTrack := false
	for _, trackRemoteId := range trackRemoteIds {
		var trackRow *sql.Row
		queryStmt = `SELECT "id", "mimeType"
						FROM "` + p.roomRecordSchema + `"."track"
						WHERE "roomId"=$1 AND "trackRemoteId"=$2`
		for retry := 0; retry < constants.RETRY_COUNT; retry++ {
			trackRow = p.postgresDB.QueryRow(queryStmt, p.roomId, trackRemoteId)
			if trackRow.Err() == nil {
				break
			}
			time.Sleep(constants.RETRY_DELAY)
		}
		if trackRow.Err() != nil {
			log.Errorf("could not query database")
			continue
		}
		var trackId string
		var mimeType string
		err = trackRow.Scan(&trackId, &mimeType)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			continue
		}

		var trackStreamRows *sql.Rows
		queryStmt = `SELECT "filePath"
						FROM "` + p.roomRecordSchema + `"."trackStream"
						WHERE "roomId"=$1 AND "trackId"=$2`
		for retry := 0; retry < constants.RETRY_COUNT; retry++ {
			trackStreamRows, err = p.postgresDB.Query(queryStmt,
				p.roomId,
				trackId)
			if err == nil {
				break
			}
			time.Sleep(constants.RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not query database: %s", err)
			continue
		}
		defer trackStreamRows.Close()
		trackStreams := make(TrackStreams, 0)
		for trackStreamRows.Next() {
			var filePath string
			err = trackStreamRows.Scan(&filePath)
			if err != nil {
				log.Errorf("could not query database: %s", err)
				continue
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
			var trackStream TrackStreams
			gob.NewDecoder(buf).Decode(&trackStream)
			trackStreams = append(trackStreams, trackStream...)
		}
		if len(trackStreams) == 0 {
			continue
		}
		sort.Sort(trackStreams)
		p.trackStreams[trackId] = trackStreams
		p.lenTrackStreams[trackId] = len(trackStreams)
		p.mimeTypes[trackId] = mimeType
		p.trackLocals[trackId], err = webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: mimeType},
			mimeType+sdk.RandomKey(8),
			p.peerId)
		if err != nil {
			log.Errorf("error creating TrackLocal: %s", err)
			continue
		}
		if strings.Contains(strings.ToUpper(mimeType), constants.MIME_AUDIO) {
			p.trackLocals[constants.MIME_VP8], err = webrtc.NewTrackLocalStaticRTP(
				webrtc.RTPCodecCapability{MimeType: constants.MIME_VP8},
				constants.MIME_VP8+sdk.RandomKey(8),
				p.peerId)
			if err != nil {
				log.Errorf("error creating TrackLocal: %s", err)
				continue
			}
		}
		trackCh := make(chan Ctrl, 32)
		p.trackCh = append(p.trackCh, trackCh)
		go p.sendTrack(trackId, trackCh)
		hasTrack = true
	}

	if !hasTrack {
		log.Errorf("preparePlaybackPeer peerId '%s' found no usable tracks", p.peerId)
		return errors.New("found no usable tracks")
	}
	log.Infof("preparePlaybackPeer peerId '%s' completed", p.peerId)
	return nil
}

func (p *PlaybackPeer) preparePlaybackOrphan() error {
	log.Infof("preparePlaybackPeer orphanRemoteId '%s'", p.orphanRemoteId)
	var err error
	var trackRow *sql.Row
	queryStmt := `SELECT "id", "mimeType"
						FROM "` + p.roomRecordSchema + `"."track"
						WHERE "roomId"=$1 AND "trackRemoteId"=$2`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		trackRow = p.postgresDB.QueryRow(queryStmt, p.roomId, p.orphanRemoteId)
		if trackRow.Err() == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if trackRow.Err() != nil {
		log.Errorf("could not query database: %s", trackRow.Err())
		return trackRow.Err()
	}
	var trackId string
	var mimeType string
	err = trackRow.Scan(&trackId, &mimeType)
	if err != nil {
		log.Errorf("could not query database: %s", err)
		return err
	}
	codecMimeType := strings.ToUpper(mimeType)
	if !strings.Contains(codecMimeType, constants.MIME_VIDEO) {
		log.Errorf("orphaned remoteTrackId '%s' is not screen share", p.orphanRemoteId)
		return errors.New("orphaned remoteTrackId is not screen share")
	}

	var trackStreamRows *sql.Rows
	queryStmt = `SELECT "filePath"
						FROM "` + p.roomRecordSchema + `"."trackStream"
						WHERE "roomId"=$1 AND "trackId"=$2`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		trackStreamRows, err = p.postgresDB.Query(queryStmt,
			p.roomId,
			trackId)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		return err
	}
	defer trackStreamRows.Close()
	trackStreams := make(TrackStreams, 0)
	for trackStreamRows.Next() {
		var filePath string
		err = trackStreamRows.Scan(&filePath)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			continue
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
		var trackStream TrackStreams
		gob.NewDecoder(buf).Decode(&trackStream)
		trackStreams = append(trackStreams, trackStream...)
	}
	if len(trackStreams) == 0 {
		return errors.New("found no usable tracks")
	}
	sort.Sort(trackStreams)
	p.trackStreams[trackId] = trackStreams
	p.lenTrackStreams[trackId] = len(trackStreams)
	p.mimeTypes[trackId] = mimeType
	p.trackLocals[trackId], err = webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: mimeType},
		mimeType+sdk.RandomKey(8),
		p.peerId)
	if err != nil {
		log.Errorf("error creating TrackLocal: %s", err)
		return err
	}
	trackCh := make(chan Ctrl, 32)
	p.trackCh = append(p.trackCh, trackCh)
	go p.sendTrack(trackId, trackCh)

	log.Infof("preparePlaybackPeer orphanRemoteId '%s' completed", p.orphanRemoteId)
	return nil
}

func (p *PlaybackPeer) start() {
	p.joinRoomCh <- struct{}{}
	for {
		select {
		case <-p.sessionCh:
			return
		case ctrl := <-p.ctrlCh:
			for id := range p.trackCh {
				p.trackCh[id] <- ctrl
			}
		case <-p.joinRoomCh:
			p.isRoomJoined = false
			if p.isSdkConnected {
				p.isSdkConnected = false
				go p.openRoom()
			}
		default:
			time.Sleep(time.Nanosecond)
		}
	}
}

func (p *PlaybackPeer) sendTrack(key string, trackCh chan Ctrl) {
	codecMimeType := strings.ToUpper(p.mimeTypes[key])
	isAudioCodec := false
	isVideoCodec := false
	isScreenShare := false
	if strings.Contains(codecMimeType, constants.MIME_AUDIO) {
		isAudioCodec = true
	}
	if strings.Contains(codecMimeType, constants.MIME_VIDEO) {
		if p.orphanRemoteId != "" {
			isScreenShare = true
		} else {
			isVideoCodec = true
		}
	}

	isRunning := false
	isPublishing := false
	needDummy := false
	var err error
	var speed10 time.Duration
	var playbackRefTime time.Time
	var actualRefTime time.Time
	trackIdx := 0
	maxTrackIdx := p.lenTrackStreams[key]
	trackStreams := p.trackStreams[key]
	trackInfo := constants.PLAYBACK_PREFIX + p.peerName
	if p.orphanRemoteId != "" {
		trackInfo = "orphanId"
	}
	trackInfo = trackInfo +
		"/" + p.mimeTypes[key] +
		"/'" + key +
		"'/isAudioCodec=" + strconv.FormatBool(isAudioCodec) +
		"/isVideoCodec=" + strconv.FormatBool(isVideoCodec) +
		"/isScreenShare=" + strconv.FormatBool(isScreenShare)

	for {
		select {
		case ctrl := <-trackCh:
			if ctrl.isPause {
				isRunning = false
				if isPublishing {
					isPublishing = false
					p.roomService.Leave(p.roomId, p.peerId)
					p.joinRoomCh <- struct{}{}
				}
			} else {
				if (isAudioCodec && ctrl.isAudio) ||
					(isVideoCodec && ctrl.isVideo) ||
					(isScreenShare && ctrl.isVideo) {
					needDummy = isAudioCodec && !ctrl.isVideo
					if !isPublishing {
						err = p.publishTrackLocal(trackInfo, key, needDummy)
						if err == nil {
							isPublishing = true
						}
					}
					isRunning = true
					speed10 = ctrl.speed10
					playbackRefTime = ctrl.playbackRefTime
					actualRefTime = ctrl.actualRefTime
					trackIdx = 0
					for ; trackIdx < maxTrackIdx; trackIdx++ {
						if trackStreams[trackIdx].Timestamp.After(playbackRefTime) {
							break
						}
					}
				}
			}
			log.Infof(` \n
						%s\n
						isRunning=%v\m
						ctrl.isAudio=%v\n
						ctrl.isVideo=%v\n
						speed=%v/%v\n
						playbackRefTime=%s\n
						actualRefTime=%s\n
						trackIdx=%d/%d\n
						startTime=%s`,
				trackInfo,
				isRunning,
				ctrl.isAudio,
				ctrl.isVideo,
				speed10, constants.PLAYBACK_SPEED10,
				playbackRefTime,
				actualRefTime,
				trackIdx, maxTrackIdx,
				trackStreams[0].Timestamp)
		default:
			time.Sleep(time.Nanosecond)
			if !isRunning {
				continue
			}
			if trackIdx >= maxTrackIdx {
				continue
			}
			if speed10*time.Since(actualRefTime) <
				constants.PLAYBACK_SPEED10*trackStreams[trackIdx].Timestamp.Sub(playbackRefTime) {
				continue
			}
			if !isPublishing {
				err = p.publishTrackLocal(trackInfo, key, needDummy)
				if err == nil {
					isPublishing = true
				}
			}
			if isPublishing {
				_, err = p.trackLocals[key].Write(trackStreams[trackIdx].Data)
				if err != nil {
					log.Errorf("send err: %s", err)
				}
			}
			trackIdx++
		}
	}
}

func (p *PlaybackPeer) publishTrackLocal(trackInfo, key string, needDummy bool) error {
	if !p.isRoomJoined {
		return errors.New("room is not joined")
	}

	_, err := p.roomRTC.Publish(p.trackLocals[key])
	if err != nil {
		log.Errorf("error publishing %s", err)
		return err
	}
	if needDummy {
		_, err = p.roomRTC.Publish(p.trackLocals[constants.MIME_VP8])
		if err != nil {
			log.Errorf("error publishing %s", err)
			return err
		}
	}
	return nil
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
		p.sdkConnector, p.roomService, p.roomRTC, err = getRoomService(p.conf)
		if err == nil {
			p.isSdkConnected = true
			break
		}
		time.Sleep(constants.RECONNECTION_INTERVAL)
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
			Sid:         p.sessionId,
			Uid:         constants.PLAYBACK_PREFIX + p.peerId,
			DisplayName: constants.PLAYBACK_PREFIX + p.peerName,
		},
	)
	if err != nil {
		p.joinRoomCh <- struct{}{}
		return
	}

	log.Infof("room.Join ok sessionId=%s", p.sessionId)
}

func (p *PlaybackPeer) onRoomJoin(success bool, info sdk.RoomInfo, err error) {
	log.Infof("onRoomJoin success = %v, info = %v, err = %s", success, info, err)

	p.roomRTC.OnTrack = p.onRTCTrack
	p.roomRTC.OnDataChannel = p.onRTCDataChannel
	p.roomRTC.OnError = p.onRTCError
	p.roomRTC.OnTrackEvent = p.onRTCTrackEvent
	p.roomRTC.OnSpeaker = p.onRTCSpeaker

	err = p.roomRTC.Join(p.sessionId, constants.PLAYBACK_PREFIX+p.peerId)
	if err != nil {
		p.joinRoomCh <- struct{}{}
		return
	}
	p.isRoomJoined = true
	log.Infof("rtc.Join ok sessionId=%s", p.sessionId)
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
	log.Errorf("onRoomError %s", err)
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
