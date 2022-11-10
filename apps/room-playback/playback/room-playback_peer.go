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

	"github.com/google/uuid"
	"github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
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

	roomId         string
	playbackId     string
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
	trackLocals     map[string]*webrtc.TrackLocalStaticRTP
	trackCh         []chan Ctrl

	ctrlCh chan Ctrl
}

func (s *RoomPlaybackService) NewPlaybackPeer(peerId, peerName, orphanRemoteId string) *PlaybackPeer {
	p := &PlaybackPeer{
		conf:     s.conf,
		waitPeer: s.waitPeer,

		postgresDB:       s.postgresDB,
		roomMgmtSchema:   s.roomMgmtSchema,
		roomRecordSchema: s.roomRecordSchema,

		minioClient: s.minioClient,
		bucketName:  s.bucketName,

		roomId:         s.roomId,
		playbackId:     s.playbackId,
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
	for retry := 0; retry < RETRY_COUNT; retry++ {
		trackEventRow = p.postgresDB.QueryRow(queryStmt, p.roomId, p.peerId)
		if trackEventRow.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if trackEventRow.Err() != nil {
		log.Errorf("could not query database")
		return errors.New("could not query database")
	}
	var trackRemoteIds pq.StringArray
	err = trackEventRow.Scan(&trackRemoteIds)
	if err != nil {
		log.Errorf("could not query database")
		return errors.New("could not query database")
	}
	hasTrack := false
	for _, trackRemoteId := range trackRemoteIds {
		var trackRow *sql.Row
		queryStmt = `SELECT "id", "mimeType"
						FROM "` + p.roomRecordSchema + `"."track"
						WHERE "roomId"=$1 AND "trackRemoteId"=$2`
		for retry := 0; retry < RETRY_COUNT; retry++ {
			trackRow = p.postgresDB.QueryRow(queryStmt, p.roomId, trackRemoteId)
			if trackRow.Err() == nil {
				break
			}
			time.Sleep(RETRY_DELAY)
		}
		if trackRow.Err() != nil {
			log.Errorf("could not query database")
			continue
		}
		var trackId string
		var mimeType string
		err = trackRow.Scan(&trackId, &mimeType)
		if err != nil {
			log.Errorf("could not query database")
			continue
		}

		var trackStreamRows *sql.Rows
		queryStmt = `SELECT "filePath"
						FROM "` + p.roomRecordSchema + `"."trackStream"
						WHERE "roomId"=$1 AND "trackId"=$2`
		for retry := 0; retry < RETRY_COUNT; retry++ {
			trackStreamRows, err = p.postgresDB.Query(queryStmt,
				p.roomId,
				trackId)
			if err == nil {
				break
			}
			time.Sleep(RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not query database: %s", err)
			continue
		}
		defer trackStreamRows.Close()
		trackStreams := make(TrackStreams, 0)
		for trackStreamRows.Next() {
			var filePath string
			err := trackStreamRows.Scan(&filePath)
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
		p.trackLocals[trackId], err = webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: mimeType},
			mimeType+uuid.NewString(),
			p.peerId)
		if err != nil {
			log.Errorf("error creating TrackLocal: %s", err.Error())
			continue
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
	for retry := 0; retry < RETRY_COUNT; retry++ {
		trackRow = p.postgresDB.QueryRow(queryStmt, p.roomId, p.orphanRemoteId)
		if trackRow.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if trackRow.Err() != nil {
		log.Errorf("could not query database")
		return errors.New("could not query database")
	}
	var trackId string
	var mimeType string
	err = trackRow.Scan(&trackId, &mimeType)
	if err != nil {
		log.Errorf("could not query database")
		return errors.New("could not query database")
	}
	codecMimeType := strings.ToUpper(mimeType)
	if !strings.Contains(codecMimeType, MIME_VIDEO) {
		log.Errorf("orphaned remoteTrackId '%' is not screen share", p.orphanRemoteId)
		return errors.New("orphaned remoteTrackId is not screen share")
	}

	var trackStreamRows *sql.Rows
	queryStmt = `SELECT "filePath"
						FROM "` + p.roomRecordSchema + `"."trackStream"
						WHERE "roomId"=$1 AND "trackId"=$2`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		trackStreamRows, err = p.postgresDB.Query(queryStmt,
			p.roomId,
			trackId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		return err
	}
	defer trackStreamRows.Close()
	trackStreams := make(TrackStreams, 0)
	for trackStreamRows.Next() {
		var filePath string
		err := trackStreamRows.Scan(&filePath)
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
	p.trackLocals[trackId], err = webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: mimeType},
		mimeType+uuid.NewString(),
		p.peerId)
	if err != nil {
		log.Errorf("error creating TrackLocal: %s", err.Error())
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
	codecMimeType := strings.ToUpper(p.trackLocals[key].Codec().MimeType)
	isAudioCodec := false
	isVideoCodec := false
	isScreenShare := false
	if strings.Contains(codecMimeType, MIME_AUDIO) {
		isAudioCodec = true
	}
	if strings.Contains(codecMimeType, MIME_VIDEO) {
		if p.orphanRemoteId != "" {
			isScreenShare = true
		} else {
			isVideoCodec = true
		}
	}

	isRunning := false
	isPublishing := false
	isVideoOff := false
	var err error
	var rtpSender *webrtc.RTPSender = nil
	var speed10 time.Duration
	var playbackRefTime time.Time
	var actualRefTime time.Time
	trackIdx := 0
	maxTrackIdx := p.lenTrackStreams[key]
	trackStreams := p.trackStreams[key]
	trackInfo := PLAYBACK_PREFIX + p.peerName
	if p.orphanRemoteId != "" {
		trackInfo = "orphanId"
	}
	trackInfo = trackInfo +
		"/" + p.trackLocals[key].Codec().MimeType +
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
					err = p.roomRTC.UnPublish(rtpSender)
					if err != nil {
						log.Errorf("error un-publishing %s", trackInfo)
					}
				}
			} else {
				if (isAudioCodec && ctrl.isAudio) ||
					(isVideoCodec && (ctrl.isVideo || ctrl.isAudio)) ||
					(isScreenShare && ctrl.isVideo) {
					if !isPublishing {
						rtpSender, err = p.PublishTrackLocal(trackInfo, p.trackLocals[key])
						if err == nil {
							isPublishing = true
						}
					}
					isRunning = true
					isVideoOff = isVideoCodec && !ctrl.isVideo
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
				speed10, PLAYBACK_SPEED10,
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
				if isPublishing {
					isPublishing = false
					err = p.roomRTC.UnPublish(rtpSender)
					if err != nil {
						log.Errorf("error un-publishing %s", trackInfo)
					}
				}
				continue
			}
			if speed10*time.Since(actualRefTime) <
				PLAYBACK_SPEED10*trackStreams[trackIdx].Timestamp.Sub(playbackRefTime) {
				continue
			}
			if !isPublishing {
				rtpSender, err = p.PublishTrackLocal(trackInfo, p.trackLocals[key])
				if err == nil {
					isPublishing = true
				}
			}
			if isPublishing && !isVideoOff {
				_, err := p.trackLocals[key].Write(trackStreams[trackIdx].Data)
				if err != nil {
					log.Errorf("send err: %s", err.Error())
				}
			}
			trackIdx++
		}
	}
}

func (p *PlaybackPeer) PublishTrackLocal(trackInfo string, trackLocal *webrtc.TrackLocalStaticRTP) (*webrtc.RTPSender, error) {
	if !p.isRoomJoined {
		return nil, errors.New("room is not joined")
	}
	rtpSenders, err := p.roomRTC.Publish(trackLocal)
	if err != nil || rtpSenders[0] == nil {
		log.Errorf("error publishing %s", trackInfo)
		return nil, errors.New("error publishing trackLocal")
	}
	return rtpSenders[0], nil
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
	p.isRoomJoined = true
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
