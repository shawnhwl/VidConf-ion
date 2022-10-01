package recorder

import (
	"os"
	"strings"
	"time"

	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v3"
)

func (s *RoomRecord) onTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	// Send a PLI on an interval so that the publisher is pushing a keyframe every rtcpPLIInterval
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			rtcpSendErr := s.roomRTC.GetSubTransport().GetPeerConnection().WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())}})
			if rtcpSendErr != nil {
				log.Errorf("%v", rtcpSendErr)
			}
		}
	}()

	codecName := strings.Split(track.Codec().RTPCodecCapability.MimeType, "/")[1]
	log.Infof("Track has started, of type %d: %s \n", track.PayloadType(), codecName)
	buf := make([]byte, 1400)
	for {
		_, _, readErr := track.Read(buf)
		if readErr != nil {
			log.Errorf("%v", readErr)
			return
		}

	}
}

func (s *RoomRecord) onDataChannel(dc *webrtc.DataChannel) {
	log.Infof("onDataChannel: %v", dc)
}

func (s *RoomRecord) onError(err error) {
	log.Errorf("onError: %v", err)
}

func (s *RoomRecord) onTrackEvent(event sdk.TrackEvent) {
	log.Infof("onTrackEvent: %v", event)
}

func (s *RoomRecord) onSpeaker(event []string) {
	log.Infof("onSpeaker: %v", event)
}

func (s *RoomRecord) onJoin(success bool, info sdk.RoomInfo, err error) {
	log.Infof("OnJoin success = %v, info = %v, err = %v", success, info, err)

	// subscribe rtp from sessoin
	s.roomRTC.OnTrack = s.onTrack
	s.roomRTC.OnDataChannel = s.onDataChannel
	s.roomRTC.OnError = s.onError
	s.roomRTC.OnTrackEvent = s.onTrackEvent
	s.roomRTC.OnSpeaker = s.onSpeaker

	err = s.roomRTC.Join(s.roomid, s.systemUid)
	if err != nil {
		log.Panicf("RTC join error: %v", err)
	}
	log.Infof("rtc.Join ok roomid=%v", s.roomid)
}

func (s *RoomRecord) onPeerEvent(state sdk.PeerState, peer sdk.PeerInfo) {
	log.Infof("OnPeerEvent state = %v, peer = %+v", state, peer)
	s.peerevents = append(s.peerevents, PeerEventRecord{time.Now(), state, peer})
}

func (s *RoomRecord) onMessage(from string, to string, data map[string]interface{}) {
	log.Infof("OnMessage from = %v, to = %v, data = %v", from, to, data)
	s.chats = append(s.chats, ChatRecord{time.Now(), data})
}

func (s *RoomRecord) onDisconnect(sid, reason string) {
	log.Infof("OnDisconnect sid = %v, reason = %v", sid, reason)
	s.recordData()
	s.quitCh <- os.Interrupt
}

func (s *RoomRecord) joinRoom() {
	err := s.getRoomsByRoomid(s.roomid)
	if err != nil {
		log.Panicf("Join room fail:%s", err.Error())
	}

	s.roomService.OnJoin = s.onJoin
	s.roomService.OnPeerEvent = s.onPeerEvent
	s.roomService.OnMessage = s.onMessage
	s.roomService.OnDisconnect = s.onDisconnect

	// join room
	err = s.roomService.Join(
		sdk.JoinInfo{
			Sid:         s.roomid,
			Uid:         s.systemUid,
			DisplayName: s.systemUsername,
		},
	)
	if err != nil {
		log.Panicf("Session join error: %v", err)
		return
	}

	peers := s.roomService.GetPeers(s.roomid)
	for _, peer := range peers {
		s.peerevents = append(s.peerevents, PeerEventRecord{time.Now(), sdk.PeerState_JOIN, peer})
	}
	log.Infof("room.Join ok roomid=%v", s.roomid)
}

func (s *RoomRecord) RecorderSentinel() {
	log.Infof("RecorderSentinel Started")
	defer log.Infof("RecorderSentinel Ended")
	for {
		time.Sleep(s.chopInterval)
		s.recordData()
	}
}

func (s *RoomRecord) recordData() {
	// s.chats = append(s.chats, data)
	// s.peerevents = append(s.peerevents, PeerEventRecord{time.Now(), state, peer})
}
