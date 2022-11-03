package playback

import (
	"fmt"
	"net/http"
	"time"

	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
)

func (s *RoomPlaybackService) playbackSentry() {
	exitCh := make(chan struct{})
	for id := range s.peers {
		go s.peers[id].Start(exitCh)
	}
	for {
		select {
		case ctrl := <-s.ctrlCh:
			if ctrl == "paused" {
				if s.isRunning {
					s.isRunning = false
					close(exitCh)
					s.pauseTime = time.Now()
				}
			} else if ctrl == "play" {
				if !s.isRunning {

					exitCh = make(chan struct{})
					for _, peer := range s.peers {
						peer.Start(exitCh)
					}
				}
			} else {
				if s.isRunning {
					s.isRunning = false
					close(exitCh)
					s.pauseTime = time.Now()
				}

				// secondsFromStart, _ := strconv.Atoi(ctrl)

				exitCh = make(chan struct{})
				for _, peer := range s.peers {
					peer.Start(exitCh)
				}
			}
		default:
			time.Sleep(time.Microsecond)
			if s.isRunning {
			}
			// send chat
		}
	}
}

func (s *RoomPlaybackService) checkForEmptyRoom() {
	time.Sleep(ROOMEMPTYWAIT_INTERVAL)
	for {
		time.Sleep(ROOMEMPTY_INTERVAL)
		if s.roomService != nil {
			peers := s.roomService.GetPeers(s.playbackId)
			peerCount := 0
			for _, peer := range peers {
				if len(peer.Uid) >= s.lenSystemUserId {
					if peer.Uid[:s.lenSystemUserId] == s.systemUserId {
						continue
					}
				}
				if len(peer.Uid) >= LEN_PLAYBACK_PREFIX {
					if peer.Uid[:LEN_PLAYBACK_PREFIX] == PLAYBACK_PREFIX {
						continue
					}
				}
				peerCount++
			}
			if peerCount == 0 {
				log.Warnf("Deleting playbackId '%s' since no viewers left", s.playbackId)
				requestURL := s.conf.RoomSentry.Url + "/delete/playback/" + s.playbackId
				s.httpPost(requestURL)
				return
			}
		}
	}
}

func (s *RoomPlaybackService) httpPost(requestURL string) error {
	request, err := http.NewRequest(http.MethodPost, requestURL, nil)
	if err != nil {
		log.Errorf("error sending http.POST: %v", err)
		return err
	}
	for retry := 0; retry < RETRY_COUNT; retry++ {
		err = s.httpClient(request)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("error sending http.POST: %v", err)
		return err
	}
	return nil
}

func (s *RoomPlaybackService) httpClient(request *http.Request) error {
	var response *http.Response
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return err
	} else if response.StatusCode != http.StatusOK {
		return fmt.Errorf("response.StatusCode=%v", response.StatusCode)
	}
	return nil
}

func (s *RoomPlaybackService) checkForRoomError() {
	for {
		<-s.joinRoomCh
		s.timeReady = ""
		if s.isSdkConnected {
			s.isSdkConnected = false
			go s.openRoom()
		}
	}
}

func (s *RoomPlaybackService) closeRoom() {
	if s.roomService != nil {
		s.roomService = nil
	}
	if s.sdkConnector != nil {
		s.sdkConnector = nil
	}
}

func (s *RoomPlaybackService) openRoom() {
	s.closeRoom()
	var err error
	for {
		time.Sleep(RECONNECTION_INTERVAL)
		s.sdkConnector, s.roomService, _, err = getRoomService(s.conf)
		if err == nil {
			s.isSdkConnected = true
			break
		}
	}
	s.getRoomByPlaybackId(s.playbackId)
	s.joinRoom()
}

func (s *RoomPlaybackService) joinRoom() {
	log.Infof("--- Joining Room ---")
	var err error
	s.roomService.OnJoin = s.onRoomJoin
	s.roomService.OnPeerEvent = s.onRoomPeerEvent
	s.roomService.OnMessage = s.onRoomMessage
	s.roomService.OnDisconnect = s.onRoomDisconnect
	s.roomService.OnError = s.onRoomError
	s.roomService.OnLeave = s.onRoomLeave
	s.roomService.OnRoomInfo = s.onRoomInfo

	err = s.roomService.Join(
		sdk.JoinInfo{
			Sid: s.playbackId,
			Uid: s.systemUserId + sdk.RandomKey(16),
		},
	)
	if err != nil {
		s.joinRoomCh <- struct{}{}
		return
	}

	log.Infof("room.Join ok roomid=%v", s.playbackId)
}

func (s *RoomPlaybackService) onRoomJoin(success bool, info sdk.RoomInfo, err error) {
	log.Infof("onRoomJoin success = %v, info = %v, err = %v", success, info, err)
	s.timeReady = time.Now().Format(time.RFC3339)
}

func (s *RoomPlaybackService) onRoomPeerEvent(state sdk.PeerState, peer sdk.PeerInfo) {
}

func (s *RoomPlaybackService) onRoomMessage(from string, to string, data map[string]interface{}) {
}

func (s *RoomPlaybackService) onRoomDisconnect(sid, reason string) {
	log.Infof("onRoomDisconnect sid = %+v, reason = %+v", sid, reason)
	s.joinRoomCh <- struct{}{}
}

func (s *RoomPlaybackService) onRoomError(err error) {
	log.Errorf("onRoomError %v", err)
	s.joinRoomCh <- struct{}{}
}

func (s *RoomPlaybackService) onRoomLeave(success bool, err error) {
}

func (s *RoomPlaybackService) onRoomInfo(info sdk.RoomInfo) {
}
