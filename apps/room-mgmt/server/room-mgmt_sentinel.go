package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
)

type Payload struct {
	Uid  string `json:"uid"`
	Name string `json:"name"`
	Text string `json:"text"`
}

type Message struct {
	Msg Payload `json:"msg"`
}

func (s *RoomMgmtService) createRoom(roomid string) error {
	err := s.roomService.CreateRoom(sdk.RoomInfo{Sid: roomid})
	if err != nil {
		output := fmt.Sprintf("Error creating roomid '%s' : %v", roomid, err)
		log.Errorf(output)
		return errors.New(output)
	}
	log.Infof("Created roomid '%s'", roomid)
	return nil
}

func (s *RoomMgmtService) postMessage(roomid, message string, toid []string) error {
	peerinfo := s.roomService.GetPeers(roomid)
	if len(peerinfo) == 0 {
		output := fmt.Sprintf("Roomid '%s' is empty", roomid)
		log.Errorf(output)
		return errors.New(output)
	}
	payload := map[string]interface{}{"uid": s.systemId, "name": s.systemName, "text": message}
	if len(toid) == 0 {
		err := s.roomService.SendMessage(roomid, s.systemId, "all", map[string]interface{}{"msg": payload})
		if err != nil {
			output := fmt.Sprintf("Error sending message '%s' to all users in roomid '%s' : %v", message, roomid, err)
			log.Errorf(output)
			return errors.New(output)
		}
	} else {
		var err error
		for _, userid := range toid {
			hasRecipient := false
			for _, peer := range peerinfo {
				if peer.Uid == userid {
					hasRecipient = true
				}
			}
			if !hasRecipient {
				output := fmt.Sprintf("Roomid '%s' missing recipientid '%s'", roomid, userid)
				log.Errorf(output)
				err = errors.New(output)
				continue
			}
			err1 := s.roomService.SendMessage(roomid, s.systemId, userid, map[string]interface{}{"msg": payload})
			if err1 != nil {
				err = err1
			}
		}
		if err != nil {
			output := fmt.Sprintf("Error sending message '%s' to roomid '%s' : %v", message, roomid, err)
			log.Errorf(output)
			return errors.New(output)
		}
	}
	log.Infof("Sent message '%s' to users '%v' in roomid '%s'", message, toid, roomid)
	return nil
}

func (s *RoomMgmtService) endRoom(roomid, reason string) error {
	err := s.createRoom(roomid)
	if err != nil {
		return err
	}
	peerinfo := s.roomService.GetPeers(roomid)
	for _, peer := range peerinfo {
		s.kickUser(roomid, peer.Uid)
	}
	if reason == "" {
		reason = "session ended"
	}
	err = s.roomService.EndRoom(roomid, reason, true)
	if err != nil {
		output := fmt.Sprintf("Error ending room '%s' : %v", roomid, err)
		log.Errorf(output)
		return errors.New((output))
	}
	log.Infof("Ended room '%s'", roomid)
	return nil
}

func (s *RoomMgmtService) getPeers(roomid string) []User {
	peers := s.roomService.GetPeers(roomid)
	log.Infof("%v", peers)

	users := make([]User, 0)
	for _, peer := range peers {
		if peer.Uid == s.systemId {
			continue
		}
		var user User
		user.UserId = peer.Uid
		user.UserName = peer.DisplayName
		users = append(users, user)
	}
	return users
}

func (s *RoomMgmtService) kickUser(roomid, userid string) (error, error) {
	peerinfo := s.roomService.GetPeers(roomid)
	for _, peer := range peerinfo {
		if userid == peer.Uid {
			err := s.roomService.RemovePeer(roomid, peer.Uid)
			if err != nil {
				output := fmt.Sprintf("Error kicking '%s' from room '%s' : %v", userid, roomid, err)
				log.Errorf(output)
				return nil, errors.New(output)
			}
			log.Infof("Kicked '%s' from room '%s'", userid, roomid)
			return nil, nil
		}
	}
	output := fmt.Sprintf("userId '%s' not found in roomId '%s'", userid, roomid)
	log.Warnf(output)
	return errors.New(output), nil
}

func (s *RoomMgmtService) RoomMgmtSentinel() {
	s.initTimings()
	s.onChanges <- "ok"

	for {
		select {
		case roomid := <-s.onChanges:
			log.Infof("database changes to roomid '%s'", roomid)
			s.updateTimes(roomid)
		default:
			s.startRooms()
			s.endRooms()
			s.sendAnnouncements()
			time.Sleep(s.pollInterval)
		}
	}
}

func (s *RoomMgmtService) initTimings() {
	s.roomStarts = make(map[string]time.Time)
	s.roomStartKeys = make([]string, 0)
	s.roomEnds = make(map[string]Terminations)
	s.roomEndKeys = make([]string, 0)
	s.announcements = make(map[AnnounceKey]Announcements)
	s.announcementKeys = make([]AnnounceKey, 0)

	dbRecords := s.redisDB.Get(DB_ROOMS)
	if dbRecords == "" {
		return
	}

	var rooms Rooms
	err := json.Unmarshal([]byte(dbRecords), &rooms)
	if err != nil {
		log.Errorf(err.Error())
		return
	}

	toDeleteId := make([]int, 0)
	for id, roomid := range rooms.RoomIds {
		dbRecords := s.redisDB.Get(roomid)
		if dbRecords == "" {
			log.Errorf("missing roomid '%s' records in database", roomid)
			toDeleteId = append(toDeleteId, id)
			continue
		}
		var roomInfo Room
		err := json.Unmarshal([]byte(dbRecords), &roomInfo)
		if err != nil {
			log.Errorf("could not decode roomid '%s' records: %s", roomid, err)
			toDeleteId = append(toDeleteId, id)
			continue
		}
	}
	rooms.RoomIds = deleteSlices(rooms.RoomIds, toDeleteId)

	if len(toDeleteId) != 0 {
		roomsJSON, err := json.Marshal(rooms)
		if err == nil {
			err = s.redisDB.Set(DB_ROOMS, roomsJSON, 0)
			if err != nil {
				log.Errorf("Error writing to Database: %s", err)
			}
		}
	}

	for _, roomid := range rooms.RoomIds {
		s.updateTimes(roomid)
	}
}

func (s *RoomMgmtService) updateTimes(roomid string) {
	dbRecords := s.redisDB.Get(roomid)

	var roomInfo Room
	err := json.Unmarshal([]byte(dbRecords), &roomInfo)
	if err != nil {
		log.Errorf("could not decode roomid '%s' records: %s", roomid, err)
		return
	}

	if roomInfo.Status == ROOM_ENDED {
		delete(s.roomStarts, roomid)
		toDeleteId := make([]int, 0)
		for id, key := range s.roomStartKeys {
			if key == roomid {
				toDeleteId = append(toDeleteId, id)
				break
			}
		}
		s.roomStartKeys = deleteSlices(s.roomStartKeys, toDeleteId)
		delete(s.roomEnds, roomid)
		toDeleteId = make([]int, 0)
		for id, key := range s.roomEndKeys {
			if key == roomid {
				toDeleteId = append(toDeleteId, id)
				break
			}
		}
		s.roomEndKeys = deleteSlices(s.roomEndKeys, toDeleteId)
		s.deleteAnnouncementsByRoom(roomid)
		return
	}
	s.roomEnds[roomid] = Terminations{roomInfo.EndTime, roomInfo.EarlyEndReason}
	if roomInfo.Status == ROOM_BOOKED {
		s.roomStarts[roomid] = roomInfo.StartTime
	} else {
		delete(s.roomStarts, roomid)
		toDeleteId := make([]int, 0)
		for id, key := range s.roomStartKeys {
			if key == roomid {
				toDeleteId = append(toDeleteId, id)
				break
			}
		}
		s.roomStartKeys = deleteSlices(s.roomStartKeys, toDeleteId)
	}
	if len(roomInfo.Announcements) == 0 {
		s.deleteAnnouncementsByRoom(roomid)
		return
	}

	for _, announce := range roomInfo.Announcements {
		if announce.Status == ANNOUNCEMENT_SENT {
			delete(s.announcements, AnnounceKey{roomid, announce.AnnounceId})
			continue
		}
		gap := time.Duration(announce.RelativeTimeInSeconds) * time.Second
		var anounceTime time.Time
		if announce.RelativeFrom == FROM_END {
			anounceTime = roomInfo.EndTime.Add(-gap)
		} else {
			anounceTime = roomInfo.StartTime.Add(gap)
		}
		s.announcements[AnnounceKey{roomid, announce.AnnounceId}] = Announcements{
			anounceTime,
			announce.Message,
			announce.UserId,
		}

	}
	toDelete := make([]AnnounceKey, 0)
	for announcekey := range s.announcements {
		if announcekey.roomId != roomid {
			continue
		}
		isValid := false
		for _, announce := range roomInfo.Announcements {
			if announce.AnnounceId == announcekey.announceId {
				isValid = true
				break
			}
		}
		if !isValid {
			toDelete = append(toDelete, announcekey)
		}
	}
	for _, announcekey := range toDelete {
		delete(s.announcements, announcekey)
	}

	s.sortTimes()
}

func (s *RoomMgmtService) deleteAnnouncementsByRoom(roomid string) {
	toDelete := make([]AnnounceKey, 0)
	toDeleteId := make([]int, 0)
	for id, announcekey := range s.announcementKeys {
		if announcekey.roomId == roomid {
			toDelete = append(toDelete, announcekey)
			toDeleteId = append(toDeleteId, id)
		}
	}
	for _, announcekey := range toDelete {
		delete(s.announcements, announcekey)
	}
	s.announcementKeys = deleteSlices(s.announcementKeys, toDeleteId)
}

func (s *RoomMgmtService) sortTimes() {
	s.roomStartKeys = make([]string, 0)
	for key := range s.roomStarts {
		s.roomStartKeys = append(s.roomStartKeys, key)
	}
	sort.SliceStable(s.roomStartKeys, func(i, j int) bool {
		return s.roomStarts[s.roomStartKeys[i]].Before(s.roomStarts[s.roomStartKeys[j]])
	})

	s.roomEndKeys = make([]string, 0)
	for key := range s.roomEnds {
		s.roomEndKeys = append(s.roomEndKeys, key)
	}
	sort.SliceStable(s.roomEndKeys, func(i, j int) bool {
		return s.roomEnds[s.roomEndKeys[i]].timeTick.Before(s.roomEnds[s.roomEndKeys[j]].timeTick)
	})

	s.announcementKeys = make([]AnnounceKey, 0)
	for key := range s.announcements {
		s.announcementKeys = append(s.announcementKeys, key)
	}
	sort.SliceStable(s.announcementKeys, func(i, j int) bool {
		return s.announcements[s.announcementKeys[i]].timeTick.Before(s.announcements[s.announcementKeys[j]].timeTick)
	})
}

func (s *RoomMgmtService) startRoomStatus(roomId string) {
	dbRecords := s.redisDB.Get(roomId)
	if dbRecords == "" {
		log.Warnf(s.roomNotFound(roomId))
		return
	}

	var room Room
	err := json.Unmarshal([]byte(dbRecords), &room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		return
	}

	room.Status = ROOM_STARTED
	roomJSON, err := json.Marshal(room)
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		return
	}
	err = s.redisDB.Set(roomId, roomJSON, 0)
	if err != nil {
		log.Errorf("Error writing to Database: %s", err)
		return
	}
}

func (s *RoomMgmtService) endRoomStatus(roomId string) {
	dbRecords := s.redisDB.Get(roomId)
	if dbRecords == "" {
		log.Warnf(s.roomNotFound(roomId))
		return
	}

	var room Room
	err := json.Unmarshal([]byte(dbRecords), &room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		return
	}

	room.Status = ROOM_ENDED
	roomJSON, err := json.Marshal(room)
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		return
	}
	err = s.redisDB.Set(roomId, roomJSON, 0)
	if err != nil {
		log.Errorf("Error writing to Database: %s", err)
		return
	}
}

func (s *RoomMgmtService) sendAnnouncementStatus(roomId string, announceId int64) {
	dbRecords := s.redisDB.Get(roomId)
	if dbRecords == "" {
		log.Warnf(s.roomNotFound(roomId))
		return
	}

	var room Room
	err := json.Unmarshal([]byte(dbRecords), &room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		return
	}

	for id := range room.Announcements {
		if room.Announcements[id].AnnounceId == announceId {
			room.Announcements[id].Status = ANNOUNCEMENT_SENT
			break
		}
	}
	roomJSON, err := json.Marshal(room)
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		return
	}
	err = s.redisDB.Set(roomId, roomJSON, 0)
	if err != nil {
		log.Errorf("Error writing to Database: %s", err)
		return
	}
}

func (s *RoomMgmtService) startRooms() {
	toDeleteId := make([]int, 0)
	for id, key := range s.roomStartKeys {
		if s.roomStarts[key].After(time.Now()) {
			break
		}
		log.Infof("startRoom %v, %v", key, s.roomStarts[key])
		go s.createRoom(key)
		go s.startRoomStatus(key)
		delete(s.roomStarts, key)
		toDeleteId = append(toDeleteId, id)
	}
	s.roomStartKeys = deleteSlices(s.roomStartKeys, toDeleteId)
}

func (s *RoomMgmtService) endRooms() {
	toDeleteId := make([]int, 0)
	for id, key := range s.roomEndKeys {
		if s.roomEnds[key].timeTick.After(time.Now()) {
			break
		}
		log.Infof("endRoom %v, %v", key, s.roomEnds[key])
		go s.endRoom(key, s.roomEnds[key].reason)
		go s.endRoomStatus(key)
		delete(s.roomEnds, key)
		toDeleteId = append(toDeleteId, id)
	}
	s.roomEndKeys = deleteSlices(s.roomEndKeys, toDeleteId)
}

func (s *RoomMgmtService) sendAnnouncements() {
	toDeleteId := make([]int, 0)
	for id, key := range s.announcementKeys {
		if s.announcements[key].timeTick.After(time.Now()) {
			break
		}
		log.Infof("sendAnnouncements %v, %v", key, s.announcements[key])
		go s.postMessage(key.roomId, s.announcements[key].message, s.announcements[key].userId)
		go s.sendAnnouncementStatus(key.roomId, key.announceId)
		delete(s.announcements, key)
		toDeleteId = append(toDeleteId, id)
	}
	s.announcementKeys = deleteSlices(s.announcementKeys, toDeleteId)
}
