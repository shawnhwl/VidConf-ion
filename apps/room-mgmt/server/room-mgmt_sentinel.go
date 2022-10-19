package server

import (
	"database/sql"
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

func (s *RoomMgmtService) createRoom(roomid, roomname string) error {
	err := s.roomService.CreateRoom(sdk.RoomInfo{Sid: roomid, Name: roomname})
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
	payload := map[string]interface{}{
		"msg": map[string]interface{}{
			"uid":       s.systemUid,
			"name":      s.systemUsername,
			"text":      message,
			"timestamp": time.Now().Format(time.RFC3339),
		},
	}
	if len(toid) == 0 {
		err := s.roomService.SendMessage(roomid, s.systemUid, "all", payload)
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
			err1 := s.roomService.SendMessage(roomid, s.systemUid, userid, payload)
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

func (s *RoomMgmtService) endRoom(roomid, roomname, reason string) error {
	err := s.createRoom(roomid, roomname)
	if err != nil {
		return err
	}
	peerinfo := s.roomService.GetPeers(roomid)
	for _, peer := range peerinfo {
		s.kickUser(roomid, peer.Uid)
	}
	s.kickUser(roomid, s.systemUid)
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
		if peer.Uid == s.systemUid {
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
	s.sortTimes()
	s.onChanges <- "ok"

	log.Infof("RoomMgmtSentinel Started")
	for {
		select {
		case roomid := <-s.onChanges:
			s.updateTimes(roomid)
			s.sortTimes()
		default:
			s.startRooms()
			s.endRooms()
			s.sendAnnouncements()
			time.Sleep(s.pollInterval)
		}
	}
}

func (s *RoomMgmtService) initTimings() {
	s.roomStarts = make(map[string]StartRooms)
	s.roomStartKeys = make([]string, 0)
	s.roomEnds = make(map[string]Terminations)
	s.roomEndKeys = make([]string, 0)
	s.announcements = make(map[AnnounceKey]Announcements)
	s.announcementKeys = make([]AnnounceKey, 0)

	queryStmt := `SELECT "id" FROM "` + s.roomMgmtSchema + `"."room" WHERE "status"<>$1`
	var rows *sql.Rows
	var err error
	for retry := 0; retry < DB_RETRY; retry++ {
		rows, err = s.postgresDB.Query(queryStmt, ROOM_ENDED)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("could not query database: %s", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var roomid string
		err = rows.Scan(&roomid)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			continue
		}
		s.updateTimes(roomid)
	}
}

func (s *RoomMgmtService) updateTimes(roomid string) {
	log.Infof("Database changes in RoomId '%s'", roomid)

	room, err := s.queryRoom(roomid)
	if err != nil {
		log.Errorf("could not query database: %s", err.Error())
		return
	}

	if room.status == ROOM_ENDED {
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

	s.roomEnds[roomid] = Terminations{room.endTime, room.name, room.earlyEndReason}
	if room.status == ROOM_BOOKED {
		s.roomStarts[roomid] = StartRooms{room.startTime, room.name}
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
	if len(room.announcements) == 0 {
		s.deleteAnnouncementsByRoom(roomid)
		return
	}

	for _, announce := range room.announcements {
		if announce.status == ANNOUNCEMENT_SENT {
			delete(s.announcements, AnnounceKey{roomid, announce.id})
			continue
		}
		gap := time.Duration(announce.relativeTimeInSeconds) * time.Second
		var anounceTime time.Time
		if announce.relativeFrom == FROM_END {
			anounceTime = room.endTime.Add(-gap)
		} else {
			anounceTime = room.startTime.Add(gap)
		}
		s.announcements[AnnounceKey{roomid, announce.id}] = Announcements{
			anounceTime,
			announce.message,
			announce.userId,
		}

	}
	toDelete := make([]AnnounceKey, 0)
	for announcekey := range s.announcements {
		if announcekey.roomId != roomid {
			continue
		}
		isValid := false
		for _, announce := range room.announcements {
			if announce.id == announcekey.announceId {
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
		return s.roomStarts[s.roomStartKeys[i]].timeTick.Before(s.roomStarts[s.roomStartKeys[j]].timeTick)
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
	updateStmt := `update "` + s.roomMgmtSchema + `"."room" set "status"=$1 where "id"=$2`
	var err error
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			ROOM_STARTED,
			roomId)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("could not update database: %s", err)
	}
}

func (s *RoomMgmtService) endRoomStatus(roomId string) {
	updateStmt := `update "` + s.roomMgmtSchema + `"."room" set "status"=$1 where "id"=$2`
	var err error
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			ROOM_ENDED,
			roomId)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("could not update database: %s", err)
	}
}

func (s *RoomMgmtService) sendAnnouncementStatus(announceId string) {
	updateStmt := `update "` + s.roomMgmtSchema + `"."announcement" set "status"=$1 where "id"=$2`
	var err error
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			ANNOUNCEMENT_SENT,
			announceId)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("could not update database: %s", err)
	}
}

func (s *RoomMgmtService) startRooms() {
	toDeleteId := make([]int, 0)
	for id, key := range s.roomStartKeys {
		if s.roomStarts[key].timeTick.After(time.Now()) {
			break
		}
		log.Infof("startRoom %v, %v", key, s.roomStarts[key])
		go s.createRoom(key, s.roomStarts[key].roomname)
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
		go s.endRoom(key, s.roomEnds[key].roomname, s.roomEnds[key].reason)
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
		go s.sendAnnouncementStatus(key.announceId)
		delete(s.announcements, key)
		toDeleteId = append(toDeleteId, id)
	}
	s.announcementKeys = deleteSlices(s.announcementKeys, toDeleteId)
}
