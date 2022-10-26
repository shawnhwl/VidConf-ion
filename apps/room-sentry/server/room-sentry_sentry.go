package server

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/lib/pq"
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

type Announcement struct {
	id                    string
	status                string
	message               string
	relativeFrom          string
	relativeTimeInSeconds int64
	userId                pq.StringArray
	createdBy             string
	createdAt             time.Time
	updatedBy             string
	updatedAt             time.Time
}

type Room struct {
	id             string
	name           string
	status         string
	startTime      time.Time
	endTime        time.Time
	announcements  []Announcement
	allowedUserId  pq.StringArray
	earlyEndReason string
	createdBy      string
	createdAt      time.Time
	updatedBy      string
	updatedAt      time.Time
}

func (s *RoomMgmtSentryService) createRoom(roomId, roomname string) error {
	err := s.roomService.CreateRoom(sdk.RoomInfo{Sid: roomId, Name: roomname})
	if err != nil {
		output := fmt.Sprintf("Error creating roomId '%s' : %v", roomId, err)
		log.Errorf(output)
		return errors.New(output)
	}
	log.Infof("Created roomId '%s'", roomId)
	return nil
}

func (s *RoomMgmtSentryService) postMessage(roomId, message string, toId []string) error {
	peerinfo := s.roomService.GetPeers(roomId)
	if len(peerinfo) == 0 {
		output := fmt.Sprintf("Roomid '%s' is empty", roomId)
		log.Warnf(output)
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
	if len(toId) == 0 {
		err := s.roomService.SendMessage(roomId, s.systemUid, "all", payload)
		if err != nil {
			output := fmt.Sprintf("Error sending message '%s' to all users in roomId '%s' : %v", message, roomId, err)
			log.Errorf(output)
			return errors.New(output)
		}
	} else {
		var err error
		for _, userId := range toId {
			hasRecipient := false
			for _, peer := range peerinfo {
				if peer.Uid == userId {
					hasRecipient = true
				}
			}
			if !hasRecipient {
				output := fmt.Sprintf("Roomid '%s' missing recipientId '%s'", roomId, userId)
				log.Warnf(output)
				err = errors.New(output)
				continue
			}
			err1 := s.roomService.SendMessage(roomId, s.systemUid, userId, payload)
			if err1 != nil {
				err = err1
			}
			payload = map[string]interface{}{
				"msg": map[string]interface{}{
					"uid":              s.systemUid,
					"name":             s.systemUsername,
					"text":             message,
					"timestamp":        time.Now().Format(time.RFC3339),
					"ignoreByRecorder": true,
				},
			}
		}
		if err != nil {
			output := fmt.Sprintf("Error sending message '%s' to roomId '%s' : %v", message, roomId, err)
			log.Errorf(output)
			return errors.New(output)
		}
	}
	log.Infof("Sent message '%s' to users '%v' in roomId '%s'", message, toId, roomId)
	return nil
}

func (s *RoomMgmtSentryService) endRoom(roomId, roomname, reason string) error {
	err := s.createRoom(roomId, roomname)
	if err != nil {
		return err
	}
	s.postMessage(roomId, "Room has ended, Goodbye", make([]string, 0))
	peerinfo := s.roomService.GetPeers(roomId)
	for _, peer := range peerinfo {
		s.kickUser(roomId, peer.Uid)
	}
	if reason == "" {
		reason = "session ended"
	}
	err = s.roomService.EndRoom(roomId, reason, true)
	if err != nil {
		output := fmt.Sprintf("Error ending room '%s' : %v", roomId, err)
		log.Errorf(output)
		return errors.New((output))
	}
	log.Infof("Ended room '%s'", roomId)
	return nil
}

func (s *RoomMgmtSentryService) kickUser(roomId, userId string) (error, error) {
	peerinfo := s.roomService.GetPeers(roomId)
	for _, peer := range peerinfo {
		if userId == peer.Uid {
			err := s.roomService.RemovePeer(roomId, peer.Uid)
			if err != nil {
				output := fmt.Sprintf("Error kicking '%s' from room '%s' : %v", userId, roomId, err)
				log.Errorf(output)
				return nil, errors.New(output)
			}
			log.Infof("Kicked '%s' from room '%s'", userId, roomId)
			return nil, nil
		}
	}
	output := fmt.Sprintf("userId '%s' not found in roomId '%s'", userId, roomId)
	log.Warnf(output)
	return errors.New(output), nil
}

func (s *RoomMgmtSentryService) sentry() {
	s.initTimings()
	s.sortTimes()
	s.timeReady = time.Now().Format(time.RFC3339)
	s.onChanges <- "ok"

	log.Infof("RoomMgmtSentryService Started")
	for {
		select {
		case roomId := <-s.onChanges:
			s.updateTimes(roomId)
			s.sortTimes()
		default:
			s.startRooms()
			s.endRooms()
			s.sendAnnouncements()
			time.Sleep(s.pollInterval)
		}
	}
}

func (s *RoomMgmtSentryService) initTimings() {
	queryStmt := `SELECT "id" FROM "` + s.roomMgmtSchema + `"."room" WHERE "status"<>$1`
	var rows *sql.Rows
	var err error
	for retry := 0; retry < RETRY_COUNT; retry++ {
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
		var roomId string
		err = rows.Scan(&roomId)
		if err != nil {
			log.Errorf("could not query database: %s", err)
			continue
		}
		s.updateTimes(roomId)
	}
}

func (s *RoomMgmtSentryService) updateTimes(roomId string) {
	log.Infof("Database changes in RoomId '%s'", roomId)

	room, err := s.queryRoom(roomId)
	if err != nil {
		log.Errorf("could not query database: %s", err.Error())
		return
	}

	if room.status == ROOM_ENDED {
		delete(s.roomStarts, roomId)
		toDeleteId := make([]int, 0)
		for id, key := range s.roomStartKeys {
			if key == roomId {
				toDeleteId = append(toDeleteId, id)
				break
			}
		}
		s.roomStartKeys = deleteSlices(s.roomStartKeys, toDeleteId)
		delete(s.roomEnds, roomId)
		toDeleteId = make([]int, 0)
		for id, key := range s.roomEndKeys {
			if key == roomId {
				toDeleteId = append(toDeleteId, id)
				break
			}
		}
		s.roomEndKeys = deleteSlices(s.roomEndKeys, toDeleteId)
		s.deleteAnnouncementsByRoom(roomId)
		return
	}

	s.roomEnds[roomId] = Terminations{room.endTime, room.name, room.earlyEndReason}
	if room.status == ROOM_BOOKED && room.endTime.After(time.Now()) {
		s.roomStarts[roomId] = StartRooms{room.startTime, room.name}
	} else {
		delete(s.roomStarts, roomId)
		toDeleteId := make([]int, 0)
		for id, key := range s.roomStartKeys {
			if key == roomId {
				toDeleteId = append(toDeleteId, id)
				break
			}
		}
		s.roomStartKeys = deleteSlices(s.roomStartKeys, toDeleteId)
	}
	if len(room.announcements) == 0 {
		s.deleteAnnouncementsByRoom(roomId)
		return
	}

	for _, announce := range room.announcements {
		if announce.status == ANNOUNCEMENT_SENT {
			delete(s.announcements, AnnounceKey{roomId, announce.id})
			continue
		}
		gap := time.Duration(announce.relativeTimeInSeconds) * time.Second
		var anounceTime time.Time
		if announce.relativeFrom == FROM_END {
			anounceTime = room.endTime.Add(-gap)
		} else {
			anounceTime = room.startTime.Add(gap)
		}
		s.announcements[AnnounceKey{roomId, announce.id}] = Announcements{
			anounceTime,
			announce.message,
			announce.userId,
		}

	}
	toDelete := make([]AnnounceKey, 0)
	for announcekey := range s.announcements {
		if announcekey.roomId != roomId {
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

func (s *RoomMgmtSentryService) deleteAnnouncementsByRoom(roomId string) {
	toDelete := make([]AnnounceKey, 0)
	toDeleteId := make([]int, 0)
	for id, announcekey := range s.announcementKeys {
		if announcekey.roomId == roomId {
			toDelete = append(toDelete, announcekey)
			toDeleteId = append(toDeleteId, id)
		}
	}
	for _, announcekey := range toDelete {
		delete(s.announcements, announcekey)
	}
	s.announcementKeys = deleteSlices(s.announcementKeys, toDeleteId)
}

func (s *RoomMgmtSentryService) sortTimes() {
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

func (s *RoomMgmtSentryService) startRoomStatus(roomId string) {
	var err error
	updateStmt := `update "` + s.roomMgmtSchema + `"."room" set "status"=$1 where "id"=$2`
	for retry := 0; retry < RETRY_COUNT; retry++ {
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
	log.Infof("startRoomStatus: %s", roomId)
}

func (s *RoomMgmtSentryService) endRoomStatus(roomId string) {
	var err error
	updateStmt := `update "` + s.roomMgmtSchema + `"."room" set "status"=$1 where "id"=$2`
	for retry := 0; retry < RETRY_COUNT; retry++ {
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
	log.Infof("endRoomStatus: %s", roomId)
}

func (s *RoomMgmtSentryService) sendAnnouncementStatus(announceId string) {
	var err error
	updateStmt := `update "` + s.roomMgmtSchema + `"."announcement" set "status"=$1 where "id"=$2`
	for retry := 0; retry < RETRY_COUNT; retry++ {
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
	log.Infof("sendAnnouncementStatus: %s", announceId)
}

func (s *RoomMgmtSentryService) startRooms() {
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

func (s *RoomMgmtSentryService) endRooms() {
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

func (s *RoomMgmtSentryService) sendAnnouncements() {
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

func (s *RoomMgmtSentryService) queryRoom(roomId string) (Room, error) {
	queryStmt := `SELECT    "id",
							"name",
							"status",
							"startTime",
							"endTime",
							"allowedUserId",
							"earlyEndReason",
							"createdBy",
							"createdAt",
							"updatedBy",
							"updatedAt"
					FROM "` + s.roomMgmtSchema + `"."room" WHERE "id"=$1`
	var rows *sql.Row
	for retry := 0; retry < RETRY_COUNT; retry++ {
		rows = s.postgresDB.QueryRow(queryStmt, roomId)
		if rows.Err() == nil {
			break
		}
	}
	if rows.Err() != nil {
		errorString := fmt.Sprintf("could not query database: %s", rows.Err())
		log.Errorf(errorString)
		return Room{}, rows.Err()
	}
	var room Room
	err := rows.Scan(&room.id,
		&room.name,
		&room.status,
		&room.startTime,
		&room.endTime,
		&room.allowedUserId,
		&room.earlyEndReason,
		&room.createdBy,
		&room.createdAt,
		&room.updatedBy,
		&room.updatedAt)
	if err != nil {
		return Room{}, err
	}

	room.announcements = make([]Announcement, 0)
	queryStmt = `SELECT "id",
						"status",
						"message",
						"relativeFrom",
						"relativeTimeInSeconds",
						"userId",
						"createdBy",
						"createdAt",
						"updatedBy",
						"updatedAt"
					FROM "` + s.roomMgmtSchema + `"."announcement" WHERE "roomId"=$1`
	var announcements *sql.Rows
	for retry := 0; retry < RETRY_COUNT; retry++ {
		announcements, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
	}
	if err != nil {
		return Room{}, err
	}
	defer announcements.Close()
	for announcements.Next() {
		var announcement Announcement
		err = announcements.Scan(&announcement.id,
			&announcement.status,
			&announcement.message,
			&announcement.relativeFrom,
			&announcement.relativeTimeInSeconds,
			&announcement.userId,
			&announcement.createdBy,
			&announcement.createdAt,
			&announcement.updatedBy,
			&announcement.updatedAt)
		if err != nil {
			return Room{}, err
		}
		room.announcements = append(room.announcements, announcement)
	}

	return room, nil
}

func deleteSlices[T any](slice []T, ids []int) []T {
	for id := len(ids) - 1; id >= 0; id-- {
		slice = removeSlice(slice, ids[id])
	}
	return slice
}

func removeSlice[T any](slice []T, id int) []T {
	if id >= len(slice) {
		return slice
	}
	copy(slice[id:], slice[id+1:])
	return slice[:len(slice)-1]
}
