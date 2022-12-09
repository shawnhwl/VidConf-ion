package sentry

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/lib/pq"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	constants "github.com/pion/ion/apps/constants"
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
}

type Room struct {
	id             string
	name           string
	status         string
	startTime      time.Time
	endTime        time.Time
	announcements  []Announcement
	earlyEndReason string
}

func (s *RoomSentryService) createRoom(roomId, roomname string) error {
	sdkConnector, roomService, err := s.getRoomService(s.conf.Signal.Addr)
	if err != nil {
		return err
	}
	defer s.closeRoomService(sdkConnector, roomService)
	err = roomService.CreateRoom(sdk.RoomInfo{Sid: roomId, Name: roomname})
	if err != nil {
		output := fmt.Sprintf("Error creating roomId '%s' : %s", roomId, err)
		log.Errorf(output)
		return errors.New(output)
	}
	log.Infof("Created roomId '%s'", roomId)
	return nil
}

func (s *RoomSentryService) postMessage(roomId, message string, toId []string) error {
	sdkConnector, roomService, err := s.getRoomService(s.conf.Signal.Addr)
	if err != nil {
		return err
	}
	defer s.closeRoomService(sdkConnector, roomService)
	peerinfo := roomService.GetPeers(roomId)
	if len(peerinfo) == 0 {
		output := fmt.Sprintf("Roomid '%s' is empty", roomId)
		log.Warnf(output)
		return errors.New(output)
	}
	payload := map[string]interface{}{
		"msg": map[string]interface{}{
			"uid":       s.systemUserIdPrefix,
			"name":      s.systemUsername,
			"mimeType":  constants.MIME_MESSAGE,
			"text":      message,
			"timestamp": time.Now().Format(time.RFC3339),
		},
	}
	if len(toId) == 0 {
		err := roomService.SendMessage(roomId, s.systemUserIdPrefix, "all", payload)
		if err != nil {
			output := fmt.Sprintf("Error sending message '%s' to all users in roomId '%s' : %s", message, roomId, err)
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
			err1 := roomService.SendMessage(roomId, s.systemUserIdPrefix, userId, payload)
			if err1 != nil {
				err = err1
			}
			payload = map[string]interface{}{
				"msg": map[string]interface{}{
					"uid":              s.systemUserIdPrefix,
					"name":             s.systemUsername,
					"text":             message,
					"mimeType":         constants.MIME_MESSAGE,
					"timestamp":        time.Now().Format(time.RFC3339),
					"ignoreByRecorder": true,
				},
			}
		}
		if err != nil {
			output := fmt.Sprintf("Error sending message '%s' to roomId '%s' : %s", message, roomId, err)
			log.Errorf(output)
			return errors.New(output)
		}
	}
	log.Infof("Sent message '%s' to users '%v' in roomId '%s'", message, toId, roomId)
	return nil
}

func (s *RoomSentryService) closeRoom(roomId, reason string) error {
	err := s.createRoom(roomId, "")
	if err != nil {
		return err
	}
	s.postMessage(roomId, "Room has ended, Goodbye", make([]string, 0))
	s.kickUser(roomId)

	sdkConnector, roomService, err := s.getRoomService(s.conf.Signal.Addr)
	if err != nil {
		return err
	}
	defer s.closeRoomService(sdkConnector, roomService)
	if reason == "" {
		reason = "session ended"
	}
	err = roomService.EndRoom(roomId, reason, true)
	if err != nil {
		output := fmt.Sprintf("Error ending room '%s' : %s", roomId, err)
		log.Errorf(output)
		return errors.New((output))
	}
	log.Infof("Ended room '%s'", roomId)
	return nil
}

func (s *RoomSentryService) kickUser(roomId string) error {
	sdkConnector, roomService, err := s.getRoomService(s.conf.Signal.Addr)
	if err != nil {
		return err
	}
	defer s.closeRoomService(sdkConnector, roomService)
	peerinfo := roomService.GetPeers(roomId)
	for _, peer := range peerinfo {
		err := roomService.RemovePeer(roomId, peer.Uid)
		if err != nil {
			log.Errorf("Error kicking '%s' from room '%s' : %s", peer.Uid, roomId, err)
		} else {
			log.Infof("Kicked '%s' from room '%s'", peer.Uid, roomId)
		}
	}
	return nil
}

func (s *RoomSentryService) getRoomService(signalAddr string) (*sdk.Connector, *sdk.Room, error) {
	log.Infof("--- Connecting to Room Signal ---")
	log.Infof("attempt gRPC connection to %s", signalAddr)
	sdkConnector := sdk.NewConnector(signalAddr)
	if sdkConnector == nil {
		log.Errorf("connection to %s fail", signalAddr)
		return nil, nil, errors.New("")
	}
	roomService := sdk.NewRoom(sdkConnector)
	return sdkConnector, roomService, nil
}

func (s *RoomSentryService) closeRoomService(sdkConnector *sdk.Connector, roomService *sdk.Room) {
	if roomService != nil {
		roomService = nil
	}
	if sdkConnector != nil {
		sdkConnector = nil
	}
}

func (s *RoomSentryService) checkForServiceCall() {
	s.initTimings()
	s.sortTimes()
	s.onRoomUpdatesCh <- "ok"

	log.Infof("RoomMgmtSentryService Started, Poll interval:%v", s.pollInterval)
	for {
		select {
		case roomId := <-s.onRoomUpdatesCh:
			go s.roomChanges(roomId)
		default:
			s.startRooms()
			s.endRooms()
			s.sendAnnouncements()
			time.Sleep(s.pollInterval)
		}
	}
}

func (s *RoomSentryService) deletePlayback(playbackId string) error {
	var err error
	deleteStmt := `DELETE FROM "` + s.roomMgmtSchema + `"."playback" WHERE "id"=$1`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(deleteStmt, playbackId)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		errorString := fmt.Sprintf("could not delete from database: %s", err)
		log.Errorf("%s", errorString)
		return errors.New(errorString)
	}
	s.natsPublish(constants.ENDPLAYBACK_TOPIC+playbackId, nil)
	err = s.closeRoom(playbackId, "")
	if err != nil {
		return err
	}

	return nil
}

func (s *RoomSentryService) roomChanges(roomId string) {
	s.updateTimes(roomId)
	s.sortTimes()
}

func (s *RoomSentryService) initTimings() {
	queryStmt := `SELECT "id" FROM "` + s.roomMgmtSchema + `"."room" WHERE "status"<>$1`
	var rows *sql.Rows
	var err error
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		rows, err = s.postgresDB.Query(queryStmt, constants.ROOM_ENDED)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
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

func (s *RoomSentryService) updateTimes(roomId string) {
	log.Infof("Database changes in RoomId '%s'", roomId)

	room, err := s.queryRoom(roomId)
	if err != nil {
		log.Errorf("could not query database: %s", err.Error())
		return
	}

	if room.status == constants.ROOM_ENDED {
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
	if room.status == constants.ROOM_BOOKED && room.endTime.After(time.Now()) {
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
		if announce.status == constants.ANNOUNCEMENT_SENT {
			delete(s.announcements, AnnounceKey{roomId, announce.id})
			continue
		}
		gap := time.Duration(announce.relativeTimeInSeconds) * time.Second
		var anounceTime time.Time
		if announce.relativeFrom == constants.FROM_END {
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

func (s *RoomSentryService) deleteAnnouncementsByRoom(roomId string) {
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

func (s *RoomSentryService) sortTimes() {
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

func (s *RoomSentryService) startRoom(roomId, name string, startTime time.Time) {
	log.Infof("startRoomStatus: %s", roomId)
	timeNow := time.Now()
	var err error
	insertStmt := `INSERT INTO "` + s.roomRecordSchema + `"."room"(
		"id",
		"name",
		"startTime",
		"endTime")
		VALUES($1, $2, $3, $4)`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			roomId,
			name,
			timeNow,
			timeNow)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not insert into database: %s", err)
		return
	}
	updateStmt := `UPDATE "` + s.roomMgmtSchema + `"."room"
					SET "status"=$1,
						"startTime"=$2 WHERE "id"=$3`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			constants.ROOM_STARTED,
			timeNow,
			roomId)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not update database: %s", err)
		return
	}
	s.natsPublish(constants.STARTRECORDING_TOPIC, []byte(roomId))
	s.createRoom(roomId, name)
}

func (s *RoomSentryService) endRoom(roomId string) {
	log.Infof("endRoomStatus: %s", roomId)
	var err error
	updateStmt := `UPDATE "` + s.roomMgmtSchema + `"."room" SET "status"=$1 WHERE "id"=$2`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			constants.ROOM_ENDED,
			roomId)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not update database: %s", err)
		return
	}
	updateStmt = `UPDATE "` + s.roomRecordSchema + `"."room" SET "endTime"=$1 WHERE "id"=$2`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			time.Now(),
			roomId)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not update database: %s", err)
		return
	}
	s.natsPublish(constants.ENDRECORDING_TOPIC+roomId, nil)
	s.closeRoom(roomId, s.roomEnds[roomId].reason)
}

func (s *RoomSentryService) sendAnnouncementStatus(announceId string) {
	var err error
	updateStmt := `UPDATE "` + s.roomMgmtSchema + `"."announcement" SET "status"=$1 WHERE "id"=$2`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			constants.ANNOUNCEMENT_SENT,
			announceId)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not update database: %s", err)
		return
	}
	log.Infof("sendAnnouncementStatus: %s", announceId)
}

func (s *RoomSentryService) startRooms() {
	toDeleteId := make([]int, 0)
	for id, key := range s.roomStartKeys {
		if s.roomStarts[key].timeTick.After(time.Now()) {
			break
		}
		log.Infof("startRoom %s, %v", key, s.roomStarts[key])
		go s.startRoom(key, s.roomStarts[key].roomname, s.roomStarts[key].timeTick)
		delete(s.roomStarts, key)
		toDeleteId = append(toDeleteId, id)
	}
	s.roomStartKeys = deleteSlices(s.roomStartKeys, toDeleteId)
}

func (s *RoomSentryService) endRooms() {
	toDeleteId := make([]int, 0)
	for id, key := range s.roomEndKeys {
		if s.roomEnds[key].timeTick.After(time.Now()) {
			break
		}
		log.Infof("endRoom %s, %v", key, s.roomEnds[key])
		go s.endRoom(key)
		delete(s.roomEnds, key)
		toDeleteId = append(toDeleteId, id)
	}
	s.roomEndKeys = deleteSlices(s.roomEndKeys, toDeleteId)
}

func (s *RoomSentryService) sendAnnouncements() {
	toDeleteId := make([]int, 0)
	for id, key := range s.announcementKeys {
		if s.announcements[key].timeTick.After(time.Now()) {
			break
		}
		log.Infof("sendAnnouncements %s, %v", key, s.announcements[key])
		go s.postMessage(key.roomId, s.announcements[key].message, s.announcements[key].userId)
		go s.sendAnnouncementStatus(key.announceId)
		delete(s.announcements, key)
		toDeleteId = append(toDeleteId, id)
	}
	s.announcementKeys = deleteSlices(s.announcementKeys, toDeleteId)
}

func (s *RoomSentryService) queryRoom(roomId string) (Room, error) {
	queryStmt := `SELECT    "id",
							"name",
							"status",
							"startTime",
							"endTime",
							"earlyEndReason"
					FROM "` + s.roomMgmtSchema + `"."room" WHERE "id"=$1`
	var rows *sql.Row
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		rows = s.postgresDB.QueryRow(queryStmt, roomId)
		if rows.Err() == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
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
		&room.earlyEndReason)
	if err != nil {
		return Room{}, err
	}

	room.announcements = make([]Announcement, 0)
	queryStmt = `SELECT "id",
						"status",
						"message",
						"relativeFrom",
						"relativeTimeInSeconds",
						"userId"
					FROM "` + s.roomMgmtSchema + `"."announcement" WHERE "roomId"=$1`
	var announcements *sql.Rows
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		announcements, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
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
			&announcement.userId)
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
