package server

import (
	"encoding/json"
	"errors"

	log "github.com/pion/ion-log"
)

const (
	ROOM_BOOKED  string = "Booked"
	ROOM_STARTED string = "Started"
	ROOM_ENDED   string = "Ended"
)

type Get_RoomBooking struct {
	Status          string   `json:"status"`
	RoomName        string   `json:"roomName"`
	PermittedUserId []string `json:"permittedUserId"`
}

func (s *RoomSignalService) getRoomsByRoomid(roomid, uid string) (string, error) {
	dbRecords := s.rs.redis.Get(roomid)
	if dbRecords == "" {
		log.Warnf(roomNotFound(roomid))
		return "", errors.New(roomNotFound(roomid))
	}

	var get_room Get_RoomBooking
	err := json.Unmarshal([]byte(dbRecords), &get_room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		return "", errors.New("database corrupted")
	}

	if get_room.Status == ROOM_BOOKED {
		log.Warnf(roomNotStarted(roomid))
		return "", errors.New(roomNotStarted(roomid))
	}

	if get_room.Status == ROOM_ENDED {
		log.Warnf(roomEnded(roomid))
		return "", errors.New(roomEnded(roomid))
	}

	isPermitted := false
	if uid == s.rs.systemUid || len(get_room.PermittedUserId) == 0 {
		isPermitted = true
	} else {
		for _, permittedUserId := range get_room.PermittedUserId {
			if permittedUserId == uid {
				isPermitted = true
				break
			}
		}
	}
	if !isPermitted {
		log.Warnf(roomBlocked(roomid, uid))
		return "", errors.New(roomBlocked(roomid, uid))
	}

	return get_room.RoomName, nil
}

func roomNotFound(roomId string) string {
	return "RoomId '" + roomId + "' not found in database"
}

func roomNotStarted(roomId string) string {
	return "RoomId '" + roomId + "' session not started"
}

func roomEnded(roomId string) string {
	return "RoomId '" + roomId + "' has ended"
}

func roomBlocked(roomId, uId string) string {
	return "UserId '" + uId + "' is not permitted in RoomId '" + roomId + "'"
}
