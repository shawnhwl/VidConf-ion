package recorder

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
	Status string `json:"status"`
}

func (s *RoomRecord) getRoomsByRoomid(roomid string) error {
	dbRecords := s.redisDB.Get(roomid)
	if dbRecords == "" {
		log.Warnf(roomNotFound(roomid))
		return errors.New(roomNotFound(roomid))
	}

	var get_room Get_RoomBooking
	err := json.Unmarshal([]byte(dbRecords), &get_room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		return errors.New("database corrupted")
	}

	if get_room.Status == ROOM_BOOKED {
		log.Warnf(roomNotStarted(roomid))
		return errors.New(roomNotStarted(roomid))
	}

	if get_room.Status == ROOM_ENDED {
		log.Warnf(roomEnded(roomid))
		return errors.New(roomEnded(roomid))
	}

	return nil
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
