package server

import (
	"errors"
	"strings"

	"github.com/lib/pq"
	log "github.com/pion/ion-log"
)

const (
	ROOM_BOOKED string = "Booked"
	ROOM_ENDED  string = "Ended"
)

type RoomBooking struct {
	status        string
	name          string
	allowedUserId pq.StringArray
}

func (s *RoomSignalService) getRoomsByRoomid(roomId, uId string) (string, error) {
	queryStmt := `SELECT "name",
						 "status",
						 "allowedUserId" FROM "room" WHERE "id"=$1`
	rooms := s.rs.postgresDB.QueryRow(queryStmt, roomId)
	if rooms.Err() != nil {
		if strings.Contains(rooms.Err().Error(), "no rows in result set") {
			log.Warnf(roomNotFound(roomId))
		} else {
			log.Errorf("could not query database: %s", rooms.Err().Error())
		}
		return "", rooms.Err()
	}
	var booking RoomBooking
	err := rooms.Scan(&booking.name,
		&booking.status,
		&booking.allowedUserId)
	if err != nil {
		return "", err
	}

	if booking.status == ROOM_BOOKED {
		log.Warnf(roomNotStarted(roomId))
		return "", errors.New(roomNotStarted(roomId))
	}

	if booking.status == ROOM_ENDED {
		log.Warnf(roomEnded(roomId))
		return "", errors.New(roomEnded(roomId))
	}

	isAllowed := false
	if uId == s.rs.systemUid || len(booking.allowedUserId) == 0 {
		isAllowed = true
	} else {
		for _, allowedUserId := range booking.allowedUserId {
			if allowedUserId == uId {
				isAllowed = true
				break
			}
		}
	}
	if !isAllowed {
		log.Warnf(roomBlocked(roomId, uId))
		return "", errors.New(roomBlocked(roomId, uId))
	}

	return booking.name, nil
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
	return "UserId '" + uId + "' is denied in RoomId '" + roomId + "'"
}
