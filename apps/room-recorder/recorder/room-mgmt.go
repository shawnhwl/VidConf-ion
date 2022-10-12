package recorder

import (
	"database/sql"
	"errors"
	"strings"

	log "github.com/pion/ion-log"
)

const (
	ROOM_BOOKED string = "Booked"
	ROOM_ENDED  string = "Ended"

	DB_RETRY     int    = 3
	DUP_PK       string = "duplicate key value violates unique constraint"
	NOT_FOUND_PK string = "no rows in result set"
)

type RoomBooking struct {
	status string
}

func (s *RoomRecorder) getRoomsByRoomid(roomId string) error {
	queryStmt := `SELECT "status" FROM "room" WHERE "id"=$1`
	var rooms *sql.Row
	for retry := 0; retry < DB_RETRY; retry++ {
		rooms = s.postgresDB.QueryRow(queryStmt, roomId)
		if rooms.Err() == nil {
			break
		}
	}
	if rooms.Err() != nil {
		log.Panicf("could not query database: %s", rooms.Err().Error())
		return rooms.Err()
	}
	var booking RoomBooking
	err := rooms.Scan(&booking.status)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			return errors.New(roomNotFound(roomId))
		} else {
			return err
		}
	}

	if booking.status == ROOM_BOOKED {
		log.Panicf(roomNotStarted(roomId))
		return errors.New(roomNotStarted(roomId))
	}

	if booking.status == ROOM_ENDED {
		log.Panicf(roomEnded(roomId))
		return errors.New(roomEnded(roomId))
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
