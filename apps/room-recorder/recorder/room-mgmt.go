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

type Room struct {
	status string
}

func (s *RoomRecorderService) getRoomsByRoomid(roomId string) (Room, error) {
	queryStmt := `SELECT "status" FROM "` + s.roomMgmtSchema + `"."room" WHERE "id"=$1`
	var row *sql.Row
	for retry := 0; retry < DB_RETRY; retry++ {
		row = s.postgresDB.QueryRow(queryStmt, roomId)
		if row.Err() == nil {
			break
		}
	}
	if row.Err() != nil {
		log.Panicf("could not query database: %s", row.Err().Error())
		return Room{}, row.Err()
	}
	var room Room
	err := row.Scan(&room.status)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			log.Panicf(roomNotFound(roomId))
			return Room{}, errors.New(roomNotFound(roomId))
		} else {
			log.Panicf("could not query database: %s", row.Err().Error())
			return Room{}, err
		}
	}

	return room, nil
}

func roomNotFound(roomId string) string {
	return "RoomId '" + roomId + "' not found in database"
}
