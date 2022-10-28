package recorder

import (
	"database/sql"
	"os"
	"strings"
	"time"

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
	status    string
	startTime time.Time
}

func (s *RoomRecorderService) getRoomsByRoomid(roomId string) Room {
	queryStmt := `SELECT "status", "startTime" FROM "` + s.roomMgmtSchema + `"."room" WHERE "id"=$1`
	var row *sql.Row
	var room Room
	var err error
	for retryDelay := 0; retryDelay < DB_RETRY; retryDelay++ {
		for retry := 0; retry < DB_RETRY; retry++ {
			row = s.postgresDB.QueryRow(queryStmt, roomId)
			if row.Err() == nil {
				break
			}
		}
		if row.Err() != nil {
			log.Errorf("could not query database: %s", row.Err().Error())
			err = row.Err()
		} else {
			err = row.Scan(&room.status, &room.startTime)
			if err != nil {
				if strings.Contains(err.Error(), NOT_FOUND_PK) {
					log.Errorf(roomNotFound(roomId))
					os.Exit(1)
					return Room{}
				} else {
					log.Errorf("could not query database: %s", row.Err().Error())
				}
			}
		}
		if err == nil {
			break
		}
		time.Sleep(5 * time.Minute)
	}
	if err != nil {
		log.Errorf("could not proceed with room recording")
		os.Exit(1)
	}
	if room.status == ROOM_ENDED {
		log.Errorf("Room has ended, could not proceed with room recording")
		os.Exit(1)
	}

	return room
}

func roomNotFound(roomId string) string {
	return "RoomId '" + roomId + "' not found in database"
}
