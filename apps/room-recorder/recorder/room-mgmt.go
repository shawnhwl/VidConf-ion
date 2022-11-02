package recorder

import (
	"database/sql"
	"os"
	"strings"
	"syscall"
	"time"

	log "github.com/pion/ion-log"
)

const (
	ROOM_BOOKED string = "Booked"
	ROOM_ENDED  string = "Ended"

	RETRY_COUNT  int           = 3
	RETRY_DELAY  time.Duration = 5 * time.Second
	DUP_PK       string        = "duplicate key value violates unique constraint"
	NOT_FOUND_PK string        = "no rows in result set"
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
	for retry := 0; retry < RETRY_COUNT*RETRY_COUNT; retry++ {
		row = s.postgresDB.QueryRow(queryStmt, roomId)
		if row.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
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
	if err != nil {
		log.Errorf("could not proceed with room recording")
		os.Exit(1)
	}
	if room.status == ROOM_ENDED {
		log.Errorf("Room has ended, could not proceed with room recording")
		s.quitCh <- syscall.SIGTERM
	}

	return room
}

func roomNotFound(roomId string) string {
	return "RoomId '" + roomId + "' not found in database"
}
