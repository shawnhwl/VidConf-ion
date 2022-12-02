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

	RETRY_COUNT  int           = 3
	RETRY_DELAY  time.Duration = 5 * time.Second
	DUP_PK       string        = "duplicate key value violates unique constraint"
	NOT_FOUND_PK string        = "no rows in result set"
)

func (s *RoomRecorderService) getRoomsByRoomid() {
	queryStmt := `SELECT "startTime" FROM "` + s.roomRecordSchema + `"."room" WHERE "id"=$1`
	var row *sql.Row
	var err error
	for retry := 0; retry < RETRY_COUNT*RETRY_COUNT; retry++ {
		row = s.postgresDB.QueryRow(queryStmt, s.sessionId)
		if row.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if row.Err() != nil {
		log.Errorf("could not query database")
		os.Exit(1)
	} else {
		var startTime time.Time
		err = row.Scan(&startTime)
		if err != nil {
			if strings.Contains(err.Error(), NOT_FOUND_PK) {
				log.Errorf(roomNotFound(s.sessionId))
				os.Exit(1)
			} else {
				log.Errorf("could not query database")
				os.Exit(1)
			}
		}
	}
}

func roomNotFound(roomId string) string {
	return "RoomId '" + roomId + "' not found in database"
}
