package recorder

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/pion/ion-log"
	constants "github.com/pion/ion/apps/constants"
)

func (s *RoomRecorderSession) getRoomsByRoomid() error {
	queryStmt := `SELECT 	"startTime",
							"endTime"
					FROM "` + s.roomRecordSchema + `"."room" WHERE "id"=$1`
	var row *sql.Row
	var err error
	for retry := 0; retry < constants.RETRY_COUNT*constants.RETRY_COUNT; retry++ {
		row = s.postgresDB.QueryRow(queryStmt, s.sessionId)
		if row.Err() == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if row.Err() != nil {
		errorString := "could not query database"
		log.Errorf(errorString)
		return errors.New(errorString)
	} else {
		var startTime time.Time
		var endTime time.Time
		err = row.Scan(&startTime, &endTime)
		if err != nil {
			if strings.Contains(err.Error(), constants.NOT_FOUND_PK) {
				errorString := "roomId '" + s.sessionId + "' not found in database"
				log.Errorf(errorString)
				return errors.New(errorString)
			} else {
				errorString := fmt.Sprintf("could not query database: %s", err)
				log.Errorf(errorString)
				return errors.New(errorString)
			}
		}
		if endTime.After(startTime) {
			errorString := "roomId '" + s.sessionId + "' has ended"
			log.Warnf(errorString)
			return errors.New(errorString)
		}
	}
	return nil
}
