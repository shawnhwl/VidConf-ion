package playback

import (
	"database/sql"
	"os"
	"strings"
	"time"

	log "github.com/pion/ion-log"
)

func (s *RoomPlaybackService) getRoomByPlaybackId(sessionId string) error {
	queryStmt := `SELECT "roomId" FROM "` + s.roomMgmtSchema + `"."playback" WHERE "id"=$1`
	var playbackRow *sql.Row
	var err error
	for retry := 0; retry < RETRY_COUNT*RETRY_COUNT; retry++ {
		playbackRow = s.postgresDB.QueryRow(queryStmt, sessionId)
		if playbackRow.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if playbackRow.Err() != nil {
		log.Errorf("could not query database")
		os.Exit(1)
	} else {
		err = playbackRow.Scan(&s.roomId)
		if err != nil {
			if strings.Contains(err.Error(), NOT_FOUND_PK) {
				log.Warnf(sessionNotFound(sessionId))
				os.Exit(1)
			} else {
				log.Errorf("could not query database")
				os.Exit(1)
			}
		}
	}

	queryStmt = `SELECT "startTime" FROM "` + s.roomRecordSchema + `"."room" WHERE "id"=$1`
	var roomRow *sql.Row
	for retry := 0; retry < RETRY_COUNT*RETRY_COUNT; retry++ {
		roomRow = s.postgresDB.QueryRow(queryStmt, s.roomId)
		if roomRow.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if roomRow.Err() != nil {
		log.Errorf("could not query database")
		os.Exit(1)
	} else {
		err = roomRow.Scan(&s.roomStartTime)
		if err != nil {
			if strings.Contains(err.Error(), NOT_FOUND_PK) {
				log.Errorf(roomNotFound(s.roomId))
				os.Exit(1)
			} else {
				log.Errorf("could not query database")
				os.Exit(1)
			}
		}
	}
	return nil
}

func sessionNotFound(sessionId string) string {
	return "PlaybackId '" + sessionId + "' not found in database, may have ended session"
}

func roomNotFound(roomId string) string {
	return "RoomId '" + roomId + "' not found in records database"
}
