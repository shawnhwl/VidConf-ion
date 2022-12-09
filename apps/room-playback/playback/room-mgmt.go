package playback

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	log "github.com/pion/ion-log"
	constants "github.com/pion/ion/apps/constants"
)

func (s *RoomPlaybackSession) getRoomByPlaybackId() error {
	queryStmt := `SELECT "roomId" FROM "` + s.roomMgmtSchema + `"."playback" WHERE "id"=$1`
	var playbackRow *sql.Row
	var err error
	for retry := 0; retry < constants.RETRY_COUNT*constants.RETRY_COUNT; retry++ {
		playbackRow = s.postgresDB.QueryRow(queryStmt, s.sessionId)
		if playbackRow.Err() == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if playbackRow.Err() != nil {
		log.Errorf("%s", constants.ErrQueryDB)
		return constants.ErrQueryDB
	} else {
		err = playbackRow.Scan(&s.roomId)
		if err != nil {
			if strings.Contains(err.Error(), constants.NOT_FOUND_PK) {
				log.Warnf(sessionNotFound(s.sessionId))
				return errors.New(sessionNotFound(s.sessionId))
			} else {
				log.Errorf("%s", constants.ErrQueryDB)
				return constants.ErrQueryDB
			}
		}
	}

	queryStmt = `SELECT "startTime" FROM "` + s.roomRecordSchema + `"."room" WHERE "id"=$1`
	var roomRow *sql.Row
	for retry := 0; retry < constants.RETRY_COUNT*constants.RETRY_COUNT; retry++ {
		roomRow = s.postgresDB.QueryRow(queryStmt, s.roomId)
		if roomRow.Err() == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if roomRow.Err() != nil {
		log.Errorf("%s", constants.ErrQueryDB)
		return constants.ErrQueryDB
	} else {
		err = roomRow.Scan(&s.roomStartTime)
		if err != nil {
			if strings.Contains(err.Error(), constants.NOT_FOUND_PK) {
				log.Errorf(roomNotFound(s.roomId))
				return errors.New(roomNotFound(s.roomId))
			} else {
				log.Errorf("%s", constants.ErrQueryDB)
				return constants.ErrQueryDB
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
