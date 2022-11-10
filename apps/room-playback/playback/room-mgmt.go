package playback

import (
	"database/sql"
	"os"
	"strings"
	"syscall"
	"time"

	log "github.com/pion/ion-log"
)

func (s *RoomPlaybackService) getRoomByPlaybackId(playbackId string) error {
	queryStmt := `SELECT "roomId" FROM "` + s.roomMgmtSchema + `"."playback" WHERE "id"=$1`
	var playbackRow *sql.Row
	var err error
	for retry := 0; retry < RETRY_COUNT*RETRY_COUNT; retry++ {
		playbackRow = s.postgresDB.QueryRow(queryStmt, playbackId)
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
				log.Warnf(playbackNotFound(playbackId))
				s.quitCh <- syscall.SIGTERM
				return err
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
				log.Warnf(roomNotFound(s.roomId))
				s.quitCh <- syscall.SIGTERM
				return err
			} else {
				log.Errorf("could not query database")
				os.Exit(1)
			}
		}
	}
	return nil
}

func playbackNotFound(playbackId string) string {
	return "PlaybackId '" + playbackId + "' not found in database, may have ended session"
}

func roomNotFound(playbackId string) string {
	return "RoomId '" + playbackId + "' not found in database, may have ended session"
}
