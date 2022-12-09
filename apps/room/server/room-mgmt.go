package server

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/lib/pq"
	log "github.com/pion/ion-log"
	constants "github.com/pion/ion/apps/constants"
)

type RoomBooking struct {
	status        string
	name          string
	allowedUserId pq.StringArray
}

func (s *RoomSignalService) getRoomsByRoomid(roomId, uId, userName string) (string, error) {
	var booking RoomBooking
	if strings.HasPrefix(roomId, s.rs.playbackIdPrefix) {
		queryStmt := `SELECT "name" FROM "` + s.rs.roomMgmtSchema + `"."playback" WHERE "id"=$1`
		var row *sql.Row
		for retry := 0; retry < constants.RETRY_COUNT; retry++ {
			row = s.rs.postgresDB.QueryRow(queryStmt, roomId)
			if row.Err() == nil {
				break
			}
			time.Sleep(constants.RETRY_DELAY)
		}
		if row.Err() != nil {
			log.Errorf("could not query database")
			return "", errors.New("could not query database")
		}
		err := row.Scan(&booking.name)
		if err != nil {
			if strings.Contains(err.Error(), constants.NOT_FOUND_PK) {
				return "", errors.New(roomNotFound(roomId))
			} else {
				return "", err
			}
		}
		booking.allowedUserId = make(pq.StringArray, 0)
		booking.status = ""
	} else {
		queryStmt := `SELECT    "name",
								"status",
								"allowedUserId"
						FROM "` + s.rs.roomMgmtSchema + `"."room" WHERE "id"=$1`
		var row *sql.Row
		for retry := 0; retry < constants.RETRY_COUNT; retry++ {
			row = s.rs.postgresDB.QueryRow(queryStmt, roomId)
			if row.Err() == nil {
				break
			}
			time.Sleep(constants.RETRY_DELAY)
		}
		if row.Err() != nil {
			log.Errorf("could not query database")
			return "", errors.New("could not query database")
		}
		err := row.Scan(&booking.name,
			&booking.status,
			&booking.allowedUserId)
		if err != nil {
			if strings.Contains(err.Error(), constants.NOT_FOUND_PK) {
				return "", errors.New(roomNotFound(roomId))
			} else {
				return "", err
			}
		}
	}
	if booking.status == constants.ROOM_BOOKED {
		log.Warnf(roomNotStarted(roomId))
		return "", errors.New(roomNotStarted(roomId))
	}
	if booking.status == constants.ROOM_ENDED {
		log.Warnf(roomEnded(roomId))
		return "", errors.New(roomEnded(roomId))
	}

	isAllowed := false
	if strings.HasPrefix(uId, s.rs.systemUserIdPrefix) {
		isAllowed = true
	}
	if !isAllowed {
		if len(booking.allowedUserId) == 0 {
			isAllowed = true
		} else {
			for _, allowedUserId := range booking.allowedUserId {
				if allowedUserId == uId {
					isAllowed = true
					break
				}
			}
		}
		for _, reservedName := range s.rs.reservedUsernames {
			if strings.ToUpper(userName) == reservedName {
				isAllowed = false
				break
			}
		}
	}
	if !isAllowed {
		log.Warnf(roomBlocked(roomId, uId, userName))
		return "", errors.New(roomBlocked(roomId, uId, userName))
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

func roomBlocked(roomId, uId, userName string) string {
	return "UserId '" + uId + "' userName '" + userName + "' is denied in RoomId '" + roomId + "'"
}
