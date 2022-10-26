package server

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/lib/pq"
	log "github.com/pion/ion-log"
)

const (
	ROOM_BOOKED string = "Booked"
	ROOM_ENDED  string = "Ended"

	DB_RETRY     int    = 3
	NOT_FOUND_PK string = "no rows in result set"
)

type RoomBooking struct {
	status        string
	name          string
	allowedUserId pq.StringArray
}

func (s *RoomSignalService) getRoomsByRoomid(roomId, uId, userName string) (string, error) {
	queryStmt := `SELECT    "name",
							"status",
							"allowedUserId"
					FROM "` + s.rs.roomMgmtSchema + `"."room" WHERE "id"=$1`

	var row *sql.Row
	for retry := 0; retry < DB_RETRY; retry++ {
		row = s.rs.postgresDB.QueryRow(queryStmt, roomId)
		if row.Err() == nil {
			break
		}
	}
	if row.Err() != nil {
		log.Errorf("could not query database: %s", row.Err().Error())
		return "", row.Err()
	}
	var booking RoomBooking
	err := row.Scan(&booking.name,
		&booking.status,
		&booking.allowedUserId)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			return "", errors.New(roomNotFound(roomId))
		} else {
			return "", err
		}
	}

	if booking.status == ROOM_BOOKED {
		log.Warnf(roomNotStarted(roomId))
		return "", errors.New(roomNotStarted(roomId))
	}

	if booking.status == ROOM_ENDED {
		log.Warnf(roomEnded(roomId))
		return "", errors.New(roomEnded(roomId))
	}

	isAllowed := false
	if len(uId) >= s.rs.lenSystemUid {
		if uId[:s.rs.lenSystemUid] == s.rs.systemUid {
			isAllowed = true
		}
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
