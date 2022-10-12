package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lib/pq"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
)

const (
	MISS_REQUESTOR     string = "missing mandatory field 'requestor'"
	MISS_START_TIME    string = "missing mandatory field 'startTime' in ISO9601 format"
	MISS_END_TIME      string = "missing mandatory field 'endTime' in ISO9601 format"
	MISS_ANNOUNCE_ID   string = "missing mandatory field 'announcements.announceId'"
	MISS_ANNOUNCE_MSG  string = "missing mandatory non-empty string field 'announcements.message'"
	MISS_ANNOUNCE_TIME string = "missing mandatory field 'announcements.relativeTimeInSeconds'"
	MISS_ANNOUNCE_REL  string = "missing valid field 'announcements.relativeFrom' (accepts only: NULL|'start'|'end')"
	DUP_ANNOUNCE_ID    string = "forbidden duplication of 'announcements.announceId'"

	ANNOUNCEMENT_QUEUED string = "Queued"
	ANNOUNCEMENT_SENT   string = "Sent"
	ROOM_BOOKED         string = "Booked"
	ROOM_STARTED        string = "Started"
	ROOM_ENDED          string = "Ended"
	FROM_START          string = "start"
	FROM_END            string = "end"

	DB_RETRY     int    = 3
	DUP_PK       string = "duplicate key value violates unique constraint"
	NOT_FOUND_PK string = "no rows in result set"
)

type Announcement struct {
	id                    string
	status                string
	message               string
	relativeFrom          string
	relativeTimeInSeconds int64
	userId                pq.StringArray
	createdBy             string
	createdAt             time.Time
	updatedBy             string
	updatedAt             time.Time
}

type Room struct {
	id             string
	name           string
	status         string
	startTime      time.Time
	endTime        time.Time
	announcements  []Announcement
	allowedUserId  pq.StringArray
	earlyEndReason string
	createdBy      string
	createdAt      time.Time
	updatedBy      string
	updatedAt      time.Time
}

type User struct {
	UserId   string `json:"userId"`
	UserName string `json:"userName"`
}

type Rooms struct {
	Ids []string `json:"id"`
}

type Patch_Announcement struct {
	Id                    *string        `json:"id,omitempty"`
	Message               *string        `json:"message,omitempty"`
	RelativeFrom          *string        `json:"relativeFrom,omitempty"`
	RelativeTimeInSeconds *int64         `json:"relativeTimeInSeconds,omitempty"`
	UserId                pq.StringArray `json:"userId,omitempty"`
}

type Patch_Room struct {
	Requestor     *string              `json:"requestor,omitempty"`
	Name          *string              `json:"name,omitempty"`
	StartTime     *time.Time           `json:"startTime,omitempty"`
	EndTime       *time.Time           `json:"endTime,omitempty"`
	Announcements []Patch_Announcement `json:"announcements,omitempty"`
	AllowedUserId pq.StringArray       `json:"allowedUserId,omitempty"`
}

type Get_Announcement struct {
	Id                    string         `json:"id"`
	Message               string         `json:"message"`
	RelativeFrom          string         `json:"relativeFrom"`
	RelativeTimeInSeconds int64          `json:"relativeTimeInSeconds"`
	UserId                pq.StringArray `json:"userId"`
	CreatedBy             string         `json:"createdBy"`
	CreatedAt             time.Time      `json:"createdAt"`
	UpdatedBy             string         `json:"updatedBy"`
	UpdatedAt             time.Time      `json:"updatedAt"`
}

type Get_Room struct {
	Id             string             `json:"id"`
	Name           string             `json:"name"`
	Status         string             `json:"status"`
	StartTime      time.Time          `json:"startTime"`
	EndTime        time.Time          `json:"endTime"`
	Announcements  []Get_Announcement `json:"announcements"`
	AllowedUserId  pq.StringArray     `json:"allowedUserId"`
	Users          []User             `json:"users"`
	EarlyEndReason string             `json:"earlyEndReason"`
	CreatedBy      string             `json:"createdBy"`
	CreatedAt      time.Time          `json:"createdAt"`
	UpdatedBy      string             `json:"updatedBy"`
	UpdatedAt      time.Time          `json:"updatedAt"`
}

type Delete_Room struct {
	Requestor         *string `json:"requestor,omitempty"`
	TimeLeftInSeconds *int    `json:"timeLeftInSeconds,omitempty"`
	Reason            *string `json:"reason,omitempty"`
}

type Delete_User struct {
	Requestor *string `json:"requestor,omitempty"`
}

type Delete_Announcement struct {
	Requestor *string  `json:"requestor,omitempty"`
	Id        []string `json:"id"`
}

func (s *RoomMgmtService) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomMgmtService) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func (s *RoomMgmtService) postRooms(c *gin.Context) {
	log.Infof("POST /rooms")

	var patch_room Patch_Room
	if err := c.ShouldBindJSON(&patch_room); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}

	err := s.patchRoomRequest(patch_room, c)
	if err != nil {
		return
	}

	if patch_room.StartTime == nil {
		log.Warnf(MISS_START_TIME)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_START_TIME})
		return
	}
	if patch_room.EndTime == nil {
		log.Warnf(MISS_END_TIME)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_END_TIME})
		return
	}

	var room Room
	roomId := uuid.NewString()
	room.id = roomId
	room.name = ""
	room.status = ROOM_BOOKED
	room.startTime = *patch_room.StartTime
	room.endTime = *patch_room.EndTime
	room.announcements = make([]Announcement, 0)
	room.allowedUserId = make(pq.StringArray, 0)
	room.earlyEndReason = ""
	room.createdBy = *patch_room.Requestor
	room.createdAt = time.Now()
	room.updatedBy = *patch_room.Requestor
	room.updatedAt = room.createdAt
	insertStmt := `insert into "room"(  "id",
										"name",
										"status",
										"startTime",
										"endTime",
										"allowedUserId",
										"earlyEndReason",
										"createdBy",
										"createdAt",
										"updatedBy",
										"updatedAt")
					values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(insertStmt,
			room.id,
			room.name,
			room.status,
			room.startTime,
			room.endTime,
			pq.Array(room.allowedUserId),
			room.earlyEndReason,
			room.createdBy,
			room.createdAt,
			room.updatedBy,
			room.updatedAt)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			roomId = uuid.NewString()
			room.id = roomId
		}
	}
	if err != nil {
		errorString := fmt.Sprintf("could not insert into database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	err = s.patchRoom(&room, patch_room, c)
	if err != nil {
		deleteStmt := `delete from "room" where "id"=$1`
		for retry := 0; retry < DB_RETRY; retry++ {
			_, err = s.postgresDB.Exec(deleteStmt, room.id)
			if err == nil {
				break
			}
		}
		if err != nil {
			log.Errorf("could not delete from database: %s", err)
		}
		return
	}

	s.onChanges <- roomId
	log.Infof("posted roomId '%s'", roomId)
	link := fmt.Sprintf("%s?room=%s", s.conf.WebApp.Url, roomId)
	c.JSON(http.StatusOK, map[string]interface{}{"link": link})
}

func (s *RoomMgmtService) getRooms(c *gin.Context) {
	log.Infof("GET /rooms")

	var rooms Rooms
	rooms.Ids = make(pq.StringArray, 0)
	var rows *sql.Rows
	var err error
	for retry := 0; retry < DB_RETRY; retry++ {
		rows, err = s.postgresDB.Query(`SELECT "id" FROM "room"`)
		if err == nil {
			break
		}
	}
	if err != nil {
		errorString := fmt.Sprintf("could not query database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		if err != nil {
			errorString := fmt.Sprintf("could not query database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
			return
		}
		rooms.Ids = append(rooms.Ids, id)
	}

	c.JSON(http.StatusOK, rooms)
}

func (s *RoomMgmtService) getRoomsByRoomid(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("GET /rooms/%s", roomId)

	get_room, err := s.queryGetRoom(roomId, c)
	if err != nil {
		return
	}

	c.JSON(http.StatusOK, get_room)
}

func (s *RoomMgmtService) patchRoomsByRoomid(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("PATCH /rooms/%s", roomId)

	var patch_room Patch_Room
	if err := c.ShouldBindJSON(&patch_room); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}
	err := s.patchRoomRequest(patch_room, c)
	if err != nil {
		return
	}

	room, err := s.getEditableRoom(roomId, c)
	if err != nil {
		return
	}

	room.updatedAt = time.Now()
	err = s.patchRoom(&room, patch_room, c)
	if err != nil {
		return
	}

	s.onChanges <- roomId
	get_room, err := s.queryGetRoom(roomId, c)
	if err != nil {
		return
	}
	log.Infof("patched roomId '%s'", roomId)
	c.JSON(http.StatusOK, get_room)
}

func (s *RoomMgmtService) deleteRoomsByRoomId(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("DELETE /rooms/%s", roomId)

	var delete_room Delete_Room
	if err := c.ShouldBindJSON(&delete_room); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}
	requestJSON, err := json.MarshalIndent(delete_room, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	log.Infof("request:\n%s", string(requestJSON))

	if delete_room.Requestor == nil {
		log.Warnf(MISS_REQUESTOR)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_REQUESTOR})
		return
	}

	room, err := s.getEditableRoom(roomId, c)
	if err != nil {
		return
	}

	var timeLeftInSeconds int
	if delete_room.TimeLeftInSeconds == nil {
		timeLeftInSeconds = 0
	} else {
		timeLeftInSeconds = *delete_room.TimeLeftInSeconds
	}
	room.updatedBy = *delete_room.Requestor
	room.updatedAt = time.Now()
	room.endTime = time.Now().Add(time.Second * time.Duration(timeLeftInSeconds))
	if delete_room.Reason == nil {
		room.earlyEndReason = "session terminated"
	} else if *delete_room.Reason == "" {
		room.earlyEndReason = "session terminated"
	} else {
		room.earlyEndReason = *delete_room.Reason
	}

	updateStmt := `update "room" set "updatedBy"=$1,
									 "updatedAt"=$2,
									 "endTime"=$3,
									 "earlyEndReason"=$4 where "id"=$5`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			room.updatedBy,
			room.updatedAt,
			room.endTime,
			room.earlyEndReason,
			room.id)
		if err == nil {
			break
		}
	}
	if err != nil {
		errorString := fmt.Sprintf("could not update database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	s.onChanges <- roomId
	log.Infof("deleted roomId '%s'", roomId)
	remarks := fmt.Sprintf("Ending roomId '%s' in %d minutes", roomId, timeLeftInSeconds/60)
	c.JSON(http.StatusOK, map[string]interface{}{"remarks": remarks})
}

func (s *RoomMgmtService) deleteUsersByUserId(c *gin.Context) {
	roomId := c.Param("roomid")
	userId := c.Param("userid")
	log.Infof("DELETE /rooms/%s/users/%s", roomId, userId)

	var delete_user Delete_User
	if err := c.ShouldBindJSON(&delete_user); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}
	requestJSON, err := json.MarshalIndent(delete_user, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	log.Infof("request:\n%s", string(requestJSON))

	if delete_user.Requestor == nil {
		log.Warnf(MISS_REQUESTOR)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_REQUESTOR})
		return
	}

	room, err := s.getEditableRoom(roomId, c)
	if err != nil {
		return
	}

	if room.status != ROOM_STARTED {
		log.Warnf(s.roomNotStarted(roomId))
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": s.roomNotStarted(roomId)})
		return
	}
	warn, err := s.kickUser(roomId, userId)
	if err != nil {
		log.Errorf(err.Error())
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
		return
	}
	if warn != nil {
		log.Warnf(warn.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": warn.Error()})
		return
	}
	remarks := fmt.Sprintf("kicked userId '%s' from roomId '%s'", userId, roomId)
	c.JSON(http.StatusOK, map[string]interface{}{"remarks": remarks})
}

func (s *RoomMgmtService) putAnnouncementsByRoomId(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("PUT /rooms/%s/announcements", roomId)

	var patch_room Patch_Room
	if err := c.ShouldBindJSON(&patch_room); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}
	err := s.patchRoomRequest(patch_room, c)
	if err != nil {
		return
	}

	room, err := s.getEditableRoom(roomId, c)
	if err != nil {
		return
	}

	err = s.putAnnouncement(&room, patch_room, c)
	if err != nil {
		return
	}

	updateStmt := `update "room" set "updatedBy"=$1,
									 "updatedAt"=$2 where "id"=$3`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			room.updatedBy,
			room.updatedAt,
			room.id)
		if err == nil {
			break
		}
	}
	if err != nil {
		errorString := fmt.Sprintf("could not update database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	s.onChanges <- roomId
	get_room, err := s.queryGetRoom(roomId, c)
	if err != nil {
		return
	}
	log.Infof("put announcements to roomId '%s'", roomId)
	c.JSON(http.StatusOK, get_room)
}

func (s *RoomMgmtService) deleteAnnouncementsByRoomId(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("DELETE /rooms/%s/announcements", roomId)

	var delete_announce Delete_Announcement
	if err := c.ShouldBindJSON(&delete_announce); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}
	requestJSON, err := json.MarshalIndent(delete_announce, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	log.Infof("request:\n%s", string(requestJSON))
	if delete_announce.Requestor == nil {
		log.Warnf(MISS_REQUESTOR)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_REQUESTOR})
		return
	}

	room, err := s.getEditableRoom(roomId, c)
	if err != nil {
		return
	}

	room.updatedBy = *delete_announce.Requestor
	room.updatedAt = time.Now()
	if len(room.announcements) == 0 {
		warnString := fmt.Sprintf("roomId '%s' has no announcements in database", roomId)
		log.Warnf(warnString)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": warnString})
		return
	}

	isDeleteAll := false
	if delete_announce.Id == nil {
		isDeleteAll = true
	} else if len(delete_announce.Id) == 0 {
		isDeleteAll = true
	}
	if isDeleteAll {
		room.announcements = make([]Announcement, 0)
		deleteStmt := `delete from "announcement" where "roomId"=$1`
		for retry := 0; retry < DB_RETRY; retry++ {
			_, err = s.postgresDB.Exec(deleteStmt, room.id)
			if err == nil {
				break
			}
		}
		if err != nil {
			errorString := fmt.Sprintf("could not delete from database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
			return
		}
	} else {
		idMap := make(map[string]int)
		toDeleteId := make([]int, 0)
		for _, announceId := range delete_announce.Id {
			_, exist := idMap[announceId]
			if exist {
				continue
			}
			idMap[announceId] = 1
			isFound := false
			for id, announcements := range room.announcements {
				if announcements.id == announceId {
					toDeleteId = append(toDeleteId, id)
					isFound = true
					break
				}
			}
			if !isFound {
				warnString := fmt.Sprintf("roomId '%s' does not have announceId '%s' in database", roomId, announceId)
				log.Warnf(warnString)
				c.JSON(http.StatusBadRequest, map[string]interface{}{"error": warnString})
				return
			}
		}
		deleteStmt := `delete from "announcement" where "id"=$1`
		for key := range idMap {
			for retry := 0; retry < DB_RETRY; retry++ {
				_, err = s.postgresDB.Exec(deleteStmt, key)
				if err == nil {
					break
				}
			}
			if err != nil {
				errorString := fmt.Sprintf("could not delete from database: %s", err)
				log.Errorf(errorString)
				c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
				return
			}
		}
		room.announcements = deleteSlices(room.announcements, toDeleteId)
	}

	updateStmt := `update "room" set "updatedBy"=$1,
									 "updatedAt"=$2 where "id"=$3`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			room.updatedBy,
			room.updatedAt,
			room.id)
		if err == nil {
			break
		}
	}
	if err != nil {
		errorString := fmt.Sprintf("could not update database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	s.onChanges <- roomId
	get_room, err := s.queryGetRoom(roomId, c)
	if err != nil {
		return
	}
	log.Infof("deleted announcements from roomId '%s'", roomId)
	c.JSON(http.StatusOK, get_room)
}

func (s *RoomMgmtService) getEditableRoom(roomId string, c *gin.Context) (Room, error) {
	room, err := s.queryRoom(roomId)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			errorString := fmt.Sprintf("roomId '%s' not found in database", roomId)
			log.Warnf(errorString)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
		} else {
			errorString := fmt.Sprintf("could not query database: %s", err.Error())
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		}
		return Room{}, err
	}
	if room.status == ROOM_ENDED {
		log.Warnf(s.roomHasEnded(roomId))
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": s.roomHasEnded(roomId)})
		return Room{}, errors.New(s.roomHasEnded(roomId))
	}

	return room, nil
}

func (s *RoomMgmtService) roomNotStarted(roomId string) string {
	return "RoomId '" + roomId + "' session not started"
}

func (s *RoomMgmtService) roomHasEnded(roomId string) string {
	return "RoomId '" + roomId + "' session has ended"
}

func (s *RoomMgmtService) patchRoomRequest(patch_room Patch_Room, c *gin.Context) error {
	requestJSON, err := json.MarshalIndent(patch_room, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return err
	}
	log.Infof("request:\n%s", string(requestJSON))
	if patch_room.Requestor == nil {
		log.Warnf(MISS_REQUESTOR)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_REQUESTOR})
		return errors.New(MISS_REQUESTOR)
	}
	return nil
}

func (s *RoomMgmtService) patchRoom(room *Room, patch_room Patch_Room, c *gin.Context) error {
	room.updatedBy = *patch_room.Requestor
	if patch_room.Name != nil {
		room.name = *patch_room.Name
	} else {
		room.name = ""
	}
	if patch_room.StartTime != nil {
		room.startTime = *patch_room.StartTime
		if room.status == ROOM_STARTED && room.startTime.After(time.Now()) {
			errorString := fmt.Sprintf("RoomId '%s' has started and new startTime is not due", room.id)
			log.Warnf(errorString)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
			return errors.New(errorString)
		}
	}
	if patch_room.EndTime != nil {
		room.endTime = *patch_room.EndTime
		if room.endTime.Before(room.startTime) {
			errorString := "endtime is before starttime"
			log.Warnf(errorString)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
			return errors.New(errorString)
		}
	}
	if len(patch_room.AllowedUserId) == 0 {
		room.allowedUserId = make(pq.StringArray, 0)
	}
	for _, patchuser := range patch_room.AllowedUserId {
		isPatched := false
		for _, user := range room.allowedUserId {
			if user == patchuser {
				isPatched = true
				break
			}
		}
		if isPatched {
			continue
		}

		room.allowedUserId = append(room.allowedUserId, patchuser)
	}

	updateStmt := `update "room" set "updatedBy"=$1,
									 "updatedAt"=$2,
									 "name"=$3,
									 "startTime"=$4,
									 "endTime"=$5,
									 "allowedUserId"=$6 where "id"=$7`
	var err error
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			room.updatedBy,
			room.updatedAt,
			room.name,
			room.startTime,
			room.endTime,
			room.allowedUserId,
			room.id)
		if err == nil {
			break
		}
	}
	if err != nil {
		errorString := fmt.Sprintf("could not update database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return err
	}

	idMap := make(map[string]int)
	for _, patch := range patch_room.Announcements {
		if patch.Id == nil {
			log.Warnf(MISS_ANNOUNCE_ID)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_ID})
			return errors.New(MISS_ANNOUNCE_ID)
		}
		_, exist := idMap[*patch.Id]
		if exist {
			log.Warnf(DUP_ANNOUNCE_ID)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": DUP_ANNOUNCE_ID})
			return errors.New(DUP_ANNOUNCE_ID)
		}
		idMap[*patch.Id] = 1

		isPatched := false
		for id := range room.announcements {
			if room.announcements[id].id == *patch.Id {
				isPatched = true
				if room.announcements[id].status == ANNOUNCEMENT_SENT {
					errorString := fmt.Sprintf("could not update sent announceId '%s'", *patch.Id)
					log.Warnf(errorString)
					c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
					return errors.New(errorString)
				}
				if patch.Message != nil {
					room.announcements[id].message = *patch.Message
				}
				if patch.RelativeFrom != nil {
					room.announcements[id].relativeFrom = *patch.RelativeFrom
				}
				if patch.RelativeTimeInSeconds != nil {
					room.announcements[id].relativeTimeInSeconds = *patch.RelativeTimeInSeconds
				}
				if patch.UserId != nil {
					for _, newuser := range patch.UserId {
						isPresent := false
						for _, olduser := range room.announcements[id].userId {
							if olduser == newuser {
								isPresent = true
								break
							}
						}
						if isPresent {
							continue
						}
						room.announcements[id].userId = append(room.announcements[id].userId, newuser)
					}
				}
				room.announcements[id].updatedBy = room.updatedBy
				room.announcements[id].updatedAt = room.updatedAt

				updateStmt := `update "announcement" set "message"=$1,
														 "relativeFrom"=$2,
														 "relativeTimeInSeconds"=$3,
														 "userId"=$4,
														 "updatedBy"=$5,
														 "updatedAt"=$6 where "id"=$7`
				for retry := 0; retry < DB_RETRY; retry++ {
					_, err = s.postgresDB.Exec(updateStmt,
						room.announcements[id].message,
						room.announcements[id].relativeFrom,
						room.announcements[id].relativeTimeInSeconds,
						room.announcements[id].userId,
						room.announcements[id].updatedBy,
						room.announcements[id].updatedAt,
						room.announcements[id].id)
					if err == nil {
						break
					}
				}
				if err != nil {
					errorString := fmt.Sprintf("could not update database: %s", err)
					log.Errorf(errorString)
					c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
					return err
				}
				break
			}
		}
		if isPatched {
			continue
		}

		var announcement Announcement
		announcement.status = ANNOUNCEMENT_QUEUED
		announcement.id = *patch.Id
		if patch.Message == nil || *patch.Message == "" {
			log.Warnf(MISS_ANNOUNCE_MSG)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_MSG})
			return errors.New(MISS_ANNOUNCE_MSG)
		}
		announcement.message = *patch.Message
		if patch.RelativeFrom != nil &&
			*patch.RelativeFrom != FROM_START &&
			*patch.RelativeFrom != FROM_END {
			log.Warnf(MISS_ANNOUNCE_REL)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_REL})
			return errors.New(MISS_ANNOUNCE_REL)
		}
		if patch.RelativeFrom == nil {
			announcement.relativeFrom = FROM_END
		} else {
			announcement.relativeFrom = *patch.RelativeFrom
		}
		if patch.RelativeTimeInSeconds == nil {
			log.Warnf(MISS_ANNOUNCE_TIME)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_TIME})
			return errors.New(MISS_ANNOUNCE_TIME)
		}
		announcement.relativeTimeInSeconds = *patch.RelativeTimeInSeconds
		announcement.userId = make(pq.StringArray, 0)
		announcement.userId = append(announcement.userId, patch.UserId...)
		announcement.createdBy = room.updatedBy
		announcement.createdAt = room.updatedAt
		announcement.updatedBy = room.updatedBy
		announcement.updatedAt = room.updatedAt
		room.announcements = append(room.announcements, announcement)

		insertStmt := `insert into "announcement"(  "id", 
													"roomId",
													"status",
													"message",
													"relativeFrom",
													"relativeTimeInSeconds",
													"userId",
													"createdBy",
													"createdAt",
													"updatedBy",
													"updatedAt" )
						values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
		var err error
		for retry := 0; retry < DB_RETRY; retry++ {
			_, err = s.postgresDB.Exec(insertStmt,
				announcement.id,
				room.id,
				announcement.status,
				announcement.message,
				announcement.relativeFrom,
				announcement.relativeTimeInSeconds,
				announcement.userId,
				announcement.createdBy,
				announcement.createdAt,
				announcement.updatedBy,
				announcement.updatedAt)
			if err == nil {
				break
			}
			if strings.Contains(err.Error(), DUP_PK) {
				break
			}
		}
		if err != nil {
			errorString := fmt.Sprintf("could not insert into database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
			return err
		}
	}

	return nil
}

func (s *RoomMgmtService) queryGetRoom(roomid string, c *gin.Context) (Get_Room, error) {
	queryStmt := `SELECT "id",
						 "name",
						 "status",
						 "startTime",
						 "endTime",
						 "allowedUserId",
						 "earlyEndReason",
						 "createdBy",
						 "createdAt",
						 "updatedBy",
						 "updatedAt" FROM "room" WHERE "id"=$1`
	var rooms *sql.Row
	for retry := 0; retry < DB_RETRY; retry++ {
		rooms = s.postgresDB.QueryRow(queryStmt, roomid)
		if rooms.Err() == nil {
			break
		}
		if strings.Contains(rooms.Err().Error(), NOT_FOUND_PK) {
			break
		}
	}
	if rooms.Err() != nil {
		errorString := fmt.Sprintf("could not query database: %s", rooms.Err())
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return Get_Room{}, rooms.Err()
	}
	var get_room Get_Room
	err := rooms.Scan(&get_room.Id,
		&get_room.Name,
		&get_room.Status,
		&get_room.StartTime,
		&get_room.EndTime,
		&get_room.AllowedUserId,
		&get_room.EarlyEndReason,
		&get_room.CreatedBy,
		&get_room.CreatedAt,
		&get_room.UpdatedBy,
		&get_room.UpdatedAt)
	if err != nil {
		errorString := fmt.Sprintf("could not query database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return Get_Room{}, err
	}

	get_room.Announcements = make([]Get_Announcement, 0)
	queryStmt = `SELECT "id",
						"message",
						"relativeFrom",
						"relativeTimeInSeconds",
						"userId",
						"createdBy",
						"createdAt",
						"updatedBy",
						"updatedAt"	FROM "announcement" WHERE "roomId"=$1`
	var announcements *sql.Rows
	for retry := 0; retry < DB_RETRY; retry++ {
		announcements, err = s.postgresDB.Query(queryStmt, roomid)
		if err == nil {
			break
		}
	}
	if err != nil {
		errorString := fmt.Sprintf("could not query database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return Get_Room{}, err
	}
	defer announcements.Close()
	for announcements.Next() {
		var get_announcement Get_Announcement
		err = announcements.Scan(&get_announcement.Id,
			&get_announcement.Message,
			&get_announcement.RelativeFrom,
			&get_announcement.RelativeTimeInSeconds,
			&get_announcement.UserId,
			&get_announcement.CreatedBy,
			&get_announcement.CreatedAt,
			&get_announcement.UpdatedBy,
			&get_announcement.UpdatedAt)
		if err != nil {
			errorString := fmt.Sprintf("could not query database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
			return Get_Room{}, err
		}
		get_room.Announcements = append(get_room.Announcements, get_announcement)
	}

	if get_room.Status == ROOM_STARTED {
		get_room.Users = append(get_room.Users, s.getPeers(get_room.Id)...)
	}
	return get_room, nil
}

func (s *RoomMgmtService) queryRoom(roomid string) (Room, error) {
	queryStmt := `SELECT "id",
						 "name",
						 "status",
						 "startTime",
						 "endTime",
						 "allowedUserId",
						 "earlyEndReason",
						 "createdBy",
						 "createdAt",
						 "updatedBy",
						 "updatedAt" FROM "room" WHERE "id"=$1`
	var rooms *sql.Row
	for retry := 0; retry < DB_RETRY; retry++ {
		rooms = s.postgresDB.QueryRow(queryStmt, roomid)
		if rooms.Err() == nil {
			break
		}
		if strings.Contains(rooms.Err().Error(), NOT_FOUND_PK) {
			break
		}
	}
	if rooms.Err() != nil {
		errorString := fmt.Sprintf("could not query database: %s", rooms.Err())
		log.Errorf(errorString)
		return Room{}, rooms.Err()
	}
	var room Room
	err := rooms.Scan(&room.id,
		&room.name,
		&room.status,
		&room.startTime,
		&room.endTime,
		&room.allowedUserId,
		&room.earlyEndReason,
		&room.createdBy,
		&room.createdAt,
		&room.updatedBy,
		&room.updatedAt)
	if err != nil {
		return Room{}, err
	}

	room.announcements = make([]Announcement, 0)
	queryStmt = `SELECT "id",
						"status",
						"message",
						"relativeFrom",
						"relativeTimeInSeconds",
						"userId",
						"createdBy",
						"createdAt",
						"updatedBy",
						"updatedAt"	FROM "announcement" WHERE "roomId"=$1`
	var announcements *sql.Rows
	for retry := 0; retry < DB_RETRY; retry++ {
		announcements, err = s.postgresDB.Query(queryStmt, roomid)
		if err == nil {
			break
		}
	}
	if err != nil {
		return Room{}, err
	}
	defer announcements.Close()
	for announcements.Next() {
		var announcement Announcement
		err = announcements.Scan(&announcement.id,
			&announcement.status,
			&announcement.message,
			&announcement.relativeFrom,
			&announcement.relativeTimeInSeconds,
			&announcement.userId,
			&announcement.createdBy,
			&announcement.createdAt,
			&announcement.updatedBy,
			&announcement.updatedAt)
		if err != nil {
			return Room{}, err
		}
		room.announcements = append(room.announcements, announcement)
	}

	return room, nil
}

func (s *RoomMgmtService) putAnnouncement(room *Room, patch_room Patch_Room, c *gin.Context) error {
	room.updatedBy = *patch_room.Requestor
	room.updatedAt = time.Now()

	idMap := make(map[string]int)
	for _, patch := range patch_room.Announcements {
		if patch.Id == nil {
			log.Warnf(MISS_ANNOUNCE_ID)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_ID})
			return errors.New(MISS_ANNOUNCE_ID)
		}
		_, exist := idMap[*patch.Id]
		if exist {
			log.Warnf(DUP_ANNOUNCE_ID)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": DUP_ANNOUNCE_ID})
			return errors.New(DUP_ANNOUNCE_ID)
		}
		idMap[*patch.Id] = 1

		var announcement Announcement
		announcement.status = ANNOUNCEMENT_QUEUED
		announcement.id = *patch.Id
		if patch.Message == nil || *patch.Message == "" {
			log.Warnf(MISS_ANNOUNCE_MSG)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_MSG})
			return errors.New(MISS_ANNOUNCE_MSG)
		}
		announcement.message = *patch.Message
		if patch.RelativeFrom != nil &&
			*patch.RelativeFrom != FROM_START &&
			*patch.RelativeFrom != FROM_END {
			log.Warnf(MISS_ANNOUNCE_REL)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_REL})
			return errors.New(MISS_ANNOUNCE_REL)
		}
		if patch.RelativeFrom == nil {
			announcement.relativeFrom = FROM_END
		} else {
			announcement.relativeFrom = *patch.RelativeFrom
		}
		if patch.RelativeTimeInSeconds == nil {
			log.Warnf(MISS_ANNOUNCE_TIME)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_TIME})
			return errors.New(MISS_ANNOUNCE_TIME)
		}
		announcement.relativeTimeInSeconds = *patch.RelativeTimeInSeconds
		announcement.createdBy = room.updatedBy
		announcement.createdAt = room.updatedAt
		announcement.updatedBy = room.updatedBy
		announcement.updatedAt = room.updatedAt
		announcement.userId = make(pq.StringArray, 0)
		announcement.userId = append(announcement.userId, patch.UserId...)

		isPatched := false
		for id := range room.announcements {
			if room.announcements[id].id == *patch.Id {
				isPatched = true
				if room.announcements[id].status == ANNOUNCEMENT_SENT {
					errorString := fmt.Sprintf("could not update already announced announceId '%s'", *patch.Id)
					log.Warnf(errorString)
					c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
					return errors.New(errorString)
				}
				room.announcements[id] = announcement
				updateStmt := `update "announcement" set "message"=$1,
														 "relativeFrom"=$2,
														 "relativeTimeInSeconds"=$3,
														 "userId"=$4,
														 "updatedBy"=$5,
														 "updatedAt"=$6 where "id"=$7`
				var err error
				for retry := 0; retry < DB_RETRY; retry++ {
					_, err = s.postgresDB.Exec(updateStmt,
						room.announcements[id].message,
						room.announcements[id].relativeFrom,
						room.announcements[id].relativeTimeInSeconds,
						room.announcements[id].userId,
						room.announcements[id].updatedBy,
						room.announcements[id].updatedAt,
						room.announcements[id].id)
					if err == nil {
						break
					}
				}
				if err != nil {
					errorString := fmt.Sprintf("could not update database: %s", err)
					log.Errorf(errorString)
					c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
					return err
				}

				break
			}
		}
		if isPatched {
			continue
		}

		room.announcements = append(room.announcements, announcement)
		insertStmt := `insert into "announcement"(  "id", 
													"roomId",
													"status",
													"message",
													"relativeFrom",
													"relativeTimeInSeconds",
													"userId",
													"createdBy",
													"createdAt",
													"updatedBy",
													"updatedAt" )
						values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
		var err error
		for retry := 0; retry < DB_RETRY; retry++ {
			_, err = s.postgresDB.Exec(insertStmt,
				announcement.id,
				room.id,
				announcement.status,
				announcement.message,
				announcement.relativeFrom,
				announcement.relativeTimeInSeconds,
				announcement.userId,
				announcement.createdBy,
				announcement.createdAt,
				announcement.updatedBy,
				announcement.updatedAt)
			if err == nil {
				break
			}
			if strings.Contains(err.Error(), DUP_PK) {
				break
			}
		}
		if err != nil {
			errorString := fmt.Sprintf("could not insert into database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
			return err
		}
	}

	return nil
}

func deleteSlices[T any](slice []T, ids []int) []T {
	for id := len(ids) - 1; id >= 0; id-- {
		slice = removeSlice(slice, ids[id])
	}
	return slice
}

func removeSlice[T any](slice []T, id int) []T {
	if id >= len(slice) {
		return slice
	}
	copy(slice[id:], slice[id+1:])
	return slice[:len(slice)-1]
}

type StartRooms struct {
	timeTick time.Time
	roomname string
}

type Terminations struct {
	timeTick time.Time
	roomname string
	reason   string
}

type Announcements struct {
	timeTick time.Time
	message  string
	userId   pq.StringArray
}

type AnnounceKey struct {
	roomId     string
	announceId string
}

type RoomMgmtService struct {
	conf Config

	timeLive       string
	timeReady      string
	roomService    *sdk.Room
	postgresDB     *sql.DB
	onChanges      chan string
	pollInterval   time.Duration
	systemUid      string
	systemUsername string

	roomStarts       map[string]StartRooms
	roomStartKeys    []string
	roomEnds         map[string]Terminations
	roomEndKeys      []string
	announcements    map[AnnounceKey]Announcements
	announcementKeys []AnnounceKey
}

func NewRoomMgmtService(config Config) *RoomMgmtService {
	timeLive := time.Now().Format(time.RFC3339)

	log.Infof("--- Connecting to PostgreSql ---")
	addrSplit := strings.Split(config.Postgres.Addr, ":")
	port, err := strconv.Atoi(addrSplit[1])
	if err != nil {
		log.Panicf("invalid port number: %s\n", addrSplit[1])
	}
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		addrSplit[0],
		port,
		config.Postgres.User,
		config.Postgres.Password,
		config.Postgres.Database)
	log.Infof("psqlconn: %s", psqlconn)
	var postgresDB *sql.DB
	for retry := 0; retry < DB_RETRY; retry++ {
		postgresDB, err = sql.Open("postgres", psqlconn)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to connect to database: %v\n", err)
	}
	for retry := 0; retry < DB_RETRY; retry++ {
		err = postgresDB.Ping()
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to ping database: %v\n", err)
	}
	createStmt := `CREATE TABLE IF NOT EXISTS
					 "room"( "id"             UUID PRIMARY KEY,
							 "name"           TEXT,
							 "status"         TEXT NOT NULL,
							 "startTime"      TIMESTAMP NOT NULL,
							 "endTime"        TIMESTAMP NOT NULL,
							 "allowedUserId"  TEXT ARRAY,
							 "earlyEndReason" TEXT,
							 "createdBy"      TEXT NOT NULL,
							 "createdAt"      TIMESTAMP NOT NULL,
							 "updatedBy"      TEXT NOT NULL,
							 "updatedAt"      TIMESTAMP NOT NULL)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}
	createStmt = `CREATE TABLE IF NOT EXISTS
					 "announcement"( "id"                    UUID PRIMARY KEY,
									 "roomId"                UUID NOT NULL,
									 "status"                TEXT NOT NULL,
									 "message"               TEXT NOT NULL,
									 "relativeFrom"          TEXT NOT NULL,
									 "relativeTimeInSeconds" INT NOT NULL,
									 "userId"                TEXT ARRAY,
									 "createdAt"             TIMESTAMP NOT NULL,
									 "createdBy"             TEXT NOT NULL,
									 "updatedAt"             TIMESTAMP NOT NULL,
									 "updatedBy"             TEXT NOT NULL,
									 CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < DB_RETRY; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}

	log.Infof("--- Connecting to Room Signal ---")
	log.Infof("attempt gRPC connection to %s", config.Signal.Addr)
	sdk_connector := sdk.NewConnector(config.Signal.Addr)
	if sdk_connector == nil {
		log.Panicf("connection to %s fail", config.Signal.Addr)
	}
	roomService := sdk.NewRoom(sdk_connector)
	timeReady := time.Now().Format(time.RFC3339)

	s := &RoomMgmtService{
		conf:           config,
		timeLive:       timeLive,
		timeReady:      timeReady,
		roomService:    roomService,
		postgresDB:     postgresDB,
		onChanges:      make(chan string, 2048),
		pollInterval:   time.Duration(config.RoomMgmt.PollInSeconds) * time.Second,
		systemUid:      config.RoomMgmt.SystemUid,
		systemUsername: config.RoomMgmt.SystemUsername,
	}
	go s.RoomMgmtSentinel()
	<-s.onChanges
	go s.start()
	return s
}

func (s *RoomMgmtService) start() {
	defer s.postgresDB.Close()
	defer s.roomService.Close()
	defer close(s.onChanges)

	log.Infof("--- Starting HTTP-API Server ---")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/liveness", s.getLiveness)
	router.GET("/readiness", s.getReadiness)
	router.POST("/rooms", s.postRooms)
	router.GET("/rooms", s.getRooms)
	router.GET("/rooms/:roomid", s.getRoomsByRoomid)
	router.PATCH("/rooms/:roomid", s.patchRoomsByRoomid)
	router.DELETE("/rooms/:roomid", s.deleteRoomsByRoomId)
	router.DELETE("/rooms/:roomid/users/:userid", s.deleteUsersByUserId)
	router.PUT("/rooms/:roomid/announcements", s.putAnnouncementsByRoomId)
	router.DELETE("/rooms/:roomid/announcements", s.deleteAnnouncementsByRoomId)

	if s.conf.RoomMgmt.Cert != "" && s.conf.RoomMgmt.Key != "" {
		log.Infof("HTTP service starting at %s", s.conf.RoomMgmt.Addr)
		log.Panicf("%s", router.RunTLS(s.conf.RoomMgmt.Addr, s.conf.RoomMgmt.Cert, s.conf.RoomMgmt.Key))
	} else {
		log.Infof("HTTP service starting at %s", s.conf.RoomMgmt.Addr)
		log.Panicf("%s", router.Run(s.conf.RoomMgmt.Addr))
	}
}
