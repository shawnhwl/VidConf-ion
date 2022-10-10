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
)

type Announcement struct {
	Id                    string         `json:"id"`
	Status                string         `json:"status"`
	Message               string         `json:"message"`
	RelativeFrom          string         `json:"relativeFrom"`
	RelativeTimeInSeconds int64          `json:"relativeTimeInSeconds"`
	UserId                pq.StringArray `json:"userId"`
	CreatedBy             string         `json:"createdBy"`
	CreatedAt             time.Time      `json:"createdAt"`
	UpdatedBy             string         `json:"updatedBy"`
	UpdatedAt             time.Time      `json:"updatedAt"`
}

type Room struct {
	Id             string         `json:"id"`
	Name           string         `json:"name"`
	Status         string         `json:"status"`
	StartTime      time.Time      `json:"startTime"`
	EndTime        time.Time      `json:"endTime"`
	Announcements  []Announcement `json:"announcements"`
	AllowedUserId  pq.StringArray `json:"allowedUserId"`
	EarlyEndReason string         `json:"earlyEndReason"`
	CreatedBy      string         `json:"createdBy"`
	CreatedAt      time.Time      `json:"createdAt"`
	UpdatedBy      string         `json:"updatedBy"`
	UpdatedAt      time.Time      `json:"updatedAt"`
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
	room.Id = roomId
	room.Name = ""
	room.Status = ROOM_BOOKED
	room.StartTime = *patch_room.StartTime
	room.EndTime = *patch_room.EndTime
	room.Announcements = make([]Announcement, 0)
	room.AllowedUserId = make(pq.StringArray, 0)
	room.EarlyEndReason = ""
	room.CreatedBy = *patch_room.Requestor
	room.CreatedAt = time.Now()
	room.UpdatedBy = *patch_room.Requestor
	room.UpdatedAt = room.CreatedAt
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
										"updatedAt") values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	_, err = s.postgresDB.Exec(insertStmt,
		room.Id,
		room.Name,
		room.Status,
		room.StartTime.Format(time.RFC3339),
		room.EndTime.Format(time.RFC3339),
		pq.Array(room.AllowedUserId),
		room.EarlyEndReason,
		room.CreatedBy,
		room.CreatedAt.Format(time.RFC3339),
		room.UpdatedBy,
		room.UpdatedAt.Format(time.RFC3339))
	if err != nil {
		errorString := fmt.Sprintf("could not insert into database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	err = s.patchRoom(&room, patch_room, c)
	if err != nil {
		deleteStmt := `delete from "room" where "id"=$1`
		_, err = s.postgresDB.Exec(deleteStmt, room.Id)
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
	rows, err := s.postgresDB.Query(`SELECT "id" FROM "room"`)
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

	room.UpdatedAt = time.Now()
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
	room.UpdatedBy = *delete_room.Requestor
	room.UpdatedAt = time.Now()
	room.EndTime = time.Now().Add(time.Second * time.Duration(timeLeftInSeconds))
	if delete_room.Reason == nil {
		room.EarlyEndReason = "session terminated"
	} else if *delete_room.Reason == "" {
		room.EarlyEndReason = "session terminated"
	} else {
		room.EarlyEndReason = *delete_room.Reason
	}

	updateStmt := `update "room" set "updatedBy"=$1,
									 "updatedAt"=$2,
									 "endTime"=$3,
									 "earlyEndReason"=$4 where "id"=$5`
	_, err = s.postgresDB.Exec(updateStmt,
		room.UpdatedBy,
		room.UpdatedAt,
		room.EndTime,
		room.EarlyEndReason,
		room.Id)
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

	if room.Status != ROOM_STARTED {
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
	_, err = s.postgresDB.Exec(updateStmt,
		room.UpdatedBy,
		room.UpdatedAt,
		room.Id)
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

	room.UpdatedBy = *delete_announce.Requestor
	room.UpdatedAt = time.Now()
	if len(room.Announcements) == 0 {
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
		room.Announcements = make([]Announcement, 0)
		deleteStmt := `delete from "announcement" where "room_id"=$1`
		_, err = s.postgresDB.Exec(deleteStmt, room.Id)
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
			for id, announcements := range room.Announcements {
				if announcements.Id == announceId {
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
			_, err = s.postgresDB.Exec(deleteStmt, key)
			if err != nil {
				errorString := fmt.Sprintf("could not delete from database: %s", err)
				log.Errorf(errorString)
				c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
				return
			}
		}
		room.Announcements = deleteSlices(room.Announcements, toDeleteId)
	}

	updateStmt := `update "room" set "updatedBy"=$1,
									 "updatedAt"=$2 where "id"=$3`
	_, err = s.postgresDB.Exec(updateStmt,
		room.UpdatedBy,
		room.UpdatedAt,
		room.Id)
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
		if strings.Contains(err.Error(), "no rows in result set") {
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
	if room.Status == ROOM_ENDED {
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
	room.UpdatedBy = *patch_room.Requestor
	if patch_room.Name != nil {
		room.Name = *patch_room.Name
	} else {
		room.Name = ""
	}
	if patch_room.StartTime != nil {
		room.StartTime = *patch_room.StartTime
		if room.Status == ROOM_STARTED && room.StartTime.After(time.Now()) {
			errorString := fmt.Sprintf("RoomId '%s' has started and new startTime is not due", room.Id)
			log.Warnf(errorString)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
			return errors.New(errorString)
		}
	}
	if patch_room.EndTime != nil {
		room.EndTime = *patch_room.EndTime
		if room.EndTime.Before(room.StartTime) {
			errorString := "endtime is before starttime"
			log.Warnf(errorString)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
			return errors.New(errorString)
		}
	}
	if len(patch_room.AllowedUserId) == 0 {
		room.AllowedUserId = make(pq.StringArray, 0)
	}
	for _, patchuser := range patch_room.AllowedUserId {
		isPatched := false
		for _, user := range room.AllowedUserId {
			if user == patchuser {
				isPatched = true
				break
			}
		}
		if isPatched {
			continue
		}

		room.AllowedUserId = append(room.AllowedUserId, patchuser)
	}

	updateStmt := `update "room" set "updatedBy"=$1,
									 "updatedAt"=$2,
									 "name"=$3,
									 "startTime"=$4,
									 "endTime"=$5,
									 "allowedUserId"=$6 where "id"=$7`
	_, err := s.postgresDB.Exec(updateStmt,
		room.UpdatedBy,
		room.UpdatedAt,
		room.Name,
		room.StartTime,
		room.EndTime,
		pq.Array(room.AllowedUserId),
		room.Id)
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
		for id := range room.Announcements {
			if room.Announcements[id].Id == *patch.Id {
				isPatched = true
				if room.Announcements[id].Status == ANNOUNCEMENT_SENT {
					errorString := fmt.Sprintf("could not update sent announceId '%s'", *patch.Id)
					log.Warnf(errorString)
					c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
					return errors.New(errorString)
				}
				if patch.Message != nil {
					room.Announcements[id].Message = *patch.Message
				}
				if patch.RelativeFrom != nil {
					room.Announcements[id].RelativeFrom = *patch.RelativeFrom
				}
				if patch.RelativeTimeInSeconds != nil {
					room.Announcements[id].RelativeTimeInSeconds = *patch.RelativeTimeInSeconds
				}
				if patch.UserId != nil {
					for _, newuser := range patch.UserId {
						isPresent := false
						for _, olduser := range room.Announcements[id].UserId {
							if olduser == newuser {
								isPresent = true
								break
							}
						}
						if isPresent {
							continue
						}
						room.Announcements[id].UserId = append(room.Announcements[id].UserId, newuser)
					}
				}
				room.Announcements[id].UpdatedBy = room.UpdatedBy
				room.Announcements[id].UpdatedAt = room.UpdatedAt

				updateStmt := `update "announcement" set "message"=$1,
														 "relativeFrom"=$2,
														 "relativeTimeInSeconds"=$3,
														 "userId"=$4,
														 "updatedBy"=$5,
														 "updatedAt"=$6 where "id"=$7`
				_, err := s.postgresDB.Exec(updateStmt,
					room.Announcements[id].Message,
					room.Announcements[id].RelativeFrom,
					room.Announcements[id].RelativeTimeInSeconds,
					pq.Array(room.Announcements[id].UserId),
					room.Announcements[id].UpdatedBy,
					room.Announcements[id].UpdatedAt,
					room.Announcements[id].Id)
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
		announcement.Status = ANNOUNCEMENT_QUEUED
		announcement.Id = *patch.Id
		if patch.Message == nil || *patch.Message == "" {
			log.Warnf(MISS_ANNOUNCE_MSG)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_MSG})
			return errors.New(MISS_ANNOUNCE_MSG)
		}
		announcement.Message = *patch.Message
		if patch.RelativeFrom != nil &&
			*patch.RelativeFrom != FROM_START &&
			*patch.RelativeFrom != FROM_END {
			log.Warnf(MISS_ANNOUNCE_REL)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_REL})
			return errors.New(MISS_ANNOUNCE_REL)
		}
		if patch.RelativeFrom == nil {
			announcement.RelativeFrom = FROM_END
		} else {
			announcement.RelativeFrom = *patch.RelativeFrom
		}
		if patch.RelativeTimeInSeconds == nil {
			log.Warnf(MISS_ANNOUNCE_TIME)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_TIME})
			return errors.New(MISS_ANNOUNCE_TIME)
		}
		announcement.RelativeTimeInSeconds = *patch.RelativeTimeInSeconds
		announcement.UserId = make(pq.StringArray, 0)
		announcement.UserId = append(announcement.UserId, patch.UserId...)
		announcement.CreatedBy = room.UpdatedBy
		announcement.CreatedAt = room.UpdatedAt
		announcement.UpdatedBy = room.UpdatedBy
		announcement.UpdatedAt = room.UpdatedAt
		room.Announcements = append(room.Announcements, announcement)

		insertStmt := `insert into "announcement"(  "id", 
													"room_id",
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
		_, err := s.postgresDB.Exec(insertStmt,
			announcement.Id,
			room.Id,
			announcement.Status,
			announcement.Message,
			announcement.RelativeFrom,
			announcement.RelativeTimeInSeconds,
			pq.Array(announcement.UserId),
			announcement.CreatedBy,
			announcement.CreatedAt,
			announcement.UpdatedBy,
			announcement.UpdatedAt)
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
	queryStmt := `SELECT * FROM "room" WHERE "id"=$1`
	rooms := s.postgresDB.QueryRow(queryStmt, roomid)
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
	queryStmt = `SELECT * FROM "announcement" WHERE "room_id"=$1`
	announcements, err := s.postgresDB.Query(queryStmt, roomid)
	if err != nil {
		errorString := fmt.Sprintf("could not query database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return Get_Room{}, err
	}
	defer announcements.Close()
	for announcements.Next() {
		var get_announcement Get_Announcement
		var id string
		var status string
		err = announcements.Scan(&get_announcement.Id,
			&id,
			&status,
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
	queryStmt := `SELECT * FROM "room" WHERE "id"=$1`
	rooms := s.postgresDB.QueryRow(queryStmt, roomid)
	if rooms.Err() != nil {
		return Room{}, rooms.Err()
	}
	var room Room
	err := rooms.Scan(&room.Id,
		&room.Name,
		&room.Status,
		&room.StartTime,
		&room.EndTime,
		&room.AllowedUserId,
		&room.EarlyEndReason,
		&room.CreatedBy,
		&room.CreatedAt,
		&room.UpdatedBy,
		&room.UpdatedAt)
	if err != nil {
		return Room{}, err
	}

	room.Announcements = make([]Announcement, 0)
	queryStmt = `SELECT * FROM "announcement" WHERE "room_id"=$1`
	announcements, err := s.postgresDB.Query(queryStmt, roomid)
	if err != nil {
		return Room{}, err
	}
	defer announcements.Close()
	for announcements.Next() {
		var announcement Announcement
		var id string
		err = announcements.Scan(&announcement.Id,
			&id,
			&announcement.Status,
			&announcement.Message,
			&announcement.RelativeFrom,
			&announcement.RelativeTimeInSeconds,
			&announcement.UserId,
			&announcement.CreatedBy,
			&announcement.CreatedAt,
			&announcement.UpdatedBy,
			&announcement.UpdatedAt)
		if err != nil {
			return Room{}, err
		}
		room.Announcements = append(room.Announcements, announcement)
	}

	return room, nil
}

func (s *RoomMgmtService) putAnnouncement(room *Room, patch_room Patch_Room, c *gin.Context) error {
	room.UpdatedBy = *patch_room.Requestor
	room.UpdatedAt = time.Now()

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
		announcement.Status = ANNOUNCEMENT_QUEUED
		announcement.Id = *patch.Id
		if patch.Message == nil || *patch.Message == "" {
			log.Warnf(MISS_ANNOUNCE_MSG)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_MSG})
			return errors.New(MISS_ANNOUNCE_MSG)
		}
		announcement.Message = *patch.Message
		if patch.RelativeFrom != nil &&
			*patch.RelativeFrom != FROM_START &&
			*patch.RelativeFrom != FROM_END {
			log.Warnf(MISS_ANNOUNCE_REL)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_REL})
			return errors.New(MISS_ANNOUNCE_REL)
		}
		if patch.RelativeFrom == nil {
			announcement.RelativeFrom = FROM_END
		} else {
			announcement.RelativeFrom = *patch.RelativeFrom
		}
		if patch.RelativeTimeInSeconds == nil {
			log.Warnf(MISS_ANNOUNCE_TIME)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_ANNOUNCE_TIME})
			return errors.New(MISS_ANNOUNCE_TIME)
		}
		announcement.RelativeTimeInSeconds = *patch.RelativeTimeInSeconds
		announcement.CreatedBy = room.UpdatedBy
		announcement.CreatedAt = room.UpdatedAt
		announcement.UpdatedBy = room.UpdatedBy
		announcement.UpdatedAt = room.UpdatedAt
		announcement.UserId = make(pq.StringArray, 0)
		announcement.UserId = append(announcement.UserId, patch.UserId...)

		isPatched := false
		for id := range room.Announcements {
			if room.Announcements[id].Id == *patch.Id {
				isPatched = true
				if room.Announcements[id].Status == ANNOUNCEMENT_SENT {
					errorString := fmt.Sprintf("could not update sent announceId '%s'", *patch.Id)
					log.Warnf(errorString)
					c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
					return errors.New(errorString)
				}
				room.Announcements[id] = announcement
				updateStmt := `update "announcement" set "message"=$1,
														 "relativeFrom"=$2,
														 "relativeTimeInSeconds"=$3,
														 "userId"=$4,
														 "updatedBy"=$5,
														 "updatedAt"=$6 where "id"=$7`
				_, err := s.postgresDB.Exec(updateStmt,
					room.Announcements[id].Message,
					room.Announcements[id].RelativeFrom,
					room.Announcements[id].RelativeTimeInSeconds,
					pq.Array(room.Announcements[id].UserId),
					room.Announcements[id].UpdatedBy,
					room.Announcements[id].UpdatedAt,
					room.Announcements[id].Id)
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

		room.Announcements = append(room.Announcements, announcement)
		insertStmt := `insert into "announcement"(  "id", 
													"room_id",
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
		_, err := s.postgresDB.Exec(insertStmt,
			announcement.Id,
			room.Id,
			announcement.Status,
			announcement.Message,
			announcement.RelativeFrom,
			announcement.RelativeTimeInSeconds,
			pq.Array(announcement.UserId),
			announcement.CreatedBy,
			announcement.CreatedAt,
			announcement.UpdatedBy,
			announcement.UpdatedAt)
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
	userId   []string
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
	postgresDB, err := sql.Open("postgres", psqlconn)
	if err != nil {
		log.Panicf("Unable to connect to database: %v\n", err)
	}
	err = postgresDB.Ping()
	if err != nil {
		log.Panicf("Unable to ping database: %v\n", err)
	}
	createStmt := `CREATE TABLE IF NOT EXISTS
		room( id             UUID PRIMARY KEY,
			  name           TEXT,
			  status         TEXT NOT NULL,
			  startTime      TIMESTAMP NOT NULL,
			  endTime        TIMESTAMP NOT NULL,
			  allowedUserId  TEXT ARRAY,
			  earlyEndReason TEXT,
			  createdBy      TEXT NOT NULL,
			  createdAt      TIMESTAMP NOT NULL,
			  updatedBy      TEXT NOT NULL,
			  updatedAt      TIMESTAMP NOT NULL)`
	_, err = postgresDB.Exec(createStmt)
	if err != nil {
		log.Panicf("Unable to execute sql statement: %v\n", err)
	}
	createStmt = `CREATE TABLE IF NOT EXISTS
		announcement( id                    UUID PRIMARY KEY,
					  room_id               UUID NOT NULL,
					  status                TEXT NOT NULL,
					  message               TEXT NOT NULL,
					  relativeFrom          TEXT,
					  relativeTimeInSeconds INT,
					  userId                TEXT ARRAY,
					  createdBy             TEXT NOT NULL,
					  createdAt             TIMESTAMP NOT NULL,
					  updatedBy             TEXT NOT NULL,
					  updatedAt             TIMESTAMP NOT NULL,
					  CONSTRAINT fk_room FOREIGN KEY(room_id) REFERENCES room(id) ON DELETE CASCADE)`
	_, err = postgresDB.Exec(createStmt)
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
	s.testAPI(router)

	if s.conf.RoomMgmt.Cert != "" && s.conf.RoomMgmt.Key != "" {
		log.Infof("HTTP service starting at %s", s.conf.RoomMgmt.Addr)
		log.Panicf("%s", router.RunTLS(s.conf.RoomMgmt.Addr, s.conf.RoomMgmt.Cert, s.conf.RoomMgmt.Key))
	} else {
		log.Infof("HTTP service starting at %s", s.conf.RoomMgmt.Addr)
		log.Panicf("%s", router.Run(s.conf.RoomMgmt.Addr))
	}
}

func (s *RoomMgmtService) testAPI(router *gin.Engine) {
	router.POST("/rooms/:roomid/:message/:toid", func(c *gin.Context) {
		roomid := c.Param("roomid")
		message := c.Param("message")
		toid := c.Param("toid")
		log.Infof("POST /rooms/%s/%s/%s", roomid, message, toid)
		userids := make([]string, 0)
		if toid != "all" {
			userids = append(userids, toid)
		}
		err := s.postMessage(roomid, message, userids)
		if err != nil {
			c.String(http.StatusInternalServerError,
				"POST /rooms/%s/%s/%s", roomid, message, toid, err.Error())
			return
		}
		c.String(http.StatusOK, "POST /rooms/%s/%s/%s", roomid, message, toid)
	})
}
