package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	"github.com/pion/ion/pkg/db"
)

const (
	NOT_READY          string = "service is not yet ready"
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

	RETRY_COUNT  int           = 3
	RETRY_DELAY  time.Duration = 5 * time.Second
	DUP_PK       string        = "duplicate key value violates unique constraint"
	NOT_FOUND_PK string        = "no rows in result set"

	RECONNECTION_INTERVAL time.Duration = 10 * time.Second
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

type PatchAnnouncement struct {
	Id                    *string        `json:"id,omitempty"`
	Message               *string        `json:"message,omitempty"`
	RelativeFrom          *string        `json:"relativeFrom,omitempty"`
	RelativeTimeInSeconds *int64         `json:"relativeTimeInSeconds,omitempty"`
	UserId                pq.StringArray `json:"userId,omitempty"`
}

type PatchRoom struct {
	Requestor     *string             `json:"requestor,omitempty"`
	Name          *string             `json:"name,omitempty"`
	StartTime     *time.Time          `json:"startTime,omitempty"`
	EndTime       *time.Time          `json:"endTime,omitempty"`
	Announcements []PatchAnnouncement `json:"announcements,omitempty"`
	AllowedUserId pq.StringArray      `json:"allowedUserId,omitempty"`
}

type GetAnnouncement struct {
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

type GetRoom struct {
	Id             string            `json:"id"`
	Name           string            `json:"name"`
	Status         string            `json:"status"`
	StartTime      time.Time         `json:"startTime"`
	EndTime        time.Time         `json:"endTime"`
	Announcements  []GetAnnouncement `json:"announcements"`
	AllowedUserId  pq.StringArray    `json:"allowedUserId"`
	Users          []User            `json:"users"`
	EarlyEndReason string            `json:"earlyEndReason"`
	CreatedBy      string            `json:"createdBy"`
	CreatedAt      time.Time         `json:"createdAt"`
	UpdatedBy      string            `json:"updatedBy"`
	UpdatedAt      time.Time         `json:"updatedAt"`
}

type DeleteRoom struct {
	Requestor         *string `json:"requestor,omitempty"`
	TimeLeftInSeconds *int    `json:"timeLeftInSeconds,omitempty"`
	Reason            *string `json:"reason,omitempty"`
}

type DeleteUser struct {
	Requestor *string `json:"requestor,omitempty"`
}

type DeleteAnnouncement struct {
	Requestor *string  `json:"requestor,omitempty"`
	Id        []string `json:"id"`
}

type RoomRecord struct {
	id        string
	name      string
	startTime time.Time
	endTime   time.Time
}

type GetChats struct {
	Id          string    `json:"id"`
	Name        string    `json:"name"`
	StartTime   time.Time `json:"startTime"`
	RecordCount int       `json:"recordCount"`
}

type GetChatRange struct {
	Msg ChatPayloads `json:"msg"`
}

type ChatPayload struct {
	Uid        string      `json:"uid"`
	Name       string      `json:"name"`
	Text       *string     `json:"text,omitempty"`
	Timestamp  time.Time   `json:"timestamp"`
	Base64File *Attachment `json:"base64File,omitempty"`
}

type Attachment struct {
	Name     string  `json:"name"`
	Size     int     `json:"size"`
	Data     string  `json:"data"`
	FilePath *string `json:"filePath,omitempty"`
}

type ChatPayloads []ChatPayload

func (p ChatPayloads) Len() int {
	return len(p)
}

func (p ChatPayloads) Less(i, j int) bool {
	return p[i].Timestamp.Before(p[j].Timestamp)
}

func (p ChatPayloads) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (s *RoomMgmtService) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomMgmtService) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, NOT_READY)
		return
	}
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func (s *RoomMgmtService) postRooms(c *gin.Context) {
	log.Infof("POST /rooms")
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	var patchRoom PatchRoom
	if err := c.ShouldBindJSON(&patchRoom); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}

	err := s.patchRoomRequest(patchRoom, c)
	if err != nil {
		return
	}

	if patchRoom.StartTime == nil {
		log.Warnf(MISS_START_TIME)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_START_TIME})
		return
	}
	if patchRoom.EndTime == nil {
		log.Warnf(MISS_END_TIME)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_END_TIME})
		return
	}

	var room Room
	room.id = s.getPlaybackUuid(false)
	room.name = ""
	room.status = ROOM_BOOKED
	room.startTime = *patchRoom.StartTime
	room.endTime = *patchRoom.EndTime
	room.announcements = make([]Announcement, 0)
	room.allowedUserId = make(pq.StringArray, 0)
	room.earlyEndReason = ""
	room.createdBy = *patchRoom.Requestor
	room.createdAt = time.Now()
	room.updatedBy = *patchRoom.Requestor
	room.updatedAt = room.createdAt
	insertStmt := `insert into "` + s.roomMgmtSchema + `"."room"(   "id",
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
	for retry := 0; retry < RETRY_COUNT; retry++ {
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
			room.id = s.getPlaybackUuid(false)
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		errorString := fmt.Sprintf("could not insert into database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	err = s.patchRoom(&room, patchRoom, c)
	if err != nil {
		deleteStmt := `delete from "` + s.roomMgmtSchema + `"."room" where "id"=$1`
		for retry := 0; retry < RETRY_COUNT; retry++ {
			_, err = s.postgresDB.Exec(deleteStmt, room.id)
			if err == nil {
				break
			}
			time.Sleep(RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not delete from database: %s", err)
		}
		return
	}

	s.onRoomChanges <- room.id
	getRoom, err := s.queryGetRoom(room.id, c)
	if err != nil {
		return
	}
	log.Infof("posted roomId '%s'", room.id)
	c.JSON(http.StatusOK, getRoom)
}

func (s *RoomMgmtService) getRooms(c *gin.Context) {
	log.Infof("GET /rooms")
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	var rooms Rooms
	rooms.Ids = make(pq.StringArray, 0)
	var rows *sql.Rows
	var err error
	queryStmt := `SELECT "id" FROM "` + s.roomMgmtSchema + `"."room"`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		rows, err = s.postgresDB.Query(queryStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
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
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	getRoom, err := s.queryGetRoom(roomId, c)
	if err != nil {
		return
	}

	c.JSON(http.StatusOK, getRoom)
}

func (s *RoomMgmtService) patchRoomsByRoomid(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("PATCH /rooms/%s", roomId)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	var patchRoom PatchRoom
	if err := c.ShouldBindJSON(&patchRoom); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}
	err := s.patchRoomRequest(patchRoom, c)
	if err != nil {
		return
	}

	room, err := s.getEditableRoom(roomId, c)
	if err != nil {
		return
	}

	room.updatedAt = time.Now()
	err = s.patchRoom(&room, patchRoom, c)
	if err != nil {
		return
	}

	s.onRoomChanges <- roomId
	getRoom, err := s.queryGetRoom(roomId, c)
	if err != nil {
		return
	}
	log.Infof("patched roomId '%s'", roomId)
	c.JSON(http.StatusOK, getRoom)
}

func (s *RoomMgmtService) deleteRoomsByRoomId(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("DELETE /rooms/%s", roomId)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	var deleteRoom DeleteRoom
	if err := c.ShouldBindJSON(&deleteRoom); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}
	requestJSON, err := json.MarshalIndent(deleteRoom, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	log.Infof("request:\n%s", string(requestJSON))

	if deleteRoom.Requestor == nil {
		log.Warnf(MISS_REQUESTOR)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_REQUESTOR})
		return
	}

	room, err := s.getEditableRoom(roomId, c)
	if err != nil {
		return
	}

	var timeLeftInSeconds int
	if deleteRoom.TimeLeftInSeconds == nil {
		timeLeftInSeconds = 0
	} else {
		timeLeftInSeconds = *deleteRoom.TimeLeftInSeconds
	}
	room.updatedBy = *deleteRoom.Requestor
	room.updatedAt = time.Now()
	room.endTime = time.Now().Add(time.Second * time.Duration(timeLeftInSeconds))
	if deleteRoom.Reason == nil {
		room.earlyEndReason = "session terminated"
	} else if *deleteRoom.Reason == "" {
		room.earlyEndReason = "session terminated"
	} else {
		room.earlyEndReason = *deleteRoom.Reason
	}

	updateStmt := `update "` + s.roomMgmtSchema + `"."room"
					set "updatedBy"=$1,
						"updatedAt"=$2,
						"endTime"=$3,
						"earlyEndReason"=$4 where "id"=$5`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			room.updatedBy,
			room.updatedAt,
			room.endTime,
			room.earlyEndReason,
			room.id)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		errorString := fmt.Sprintf("could not update database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	s.onRoomChanges <- roomId
	log.Infof("deleted roomId '%s'", roomId)
	remarks := fmt.Sprintf("Ending roomId '%s' in %d minutes", roomId, timeLeftInSeconds/60)
	c.JSON(http.StatusOK, map[string]interface{}{"remarks": remarks})
}

func (s *RoomMgmtService) deleteUsersByUserId(c *gin.Context) {
	roomId := c.Param("roomid")
	userId := c.Param("userid")
	log.Infof("DELETE /rooms/%s/users/%s", roomId, userId)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	var deleteUser DeleteUser
	if err := c.ShouldBindJSON(&deleteUser); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}
	requestJSON, err := json.MarshalIndent(deleteUser, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	log.Infof("request:\n%s", string(requestJSON))

	if deleteUser.Requestor == nil {
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
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	var patchRoom PatchRoom
	if err := c.ShouldBindJSON(&patchRoom); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}
	err := s.patchRoomRequest(patchRoom, c)
	if err != nil {
		return
	}

	room, err := s.getEditableRoom(roomId, c)
	if err != nil {
		return
	}

	err = s.putAnnouncement(&room, patchRoom, c)
	if err != nil {
		return
	}

	updateStmt := `update "` + s.roomMgmtSchema + `"."room"
					set "updatedBy"=$1,
						"updatedAt"=$2 where "id"=$3`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			room.updatedBy,
			room.updatedAt,
			room.id)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		errorString := fmt.Sprintf("could not update database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	s.onRoomChanges <- roomId
	getRoom, err := s.queryGetRoom(roomId, c)
	if err != nil {
		return
	}
	log.Infof("put announcements to roomId '%s'", roomId)
	c.JSON(http.StatusOK, getRoom)
}

func (s *RoomMgmtService) deleteAnnouncementsByRoomId(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("DELETE /rooms/%s/announcements", roomId)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	var deleteAnnounce DeleteAnnouncement
	if err := c.ShouldBindJSON(&deleteAnnounce); err != nil {
		log.Warnf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "JSON:" + err.Error()})
		return
	}
	requestJSON, err := json.MarshalIndent(deleteAnnounce, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return
	}
	log.Infof("request:\n%s", string(requestJSON))
	if deleteAnnounce.Requestor == nil {
		log.Warnf(MISS_REQUESTOR)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_REQUESTOR})
		return
	}

	room, err := s.getEditableRoom(roomId, c)
	if err != nil {
		return
	}

	room.updatedBy = *deleteAnnounce.Requestor
	room.updatedAt = time.Now()
	if len(room.announcements) == 0 {
		warnString := fmt.Sprintf("roomId '%s' has no announcements in database", roomId)
		log.Warnf(warnString)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": warnString})
		return
	}

	isDeleteAll := false
	if deleteAnnounce.Id == nil {
		isDeleteAll = true
	} else if len(deleteAnnounce.Id) == 0 {
		isDeleteAll = true
	}
	if isDeleteAll {
		room.announcements = make([]Announcement, 0)
		deleteStmt := `delete from "` + s.roomMgmtSchema + `"."announcement" where "roomId"=$1`
		for retry := 0; retry < RETRY_COUNT; retry++ {
			_, err = s.postgresDB.Exec(deleteStmt, room.id)
			if err == nil {
				break
			}
			time.Sleep(RETRY_DELAY)
		}
		if err != nil {
			errorString := fmt.Sprintf("could not delete from database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
			return
		}
	} else {
		idMap := make(map[string]int)
		toDeleteIds := make([]int, 0)
		for _, announceId := range deleteAnnounce.Id {
			_, exist := idMap[announceId]
			if exist {
				continue
			}
			idMap[announceId] = 1
			isFound := false
			for id, announcements := range room.announcements {
				if announcements.id == announceId {
					toDeleteIds = append(toDeleteIds, id)
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
		deleteStmt := `delete from "` + s.roomMgmtSchema + `"."announcement" where "id"=$1`
		for key := range idMap {
			for retry := 0; retry < RETRY_COUNT; retry++ {
				_, err = s.postgresDB.Exec(deleteStmt, key)
				if err == nil {
					break
				}
				time.Sleep(RETRY_DELAY)
			}
			if err != nil {
				errorString := fmt.Sprintf("could not delete from database: %s", err)
				log.Errorf(errorString)
				c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
				return
			}
		}
		room.announcements = deleteSlices(room.announcements, toDeleteIds)
	}

	updateStmt := `update "` + s.roomMgmtSchema + `"."room"
					set "updatedBy"=$1,
						"updatedAt"=$2 where "id"=$3`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(updateStmt,
			room.updatedBy,
			room.updatedAt,
			room.id)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		errorString := fmt.Sprintf("could not update database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	s.onRoomChanges <- roomId
	getRoom, err := s.queryGetRoom(roomId, c)
	if err != nil {
		return
	}
	log.Infof("deleted announcements from roomId '%s'", roomId)
	c.JSON(http.StatusOK, getRoom)
}

// chat history retrieval
func (s *RoomMgmtService) getRoomsByRoomidChatinfo(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("GET /rooms/%s/chats", roomId)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	playback, chatPayloads, err := s.getChats(roomId, c)
	if err != nil {
		return
	}
	var getChats GetChats
	getChats.Id = playback.id
	getChats.Name = playback.name
	getChats.StartTime = playback.startTime
	getChats.RecordCount = len(chatPayloads)
	c.JSON(http.StatusOK, getChats)
}

func (s *RoomMgmtService) getRoomsByRoomidChats(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("GET /rooms/%s/chats", roomId)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	_, chatPayloads, err := s.getChats(roomId, c)
	if err != nil {
		return
	}

	sort.Sort(chatPayloads)
	var getChatRange GetChatRange
	getChatRange.Msg = chatPayloads
	for id := range getChatRange.Msg {
		if getChatRange.Msg[id].Base64File != nil {
			object, err := s.minioClient.GetObject(context.Background(),
				s.bucketName,
				roomId+*getChatRange.Msg[id].Base64File.FilePath,
				minio.GetObjectOptions{})
			if err != nil {
				errorString := fmt.Sprintf("could not download attachment: %s", err)
				log.Errorf(errorString)
				c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
				return
			}
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(object)
			if err != nil {
				errorString := fmt.Sprintf("could not process download: %s", err)
				log.Errorf(errorString)
				c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
				return
			}
			getChatRange.Msg[id].Base64File.Data = buf.String()
			getChatRange.Msg[id].Base64File.FilePath = nil
		}
	}

	c.JSON(http.StatusOK, getChatRange)
}

func (s *RoomMgmtService) getRoomsByRoomidChatRange(c *gin.Context) {
	roomId := c.Param("roomid")
	fromindex := c.Param("fromindex")
	toindex := c.Param("toindex")
	log.Infof("GET /rooms/%s/chats/%s/%s", roomId, fromindex, toindex)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	fromIndex, err := strconv.Atoi(fromindex)
	if err != nil {
		errorString := "integer parameter expected for 'fromindex'"
		log.Warnf(errorString)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
		return
	}
	toIndex, err := strconv.Atoi(toindex)
	if err != nil {
		errorString := "integer parameter expected for 'toIndex'"
		log.Warnf(errorString)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
		return
	}
	if fromIndex < 0 {
		fromIndex = 0
	}
	if toIndex < 0 {
		toIndex = 0
	}
	if fromIndex > toIndex {
		fromIndex, toIndex = toIndex, fromIndex
	}

	_, chatPayloads, err := s.getChats(roomId, c)
	if err != nil {
		return
	}
	count := len(chatPayloads)
	if fromIndex >= count {
		errorString := "requested index is out of range"
		log.Warnf(errorString)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
		return
	}
	if toIndex >= count {
		toIndex = count - 1
	}

	sort.Sort(chatPayloads)
	var getChatRange GetChatRange
	getChatRange.Msg = make([]ChatPayload, 0)
	for id := fromIndex; id <= toIndex; id++ {
		if chatPayloads[id].Base64File != nil {
			object, err := s.minioClient.GetObject(context.Background(),
				s.bucketName,
				roomId+*chatPayloads[id].Base64File.FilePath,
				minio.GetObjectOptions{})
			if err != nil {
				errorString := fmt.Sprintf("could not download attachment: %s", err)
				log.Errorf(errorString)
				c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
				return
			}
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(object)
			if err != nil {
				errorString := fmt.Sprintf("could not process download: %s", err)
				log.Errorf(errorString)
				c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
				return
			}
			chatPayloads[id].Base64File.Data = buf.String()
			chatPayloads[id].Base64File.FilePath = nil
		}
		getChatRange.Msg = append(getChatRange.Msg, chatPayloads[id])
	}

	c.JSON(http.StatusOK, getChatRange)
}

// playback
func (s *RoomMgmtService) postPlayback(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("POST /playback/rooms/%s", roomId)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	roomRecord, err := s.queryRoomRecord(roomId)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			log.Warnf(s.roomNotFound(roomId))
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": s.roomNotFound(roomId)})
		} else {
			errorString := fmt.Sprintf("could not query database: %s", err.Error())
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		}
		return
	}
	var durationInSeconds float64
	if roomRecord.endTime.After(time.Now()) {
		durationInSeconds = time.Since((roomRecord.startTime)).Seconds()
	} else {
		durationInSeconds = roomRecord.endTime.Sub(roomRecord.startTime).Seconds()
	}
	insertStmt := `insert into "` + s.roomMgmtSchema + `"."playback"(   "id",
																		"roomId",
																		"name",
																		"endpoint")
					values($1, $2, $3)`
	playbackId := s.getPlaybackUuid(true)
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = s.postgresDB.Exec(insertStmt, playbackId, roomId, roomRecord.name, "")
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), DUP_PK) {
			playbackId = s.getPlaybackUuid(true)
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		errorString := fmt.Sprintf("could not insert into database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}
	s.onPlaybackCreate <- playbackId
	c.JSON(http.StatusOK, map[string]interface{}{"id": playbackId, "durationInSeconds": durationInSeconds})
}

func (s *RoomMgmtService) postPlaybackPlay(c *gin.Context) {
	playbackId := c.Param("playbackid")
	log.Infof("POST /playback/%s/play", playbackId)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	endpoint, err := s.queryPlayback(playbackId, c)
	if err != nil {
		return
	}
	s.onPlaybackCtrl <- endpoint + "/play"
	c.Status(http.StatusOK)
}

func (s *RoomMgmtService) postPlaybackPlayfrom(c *gin.Context) {
	playbackId := c.Param("playbackid")
	secondsFromStart := c.Param("secondsfromstart")
	log.Infof("POST /playback/%s/play/%s", playbackId, secondsFromStart)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}
	_, err := strconv.Atoi(secondsFromStart)
	if err != nil {
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": "non-integer skip interval"})
		return
	}
	endpoint, err := s.queryPlayback(playbackId, c)
	if err != nil {
		return
	}
	s.onPlaybackCtrl <- endpoint + "/playFrom/" + secondsFromStart
	c.Status(http.StatusOK)
}

func (s *RoomMgmtService) postPlaybackPause(c *gin.Context) {
	playbackId := c.Param("playbackid")
	log.Infof("POST /playback/%s/pause", playbackId)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	endpoint, err := s.queryPlayback(playbackId, c)
	if err != nil {
		return
	}
	s.onPlaybackCtrl <- endpoint + "/pause"
	c.Status(http.StatusOK)
}

func (s *RoomMgmtService) deletePlayback(c *gin.Context) {
	playbackId := c.Param("playbackid")
	log.Infof("DELETE /playback/%s", playbackId)
	if s.timeReady == "" {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": NOT_READY})
		return
	}

	queryStmt := `SELECT "id" FROM "` + s.roomMgmtSchema + `"."playback" WHERE "id"=$1`
	var row *sql.Row
	for retry := 0; retry < RETRY_COUNT; retry++ {
		row = s.postgresDB.QueryRow(queryStmt, playbackId)
		if row.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if row.Err() != nil {
		errorString := fmt.Sprintf("could not query database: %s", row.Err())
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}
	var id string
	err := row.Scan(&id)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			errorString := s.roomNotFound(playbackId)
			log.Warnf(errorString)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
		} else {
			errorString := fmt.Sprintf("could not query database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		}
		return
	}
	s.onPlaybackDelete <- playbackId
	c.Status(http.StatusOK)
}

func (s *RoomMgmtService) getPlaybackUuid(isPlayback bool) string {
	id := uuid.NewString()
	if isPlayback {
		id = s.playbackIdPrefix + id[len(s.playbackIdPrefix):]
	} else {
		if id[:len(s.playbackIdPrefix)] == s.playbackIdPrefix {
			return s.getPlaybackUuid(isPlayback)
		}
	}
	return id
}

func (s *RoomMgmtService) roomNotFound(roomId string) string {
	return "RoomId '" + roomId + "' not found in database"
}

func (s *RoomMgmtService) roomNotStarted(roomId string) string {
	return "RoomId '" + roomId + "' session not started"
}

func (s *RoomMgmtService) roomHasEnded(roomId string) string {
	return "RoomId '" + roomId + "' session has ended"
}

func (s *RoomMgmtService) getChats(roomId string, c *gin.Context) (RoomRecord, ChatPayloads, error) {
	roomRecord, err := s.queryRoomRecord(roomId)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			log.Warnf(s.roomNotFound(roomId))
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": s.roomNotFound(roomId)})
		} else {
			errorString := fmt.Sprintf("could not query database: %s", err.Error())
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		}
		return RoomRecord{}, ChatPayloads{}, err
	}

	chats := make(ChatPayloads, 0)
	var chatrows *sql.Rows
	queryStmt := `SELECT "userId",
						 "userName",
						 "text",
						 "timestamp"
					FROM "` + s.roomRecordSchema + `"."chatMessage" WHERE "roomId"=$1`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		chatrows, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		errorString := fmt.Sprintf("could not query database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return RoomRecord{}, ChatPayloads{}, err
	}
	defer chatrows.Close()
	for chatrows.Next() {
		var chat ChatPayload
		var text string
		err := chatrows.Scan(&chat.Uid,
			&chat.Name,
			&text,
			&chat.Timestamp)
		if err != nil {
			errorString := fmt.Sprintf("could not query database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
			return RoomRecord{}, ChatPayloads{}, err
		}
		chat.Text = &text
		chats = append(chats, chat)
	}
	sort.Sort(chats)

	attachments := make(ChatPayloads, 0)
	var attachmentrows *sql.Rows
	queryStmt = `SELECT "userId",
						"userName",
						"fileName",
						"fileSize",
						"filePath",
						"timestamp"
					FROM "` + s.roomRecordSchema + `"."chatAttachment" WHERE "roomId"=$1`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		attachmentrows, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		errorString := fmt.Sprintf("could not query database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return RoomRecord{}, ChatPayloads{}, err
	}
	defer attachmentrows.Close()
	for attachmentrows.Next() {
		var attachment ChatPayload
		var fileinfo Attachment
		var filePath string
		err := chatrows.Scan(&attachment.Uid,
			&attachment.Name,
			&fileinfo.Name,
			&fileinfo.Size,
			&filePath,
			&attachment.Timestamp)
		if err != nil {
			errorString := fmt.Sprintf("could not query database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
			return RoomRecord{}, ChatPayloads{}, err
		}
		fileinfo.FilePath = &filePath
		attachment.Base64File = &fileinfo
		attachments = append(attachments, attachment)
	}
	sort.Sort(attachments)

	chats = append(chats, attachments...)
	return roomRecord, chats, nil
}

func (s *RoomMgmtService) getEditableRoom(roomId string, c *gin.Context) (Room, error) {
	room, err := s.queryRoom(roomId)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			log.Warnf(s.roomNotFound(roomId))
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": s.roomNotFound(roomId)})
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

func (s *RoomMgmtService) patchRoomRequest(patchRoom PatchRoom, c *gin.Context) error {
	requestJSON, err := json.MarshalIndent(patchRoom, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": err.Error()})
		return err
	}
	log.Infof("request:\n%s", string(requestJSON))
	if patchRoom.Requestor == nil {
		log.Warnf(MISS_REQUESTOR)
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": MISS_REQUESTOR})
		return errors.New(MISS_REQUESTOR)
	}
	return nil
}

func (s *RoomMgmtService) patchRoom(room *Room, patchRoom PatchRoom, c *gin.Context) error {
	room.updatedBy = *patchRoom.Requestor
	if patchRoom.Name != nil {
		room.name = *patchRoom.Name
	}
	if patchRoom.StartTime != nil {
		if room.status == ROOM_STARTED && room.startTime != *patchRoom.StartTime {
			errorString := fmt.Sprintf("RoomId '%s' has already started", room.id)
			log.Warnf(errorString)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
			return errors.New(errorString)
		}
		room.startTime = *patchRoom.StartTime
	}
	if patchRoom.EndTime != nil {
		if room.endTime.Before(room.startTime) {
			errorString := "endtime is before starttime"
			log.Warnf(errorString)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
			return errors.New(errorString)
		}
		room.endTime = *patchRoom.EndTime
	}
	if len(patchRoom.AllowedUserId) == 0 {
		room.allowedUserId = make(pq.StringArray, 0)
	}
	for _, patchuser := range patchRoom.AllowedUserId {
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

	updateStmt := `update "` + s.roomMgmtSchema + `"."room"
					set "updatedBy"=$1,
						"updatedAt"=$2,
						"name"=$3,
						"startTime"=$4,
						"endTime"=$5,
						"allowedUserId"=$6 where "id"=$7`
	var err error
	for retry := 0; retry < RETRY_COUNT; retry++ {
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
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		errorString := fmt.Sprintf("could not update database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return err
	}

	idMap := make(map[string]int)
	for _, patch := range patchRoom.Announcements {
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

				updateStmt := `update "` + s.roomMgmtSchema + `"."announcement"
								set "message"=$1,
									"relativeFrom"=$2,
									"relativeTimeInSeconds"=$3,
									"userId"=$4,
									"updatedBy"=$5,
									"updatedAt"=$6 where "id"=$7`
				for retry := 0; retry < RETRY_COUNT; retry++ {
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
					time.Sleep(RETRY_DELAY)
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

		insertStmt := `insert into "` + s.roomMgmtSchema + `"."announcement"(   "id", 
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
		for retry := 0; retry < RETRY_COUNT; retry++ {
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
			time.Sleep(RETRY_DELAY)
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

func (s *RoomMgmtService) queryGetRoom(roomId string, c *gin.Context) (GetRoom, error) {
	queryStmt := `SELECT    "id",
							"name",
							"status",
							"startTime",
							"endTime",
							"allowedUserId",
							"earlyEndReason",
							"createdBy",
							"createdAt",
							"updatedBy",
							"updatedAt"
					FROM "` + s.roomMgmtSchema + `"."room" WHERE "id"=$1`
	var row *sql.Row
	for retry := 0; retry < RETRY_COUNT; retry++ {
		row = s.postgresDB.QueryRow(queryStmt, roomId)
		if row.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if row.Err() != nil {
		errorString := fmt.Sprintf("could not query database: %s", row.Err())
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return GetRoom{}, row.Err()
	}
	var getRoom GetRoom
	err := row.Scan(&getRoom.Id,
		&getRoom.Name,
		&getRoom.Status,
		&getRoom.StartTime,
		&getRoom.EndTime,
		&getRoom.AllowedUserId,
		&getRoom.EarlyEndReason,
		&getRoom.CreatedBy,
		&getRoom.CreatedAt,
		&getRoom.UpdatedBy,
		&getRoom.UpdatedAt)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			errorString := s.roomNotFound(roomId)
			log.Warnf(errorString)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
		} else {
			errorString := fmt.Sprintf("could not query database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		}
		return GetRoom{}, err
	}

	getRoom.Announcements = make([]GetAnnouncement, 0)
	queryStmt = `SELECT "id",
						"message",
						"relativeFrom",
						"relativeTimeInSeconds",
						"userId",
						"createdBy",
						"createdAt",
						"updatedBy",
						"updatedAt"
					FROM "` + s.roomMgmtSchema + `"."announcement" WHERE "roomId"=$1`
	var announcements *sql.Rows
	for retry := 0; retry < RETRY_COUNT; retry++ {
		announcements, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		errorString := fmt.Sprintf("could not query database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return GetRoom{}, err
	}
	defer announcements.Close()
	for announcements.Next() {
		var getAnnouncement GetAnnouncement
		err = announcements.Scan(&getAnnouncement.Id,
			&getAnnouncement.Message,
			&getAnnouncement.RelativeFrom,
			&getAnnouncement.RelativeTimeInSeconds,
			&getAnnouncement.UserId,
			&getAnnouncement.CreatedBy,
			&getAnnouncement.CreatedAt,
			&getAnnouncement.UpdatedBy,
			&getAnnouncement.UpdatedAt)
		if err != nil {
			errorString := fmt.Sprintf("could not query database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
			return GetRoom{}, err
		}
		getRoom.Announcements = append(getRoom.Announcements, getAnnouncement)
	}

	if getRoom.Status == ROOM_STARTED {
		getRoom.Users = append(getRoom.Users, s.getPeers(getRoom.Id)...)
	}
	return getRoom, nil
}

func (s *RoomMgmtService) queryRoom(roomId string) (Room, error) {
	queryStmt := `SELECT    "id",
							"name",
							"status",
							"startTime",
							"endTime",
							"allowedUserId",
							"earlyEndReason",
							"createdBy",
							"createdAt",
							"updatedBy",
							"updatedAt"
					FROM "` + s.roomMgmtSchema + `"."room" WHERE "id"=$1`
	var rows *sql.Row
	for retry := 0; retry < RETRY_COUNT; retry++ {
		rows = s.postgresDB.QueryRow(queryStmt, roomId)
		if rows.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if rows.Err() != nil {
		errorString := fmt.Sprintf("could not query database: %s", rows.Err())
		log.Errorf(errorString)
		return Room{}, rows.Err()
	}
	var room Room
	err := rows.Scan(&room.id,
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
						"updatedAt"
					FROM "` + s.roomMgmtSchema + `"."announcement" WHERE "roomId"=$1`
	var announcements *sql.Rows
	for retry := 0; retry < RETRY_COUNT; retry++ {
		announcements, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
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

func (s *RoomMgmtService) queryRoomRecord(roomId string) (RoomRecord, error) {
	queryStmt := `SELECT    "id",
							"name",
							"startTime",
							"endTime"
					FROM "` + s.roomRecordSchema + `"."room" WHERE "id"=$1`
	var row *sql.Row
	for retry := 0; retry < RETRY_COUNT; retry++ {
		row = s.postgresDB.QueryRow(queryStmt, roomId)
		if row.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if row.Err() != nil {
		errorString := fmt.Sprintf("could not query database: %s", row.Err())
		log.Errorf(errorString)
		return RoomRecord{}, row.Err()
	}
	var roomRecord RoomRecord
	err := row.Scan(&roomRecord.id,
		&roomRecord.name,
		&roomRecord.startTime,
		&roomRecord.endTime)
	if err != nil {
		return RoomRecord{}, err
	}
	return roomRecord, nil
}

func (s *RoomMgmtService) queryPlayback(playbackId string, c *gin.Context) (string, error) {
	queryStmt := `SELECT "endpoint" FROM "` + s.roomMgmtSchema + `"."playback" WHERE "id"=$1`
	var row *sql.Row
	for retry := 0; retry < RETRY_COUNT; retry++ {
		row = s.postgresDB.QueryRow(queryStmt, playbackId)
		if row.Err() == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if row.Err() != nil {
		errorString := fmt.Sprintf("could not query database: %s", row.Err())
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return "", row.Err()
	}
	var endpoint string
	err := row.Scan(&endpoint)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			errorString := s.roomNotFound(playbackId)
			log.Warnf(errorString)
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": errorString})
		} else {
			errorString := fmt.Sprintf("could not query database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		}
		return "", err
	}
	if endpoint == "" {
		errorString := "playback is not yet ready"
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return "", err
	}
	return endpoint, nil
}

func (s *RoomMgmtService) putAnnouncement(room *Room, patchRoom PatchRoom, c *gin.Context) error {
	room.updatedBy = *patchRoom.Requestor
	room.updatedAt = time.Now()

	idMap := make(map[string]int)
	for _, patch := range patchRoom.Announcements {
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
				updateStmt := `update "` + s.roomMgmtSchema + `"."announcement"
								set "message"=$1,
									"relativeFrom"=$2,
									"relativeTimeInSeconds"=$3,
									"userId"=$4,
									"updatedBy"=$5,
									"updatedAt"=$6 where "id"=$7`
				var err error
				for retry := 0; retry < RETRY_COUNT; retry++ {
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
					time.Sleep(RETRY_DELAY)
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
		insertStmt := `insert into "` + s.roomMgmtSchema + `"."announcement"(   "id", 
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
		for retry := 0; retry < RETRY_COUNT; retry++ {
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
			time.Sleep(RETRY_DELAY)
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

func (s *RoomMgmtService) getPeers(roomId string) []User {
	sdkConnector, roomService, err := s.getRoomService(s.conf.Signal.Addr)
	if err != nil {
		return nil
	}
	defer s.closeRoomService(sdkConnector, roomService)

	peers := roomService.GetPeers(roomId)
	log.Infof("%v", peers)

	users := make([]User, 0)
	for _, peer := range peers {
		if len(peer.Uid) >= s.lenSystemUid {
			if peer.Uid[:s.lenSystemUid] == s.systemUid {
				continue
			}
		}
		var user User
		user.UserId = peer.Uid
		user.UserName = peer.DisplayName
		users = append(users, user)
	}
	return users
}

func (s *RoomMgmtService) kickUser(roomId, userId string) (error, error) {
	sdkConnector, roomService, err := s.getRoomService(s.conf.Signal.Addr)
	if err != nil {
		return nil, err
	}
	defer s.closeRoomService(sdkConnector, roomService)

	peerinfo := roomService.GetPeers(roomId)
	for _, peer := range peerinfo {
		if userId == peer.Uid {
			err := roomService.RemovePeer(roomId, peer.Uid)
			if err != nil {
				output := fmt.Sprintf("Error kicking '%s' from room '%s' : %v", userId, roomId, err)
				log.Errorf(output)
				return nil, errors.New(output)
			}
			log.Infof("Kicked '%s' from room '%s'", userId, roomId)
			return nil, nil
		}
	}
	output := fmt.Sprintf("userId '%s' not found in roomId '%s'", userId, roomId)
	log.Warnf(output)
	return errors.New(output), nil
}

func (s *RoomMgmtService) getRoomService(signalAddr string) (*sdk.Connector, *sdk.Room, error) {
	log.Infof("--- Connecting to Room Signal ---")
	log.Infof("attempt gRPC connection to %s", signalAddr)
	sdkConnector := sdk.NewConnector(signalAddr)
	if sdkConnector == nil {
		log.Errorf("connection to %s fail", signalAddr)
		return nil, nil, errors.New("")
	}
	roomService := sdk.NewRoom(sdkConnector)
	return sdkConnector, roomService, nil
}

func (s *RoomMgmtService) closeRoomService(sdkConnector *sdk.Connector, roomService *sdk.Room) {
	if roomService != nil {
		roomService.Leave(s.systemUid, s.systemUid)
		roomService.Close()
		roomService = nil
	}
	if sdkConnector != nil {
		sdkConnector = nil
	}
}

type RoomMgmtService struct {
	conf Config

	onRoomChanges    chan string
	onPlaybackCreate chan string
	onPlaybackDelete chan string
	onPlaybackCtrl   chan string

	timeLive  string
	timeReady string

	postgresDB       *sql.DB
	roomMgmtSchema   string
	roomRecordSchema string

	minioClient *minio.Client
	bucketName  string

	systemUid        string
	lenSystemUid     int
	playbackIdPrefix string
}

func NewRoomMgmtService(config Config) *RoomMgmtService {
	timeLive := time.Now().Format(time.RFC3339)
	err := testRedisConnection(config)
	if err != nil {
		log.Errorf("redisDB connection error")
		os.Exit(1)
	}
	minioClient := getMinioClient(config)
	postgresDB := getPostgresDB(config)

	s := &RoomMgmtService{
		conf: config,

		onRoomChanges:    make(chan string, 2048),
		onPlaybackCreate: make(chan string, 2048),
		onPlaybackDelete: make(chan string, 2048),
		onPlaybackCtrl:   make(chan string, 2048),

		timeLive:  timeLive,
		timeReady: time.Now().Format(time.RFC3339),

		postgresDB:       postgresDB,
		roomMgmtSchema:   config.Postgres.RoomMgmtSchema,
		roomRecordSchema: config.Postgres.RoomRecordSchema,

		minioClient: minioClient,
		bucketName:  config.Minio.BucketName,

		systemUid:        config.RoomMgmt.SystemUid,
		lenSystemUid:     len(config.RoomMgmt.SystemUid),
		playbackIdPrefix: config.RoomMgmt.PlaybackIdPrefix,
	}

	go s.start()
	go s.checkForRemoteCtrl()

	return s
}

func testRedisConnection(config Config) error {
	redisDb := db.NewRedis(config.Redis)
	defer redisDb.Close()
	key, value := uuid.NewString(), "value"
	err := redisDb.Set(key, value, time.Second)
	if err != nil {
		return err
	}
	res := redisDb.Get(key)
	if res != value {
		return errors.New("error")
	}
	err = redisDb.Del(key)
	if err != nil {
		return err
	}
	res = redisDb.Get(key)
	if res != "" {
		return errors.New("error")
	}
	return nil
}

func getMinioClient(config Config) *minio.Client {
	log.Infof("--- Connecting to MinIO ---")
	minioClient, err := minio.New(config.Minio.Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(config.Minio.AccessKeyID,
			config.Minio.SecretAccessKey, ""),
		Secure: config.Minio.UseSSL,
	})
	if err != nil {
		log.Errorf("Unable to connect to filestore: %v\n", err)
		os.Exit(1)
	}
	err = minioClient.MakeBucket(context.Background(),
		config.Minio.BucketName,
		minio.MakeBucketOptions{})
	if err != nil {
		exists, errBucketExists := minioClient.BucketExists(context.Background(),
			config.Minio.BucketName)
		if errBucketExists != nil || !exists {
			log.Errorf("Unable to create bucket: %v\n", err)
			os.Exit(1)
		}
	}

	return minioClient
}

func getPostgresDB(config Config) *sql.DB {
	log.Infof("--- Connecting to PostgreSql ---")
	addrSplit := strings.Split(config.Postgres.Addr, ":")
	port, err := strconv.Atoi(addrSplit[1])
	if err != nil {
		log.Errorf("invalid port number: %s\n", addrSplit[1])
		os.Exit(1)
	}
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		addrSplit[0],
		port,
		config.Postgres.User,
		config.Postgres.Password,
		config.Postgres.Database)
	var postgresDB *sql.DB
	// postgresDB.Open
	for retry := 0; retry < RETRY_COUNT; retry++ {
		postgresDB, err = sql.Open("postgres", psqlconn)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	// postgresDB.Ping
	for retry := 0; retry < RETRY_COUNT; retry++ {
		err = postgresDB.Ping()
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to ping database: %v\n", err)
		os.Exit(1)
	}
	// create RoomMgmtSchema schema
	createStmt := `CREATE SCHEMA IF NOT EXISTS "` + config.Postgres.RoomMgmtSchema + `"`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "room"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomMgmtSchema + `"."room"(
					"id"             UUID PRIMARY KEY,
					"name"           TEXT NOT NULL,
					"status"         TEXT NOT NULL,
					"startTime"      TIMESTAMP NOT NULL,
					"endTime"        TIMESTAMP NOT NULL,
					"allowedUserId"  TEXT ARRAY NOT NULL,
					"earlyEndReason" TEXT NOT NULL,
					"createdBy"      TEXT NOT NULL,
					"createdAt"      TIMESTAMP NOT NULL,
					"updatedBy"      TEXT NOT NULL,
					"updatedAt"      TIMESTAMP NOT NULL)`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "announcement"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomMgmtSchema + `"."announcement"(
					"id"                    UUID PRIMARY KEY,
					"roomId"                UUID NOT NULL,
					"status"                TEXT NOT NULL,
					"message"               TEXT NOT NULL,
					"relativeFrom"          TEXT NOT NULL,
					"relativeTimeInSeconds" INT NOT NULL,
					"userId"                TEXT ARRAY NOT NULL,
					"createdAt"             TIMESTAMP NOT NULL,
					"createdBy"             TEXT NOT NULL,
					"updatedAt"             TIMESTAMP NOT NULL,
					"updatedBy"             TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}

	// create RoomRecordSchema schema
	createStmt = `CREATE SCHEMA IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "room"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."room"(
					"id"        UUID PRIMARY KEY,
					"name"      TEXT NOT NULL,
					"startTime" TIMESTAMP NOT NULL,
					"endTime"   TIMESTAMP NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("id") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "chatMessage"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."chatMessage"(
					"id"           UUID PRIMARY KEY,
					"roomId"       UUID NOT NULL,
					"timestamp"    TIMESTAMP NOT NULL,
					"userId"       TEXT NOT NULL,
					"userName"     TEXT NOT NULL,
					"text"         TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "chatAttachment"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."chatAttachment"(
					"id"           UUID PRIMARY KEY,
					"roomId"       UUID NOT NULL,
					"timestamp"    TIMESTAMP NOT NULL,
					"userId"       TEXT NOT NULL,
					"userName"     TEXT NOT NULL,
					"fileName"     TEXT NOT NULL,
					"fileSize"     INT NOT NULL,
					"filePath"     TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "playback"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomMgmtSchema + `"."playback"(
					"id"       UUID PRIMARY KEY,
					"roomId"   UUID NOT NULL,
					"name"     TEXT NOT NULL,
					"endpoint" TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."room"("id"))`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}

	return postgresDB
}

func (s *RoomMgmtService) checkForRemoteCtrl() {
	for {
		select {
		case roomId := <-s.onRoomChanges:
			log.Infof("changes to roomid '%s'", roomId)
			requestURL := s.conf.RoomMgmtSentry.Url + "/rooms/" + roomId
			go s.httpPost(requestURL)
		case playbackId := <-s.onPlaybackCreate:
			log.Infof("playbackid '%s' created", playbackId)
			requestURL := s.conf.RoomMgmtSentry.Url + "/playback/" + playbackId
			go s.httpPost(requestURL)
		case playbackId := <-s.onPlaybackDelete:
			log.Infof("playbackid '%s' deleted", playbackId)
			requestURL := s.conf.RoomMgmtSentry.Url + "/delete/playback/" + playbackId
			go s.httpPost(requestURL)
		case ctrlUrl := <-s.onPlaybackCtrl:
			log.Infof("playback cmd: %s", ctrlUrl)
			go s.httpPost(ctrlUrl)
		}
	}
}

func (s *RoomMgmtService) httpPost(requestURL string) {
	request, err := http.NewRequest(http.MethodPost, requestURL, nil)
	if err != nil {
		log.Errorf("error sending http.POST to room-sentry: %v", err)
		return
	}
	for retry := 0; retry < RETRY_COUNT; retry++ {
		err = s.httpClient(request)
		if err == nil {
			break
		}
		time.Sleep(RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("error sending http.POST to room-sentry: %v", err)
	}
}

func (s *RoomMgmtService) httpClient(request *http.Request) error {
	var response *http.Response
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return err
	} else if response.StatusCode != http.StatusOK {
		return fmt.Errorf("response.StatusCode=%v", response.StatusCode)
	}
	return nil
}

func (s *RoomMgmtService) start() {
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
	// chat history retrieval
	router.GET("/rooms/:roomid/chatinfo", s.getRoomsByRoomidChatinfo)
	router.GET("/rooms/:roomid/chats", s.getRoomsByRoomidChats)
	router.GET("/rooms/:roomid/chats/:fromindex/:toindex", s.getRoomsByRoomidChatRange)
	// playback
	router.POST("/playback/rooms/:roomid", s.postPlayback)
	router.POST("/playback/:playbackid/play", s.postPlaybackPlay)
	router.POST("/playback/:playbackid/play/:secondsfromstart", s.postPlaybackPlayfrom)
	router.POST("/playback/:playbackid/pause", s.postPlaybackPause)
	router.DELETE("/playback/:playbackid", s.deletePlayback)

	log.Infof("HTTP service starting at %s", s.conf.RoomMgmt.Addr)
	log.Errorf("%s", router.Run(s.conf.RoomMgmt.Addr))
	os.Exit(1)
}
