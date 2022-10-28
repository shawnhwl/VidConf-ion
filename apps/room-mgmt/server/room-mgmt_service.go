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

	RETRY_COUNT  int    = 3
	DUP_PK       string = "duplicate key value violates unique constraint"
	NOT_FOUND_PK string = "no rows in result set"

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

type GetChats struct {
	Id        string    `json:"id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	StartTime time.Time `json:"startTime"`
	ChatCount int       `json:"chatCount"`
}

type GetChatRange struct {
	Msg ChatPayloads `json:"msg"`
}

type ChatPayloads []ChatPayload

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

type RoomRecord struct {
	id         string
	startTime  time.Time
	endTime    time.Time
	folderPath string
}

func (s *RoomMgmtService) getLiveness(c *gin.Context) {
	log.Infof("GET /liveness")
	c.String(http.StatusOK, "Live since %s", s.timeLive)
}

func (s *RoomMgmtService) getReadiness(c *gin.Context) {
	log.Infof("GET /readiness")
	if s.timeReady == "" {
		c.String(http.StatusInternalServerError, "Not Ready yet")
		return
	}
	c.String(http.StatusOK, "Ready since %s", s.timeReady)
}

func (s *RoomMgmtService) postRooms(c *gin.Context) {
	log.Infof("POST /rooms")

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
	roomId := uuid.NewString()
	room.id = roomId
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

	err = s.patchRoom(&room, patchRoom, c)
	if err != nil {
		deleteStmt := `delete from "` + s.roomMgmtSchema + `"."room" where "id"=$1`
		for retry := 0; retry < RETRY_COUNT; retry++ {
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
	queryStmt := `SELECT "id" FROM "` + s.roomMgmtSchema + `"."room"`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		rows, err = s.postgresDB.Query(queryStmt)
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

	getRoom, err := s.queryGetRoom(roomId, c)
	if err != nil {
		return
	}

	c.JSON(http.StatusOK, getRoom)
}

func (s *RoomMgmtService) patchRoomsByRoomid(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("PATCH /rooms/%s", roomId)

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

	s.onChanges <- roomId
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
	}
	if err != nil {
		errorString := fmt.Sprintf("could not update database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	s.onChanges <- roomId
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
	}
	if err != nil {
		errorString := fmt.Sprintf("could not update database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return
	}

	s.onChanges <- roomId
	getRoom, err := s.queryGetRoom(roomId, c)
	if err != nil {
		return
	}
	log.Infof("deleted announcements from roomId '%s'", roomId)
	c.JSON(http.StatusOK, getRoom)
}

func (s *RoomMgmtService) getRoomsByRoomidChats(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("GET /rooms/%s/chats", roomId)

	getChats, chatPayloads, _, err := s.getChats(roomId, c)
	if err != nil {
		return
	}
	getChats.ChatCount = len(chatPayloads)
	c.JSON(http.StatusOK, getChats)
}

func (s *RoomMgmtService) getRoomsByRoomidChatRange(c *gin.Context) {
	roomId := c.Param("roomid")
	fromindex := c.Param("fromindex")
	toindex := c.Param("toindex")
	log.Infof("GET /rooms/%s/chats/%s/%s", roomId, fromindex, toindex)
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

	_, chatPayloads, folderPath, err := s.getChats(roomId, c)
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

	folderPathSlice := strings.Split(folderPath, "/")
	folderPath = "/" + strings.Join(folderPathSlice[1:], "/")
	log.Infof("folderPath: %s", folderPath)

	sort.Sort(chatPayloads)

	var getChatRange GetChatRange
	getChatRange.Msg = make([]ChatPayload, 0)
	for id := fromIndex; id <= toIndex; id++ {
		if chatPayloads[id].Base64File != nil {
			object, err := s.minioClient.GetObject(context.Background(),
				s.bucketName,
				*chatPayloads[id].Base64File.FilePath,
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

func (p ChatPayloads) Len() int {
	return len(p)
}

func (p ChatPayloads) Less(i, j int) bool {
	return p[i].Timestamp.Before(p[j].Timestamp)
}

func (p ChatPayloads) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
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

func (s *RoomMgmtService) getChats(roomId string, c *gin.Context) (GetChats, ChatPayloads, string, error) {
	getChats, err := s.queryChats(roomId)
	if err != nil {
		if strings.Contains(err.Error(), NOT_FOUND_PK) {
			log.Warnf(s.roomNotFound(roomId))
			c.JSON(http.StatusBadRequest, map[string]interface{}{"error": s.roomNotFound(roomId)})
		} else {
			errorString := fmt.Sprintf("could not query database: %s", err.Error())
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		}
		return GetChats{}, ChatPayloads{}, "", err
	}
	if getChats.Status == ROOM_BOOKED {
		log.Warnf(s.roomNotStarted(roomId))
		c.JSON(http.StatusBadRequest, map[string]interface{}{"error": s.roomNotStarted(roomId)})
		return GetChats{}, ChatPayloads{}, "", errors.New(s.roomNotStarted(roomId))
	}

	roomRecords := make([]RoomRecord, 0)
	var rows *sql.Rows
	queryStmt := `SELECT    "id",
							"startTime",
							"endTime",
							"folderPath"
					FROM "` + s.roomRecordSchema + `"."roomRecord" WHERE "roomId"=$1`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		rows, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
	}
	if err != nil {
		errorString := fmt.Sprintf("could not query database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return GetChats{}, ChatPayloads{}, "", err
	}
	defer rows.Close()

	for rows.Next() {
		var roomRecord RoomRecord
		err := rows.Scan(&roomRecord.id,
			&roomRecord.startTime,
			&roomRecord.endTime,
			&roomRecord.folderPath)
		if err != nil {
			errorString := fmt.Sprintf("could not query database: %s", err)
			log.Errorf(errorString)
			c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
			return GetChats{}, ChatPayloads{}, "", err
		}
		roomRecords = append(roomRecords, roomRecord)
	}

	if len(roomRecords) == 0 {
		errorString := "Room Recorder is not enabled"
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return GetChats{}, ChatPayloads{}, "", errors.New(errorString)
	}
	folderPath := roomRecords[0].folderPath

	chats := make(ChatPayloads, 0)
	var chatrows *sql.Rows
	queryStmt = `SELECT "userId",
						"userName",
						"text",
						"timestamp"
					FROM "` + s.roomRecordSchema + `"."chatMessage" WHERE "roomId"=$1`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		chatrows, err = s.postgresDB.Query(queryStmt, roomId)
		if err == nil {
			break
		}
	}
	if err != nil {
		errorString := fmt.Sprintf("could not query database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return GetChats{}, ChatPayloads{}, "", err
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
			return GetChats{}, ChatPayloads{}, "", err
		}
		chat.Text = &text
		chats = append(chats, chat)
	}
	log.Infof("chats count:%d", len(chats))
	sort.Sort(chats)
	log.Infof("chats count:%d", len(chats))
	toDeleteIds := make([]int, 0)
	for id := 0; id < len(chats); id++ {
		for scanId := id + 1; scanId < len(chats); scanId++ {
			if chats[id].Timestamp != chats[scanId].Timestamp {
				continue
			}
			if chats[id].Uid != chats[scanId].Uid {
				continue
			}
			if chats[id].Name != chats[scanId].Name {
				continue
			}
			if *chats[id].Text != *chats[scanId].Text {
				continue
			}
			toDeleteIds = append(toDeleteIds, id)
			break
		}
	}
	log.Infof("toDeleteIds count:%d", len(toDeleteIds))
	chats = deleteSlices(chats, toDeleteIds)
	log.Infof("chats count:%d", len(chats))

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
	}
	if err != nil {
		errorString := fmt.Sprintf("could not query database: %s", err)
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return GetChats{}, ChatPayloads{}, "", err
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
			return GetChats{}, ChatPayloads{}, "", err
		}
		fileinfo.FilePath = &filePath
		attachment.Base64File = &fileinfo
		attachments = append(attachments, attachment)
	}
	log.Infof("attachments count:%d", len(attachments))
	sort.Sort(attachments)
	toDeleteIds = make([]int, 0)
	for id := 0; id < len(attachments); id++ {
		for scanId := id + 1; scanId < len(attachments); scanId++ {
			if attachments[id].Timestamp != attachments[scanId].Timestamp {
				continue
			}
			if attachments[id].Uid != attachments[scanId].Uid {
				continue
			}
			if attachments[id].Name != attachments[scanId].Name {
				continue
			}
			if attachments[id].Base64File.Name != attachments[scanId].Base64File.Name {
				continue
			}
			if attachments[id].Base64File.Size != attachments[scanId].Base64File.Size {
				continue
			}
			toDeleteIds = append(toDeleteIds, id)
			break
		}
	}
	attachments = deleteSlices(attachments, toDeleteIds)
	log.Infof("attachments count:%d", len(attachments))

	chats = append(chats, attachments...)
	return getChats, chats, folderPath, nil
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
	var rows *sql.Row
	for retry := 0; retry < RETRY_COUNT; retry++ {
		rows = s.postgresDB.QueryRow(queryStmt, roomId)
		if rows.Err() == nil {
			break
		}
	}
	if rows.Err() != nil {
		errorString := fmt.Sprintf("could not query database: %s", rows.Err())
		log.Errorf(errorString)
		c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": errorString})
		return GetRoom{}, rows.Err()
	}
	var getRoom GetRoom
	err := rows.Scan(&getRoom.Id,
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

func (s *RoomMgmtService) queryChats(roomId string) (GetChats, error) {
	queryStmt := `SELECT    "id",
							"name",
							"status",
							"startTime"
					FROM "` + s.roomMgmtSchema + `"."room" WHERE "id"=$1`
	var row *sql.Row
	for retry := 0; retry < RETRY_COUNT; retry++ {
		row = s.postgresDB.QueryRow(queryStmt, roomId)
		if row.Err() == nil {
			break
		}
	}
	if row.Err() != nil {
		errorString := fmt.Sprintf("could not query database: %s", row.Err())
		log.Errorf(errorString)
		return GetChats{}, row.Err()
	}
	var getChats GetChats
	err := row.Scan(&getChats.Id,
		&getChats.Name,
		&getChats.Status,
		&getChats.StartTime)
	if err != nil {
		return GetChats{}, err
	}
	return getChats, nil
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
	peers := s.roomService.GetPeers(roomId)
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
	peerinfo := s.roomService.GetPeers(roomId)
	for _, peer := range peerinfo {
		if userId == peer.Uid {
			err := s.roomService.RemovePeer(roomId, peer.Uid)
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

type RoomMgmtService struct {
	conf           Config
	joinRoomCh     chan bool
	isSdkConnected bool

	timeLive  string
	timeReady string

	sdkConnector *sdk.Connector
	roomService  *sdk.Room

	postgresDB       *sql.DB
	roomMgmtSchema   string
	roomRecordSchema string

	minioClient *minio.Client
	bucketName  string

	onChanges    chan string
	systemUid    string
	lenSystemUid int
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
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "roomRecord"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."roomRecord"(
					"id"         UUID PRIMARY KEY,
					"roomId"     UUID NOT NULL,
					"startTime"  TIMESTAMP NOT NULL,
					"endTime"    TIMESTAMP NOT NULL,
					"folderPath" TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "chatMessage"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."chatMessage"(
					"id"           UUID PRIMARY KEY,
					"roomRecordId" UUID NOT NULL,
					"roomId"       UUID NOT NULL,
					"timeElapsed"  BIGINT NOT NULL,
					"userId"       TEXT NOT NULL,
					"userName"     TEXT NOT NULL,
					"text"         TEXT NOT NULL,
					"timestamp"    TIMESTAMP NOT NULL,
					CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."roomRecord"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}
	// create table "chatAttachment"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.Postgres.RoomRecordSchema + `"."chatAttachment"(
					"id"           UUID PRIMARY KEY,
					"roomRecordId" UUID NOT NULL,
					"roomId"       UUID NOT NULL,
					"timeElapsed"  BIGINT NOT NULL,
					"userId"       TEXT NOT NULL,
					"userName"     TEXT NOT NULL,
					"fileName"     TEXT NOT NULL,
					"fileSize"     INT NOT NULL,
					"filePath"     TEXT NOT NULL,
					"timestamp"    TIMESTAMP NOT NULL,
					CONSTRAINT fk_roomRecord FOREIGN KEY("roomRecordId") REFERENCES "` + config.Postgres.RoomRecordSchema + `"."roomRecord"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.Postgres.RoomMgmtSchema + `"."room"("id"))`
	for retry := 0; retry < RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %v\n", err)
		os.Exit(1)
	}

	return postgresDB
}

func NewRoomMgmtService(config Config) *RoomMgmtService {
	timeLive := time.Now().Format(time.RFC3339)
	minioClient := getMinioClient(config)
	postgresDB := getPostgresDB(config)

	s := &RoomMgmtService{
		conf:           config,
		joinRoomCh:     make(chan bool, 32),
		isSdkConnected: true,

		timeLive:  timeLive,
		timeReady: "",

		postgresDB:       postgresDB,
		roomMgmtSchema:   config.Postgres.RoomMgmtSchema,
		roomRecordSchema: config.Postgres.RoomRecordSchema,

		minioClient: minioClient,
		bucketName:  config.Minio.BucketName,

		sdkConnector: nil,
		roomService:  nil,

		onChanges:    make(chan string, 2048),
		systemUid:    config.RoomMgmt.SystemUid,
		lenSystemUid: len(config.RoomMgmt.SystemUid),
	}

	go s.start()
	go s.checkForRoomError()
	go s.checkForDBChanges()
	s.joinRoomCh <- true

	return s
}

func (s *RoomMgmtService) checkForDBChanges() {
	for {
		roomId := <-s.onChanges
		log.Infof("changes to roomid '%s'", roomId)
		requestURL := s.conf.RoomMgmtSentry.Url + "/rooms/" + roomId
		var err error
		var response *http.Response
		for retry := 0; retry < RETRY_COUNT; retry++ {
			response, err = http.Get(requestURL)
			if err == nil && response.StatusCode == http.StatusOK {
				break
			}
		}
		if err != nil {
			log.Errorf("error sending changes to room-sentry: %v", err)
		} else if response.StatusCode != http.StatusOK {
			log.Errorf("error sending changes to room-sentry: %v", response.StatusCode)
		}
	}
}

func (s *RoomMgmtService) closeRoom() {
	if s.roomService != nil {
		s.roomService.Leave(s.systemUid, s.systemUid)
		s.roomService.Close()
		s.roomService = nil
	}
	if s.sdkConnector != nil {
		s.sdkConnector = nil
	}
}

func getRoomService(config Config) (*sdk.Connector, *sdk.Room, error) {
	log.Infof("--- Connecting to Room Signal ---")
	log.Infof("attempt gRPC connection to %s", config.Signal.Addr)
	sdkConnector := sdk.NewConnector(config.Signal.Addr)
	if sdkConnector == nil {
		log.Errorf("connection to %s fail", config.Signal.Addr)
		return nil, nil, errors.New("")
	}
	roomService := sdk.NewRoom(sdkConnector)
	return sdkConnector, roomService, nil
}

func (s *RoomMgmtService) openRoom() {
	s.closeRoom()
	var err error
	for {
		time.Sleep(RECONNECTION_INTERVAL)
		s.sdkConnector, s.roomService, err = getRoomService(s.conf)
		if err == nil {
			s.isSdkConnected = true
			break
		}
	}
	s.joinRoom()
}

func (s *RoomMgmtService) checkForRoomError() {
	for {
		<-s.joinRoomCh
		s.timeReady = ""
		if s.isSdkConnected {
			s.isSdkConnected = false
			go s.openRoom()
		}
	}
}

func (s *RoomMgmtService) onRoomJoin(success bool, info sdk.RoomInfo, err error) {
	log.Infof("onRoomJoin success = %v, info = %v, err = %v", success, info, err)
	s.timeReady = time.Now().Format(time.RFC3339)
}

func (s *RoomMgmtService) onRoomPeerEvent(state sdk.PeerState, peer sdk.PeerInfo) {
	log.Infof("onRoomPeerEvent state = %+v, peer = %+v", state, peer)
}

func (s *RoomMgmtService) onRoomMessage(from string, to string, data map[string]interface{}) {
	log.Infof("onRoomMessage from = %+v, to = %+v, data = %+v", from, to, data)
}

func (s *RoomMgmtService) onRoomDisconnect(sid, reason string) {
	log.Infof("onRoomDisconnect sid = %+v, reason = %+v", sid, reason)
	s.joinRoomCh <- true
}

func (s *RoomMgmtService) onRoomError(err error) {
	log.Errorf("onRoomError %v", err)
	s.joinRoomCh <- true
}

func (s *RoomMgmtService) onRoomLeave(success bool, err error) {
	log.Infof("onRoomLeave: success %v, onLeave %v", success, err)
}

func (s *RoomMgmtService) onRoomInfo(info sdk.RoomInfo) {
	log.Infof("onRoomInfo: %v", info)
}

func (s *RoomMgmtService) joinRoom() {
	s.roomService.OnJoin = s.onRoomJoin
	s.roomService.OnPeerEvent = s.onRoomPeerEvent
	s.roomService.OnMessage = s.onRoomMessage
	s.roomService.OnDisconnect = s.onRoomDisconnect
	s.roomService.OnError = s.onRoomError
	s.roomService.OnLeave = s.onRoomLeave
	s.roomService.OnRoomInfo = s.onRoomInfo

	// join room
	err := s.roomService.Join(
		sdk.JoinInfo{
			Sid: s.systemUid,
			Uid: s.systemUid + sdk.RandomKey(16),
		},
	)
	if err != nil {
		s.joinRoomCh <- true
		return
	}
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
	router.GET("/rooms/:roomid/chats", s.getRoomsByRoomidChats)
	router.GET("/rooms/:roomid/chats/:fromindex/:toindex", s.getRoomsByRoomidChatRange)

	log.Infof("HTTP service starting at %s", s.conf.RoomMgmt.Addr)
	log.Errorf("%s", router.Run(s.conf.RoomMgmt.Addr))
	os.Exit(1)
}
