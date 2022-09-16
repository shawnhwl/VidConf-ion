package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/pion/ion-log"
	sdk "github.com/pion/ion-sdk-go"
	"github.com/pion/ion/pkg/db"
)

const (
	DB_ROOMS string = "ROOMS"

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

	SYSTEM_ID   string = "system"
	SYSTEM_Name string = "PA System"
)

type Announcement struct {
	Status                string   `json:"status"`
	AnnounceId            int64    `json:"announceId"`
	Message               string   `json:"message"`
	RelativeFrom          string   `json:"relativeFrom"`
	RelativeTimeInSeconds int64    `json:"relativeTimeInSeconds"`
	UserId                []string `json:"userId"`
}

type Room struct {
	Status         string         `json:"status"`
	RoomId         string         `json:"roomId"`
	RoomName       string         `json:"roomName"`
	StartTime      time.Time      `json:"startTime"`
	EndTime        time.Time      `json:"endTime"`
	Announcements  []Announcement `json:"announcements"`
	Users          []User         `json:"users"`
	EarlyEndReason string         `json:"earlyEndReason"`
}

type User struct {
	UserId   string `json:"userId"`
	UserName string `json:"userName"`
}

type Rooms struct {
	RoomIds []string `json:"roomId"`
}

type Patch_Announcement struct {
	AnnounceId            *int64   `json:"announceId,omitempty"`
	Message               *string  `json:"message,omitempty"`
	RelativeFrom          *string  `json:"relativeFrom,omitempty"`
	RelativeTimeInSeconds *int64   `json:"relativeTimeInSeconds,omitempty"`
	UserId                []string `json:"userId,omitempty"`
}

type Patch_Room struct {
	RoomName      *string              `json:"roomName,omitempty"`
	StartTime     *time.Time           `json:"startTime,omitempty"`
	EndTime       *time.Time           `json:"endTime,omitempty"`
	Announcements []Patch_Announcement `json:"announcements,omitempty"`
}

type Get_Announcement struct {
	AnnounceId            int64    `json:"announceId"`
	Message               string   `json:"message"`
	RelativeFrom          string   `json:"relativeFrom"`
	RelativeTimeInSeconds int64    `json:"relativeTimeInSeconds"`
	UserId                []string `json:"userId"`
}

type Get_Room struct {
	Status        string             `json:"status"`
	RoomId        string             `json:"roomId"`
	RoomName      string             `json:"roomName"`
	StartTime     time.Time          `json:"startTime"`
	EndTime       time.Time          `json:"endTime"`
	Announcements []Get_Announcement `json:"announcements"`
	Users         []User             `json:"users"`
}

type Delete_Room struct {
	TimeLeftInSeconds *int64  `json:"timeLeftInSeconds,omitempty"`
	Reason            *string `json:"reason,omitempty"`
}

type Delete_Announcement struct {
	AnnounceId []int64 `json:"announceId"`
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
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	roomJSON, err := json.MarshalIndent(patch_room, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	log.Infof("request:\n%s", string(roomJSON))

	if patch_room.StartTime == nil {
		log.Warnf(MISS_START_TIME)
		c.String(http.StatusBadRequest, MISS_START_TIME)
		return
	}
	if patch_room.EndTime == nil {
		log.Warnf(MISS_END_TIME)
		c.String(http.StatusBadRequest, MISS_END_TIME)
		return
	}

	var room Room
	roomId := uuid.NewString()
	room.Status = ROOM_BOOKED
	room.RoomId = roomId
	room.Users = make([]User, 0)
	room.Announcements = make([]Announcement, 0)
	room.EarlyEndReason = ""
	err = s.patchRoom(&room, patch_room)
	if err != nil {
		log.Warnf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	roomJSON, err = json.Marshal(room)
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}

	s.redisDB.Set(roomId, roomJSON, 0)

	roomJSON, _ = json.MarshalIndent(room, "", "    ")
	log.Infof("%s", roomJSON)

	dbRecords := s.redisDB.Get(DB_ROOMS)
	var rooms Rooms
	if dbRecords != "" {
		err = json.Unmarshal([]byte(dbRecords), &rooms)
		if err != nil {
			log.Errorf("could not decode booking records: %s", err)
			c.String(http.StatusInternalServerError, "database corrupted")
			return
		}
	}
	rooms.RoomIds = append(rooms.RoomIds, roomId)
	roomsJSON, err := json.Marshal(rooms)
	if err != nil {
		log.Errorf(err.Error())
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}
	s.redisDB.Set(DB_ROOMS, roomsJSON, 0)

	s.onChanges <- roomId
	log.Infof("posted room '%s':\n%s\nrooms:%s", roomId, s.redisDB.Get(roomId), s.redisDB.Get(DB_ROOMS))
	c.String(http.StatusOK, "%s?room=%s", s.conf.WebApp.Url, roomId)
}

func (s *RoomMgmtService) getRooms(c *gin.Context) {
	log.Infof("GET /rooms")

	dbRecords := s.redisDB.Get(DB_ROOMS)
	if dbRecords == "" {
		c.String(http.StatusOK, "{\n    \"roomId\": []\n}")
		return
	}

	var rooms Rooms
	err := json.Unmarshal([]byte(dbRecords), &rooms)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		c.String(http.StatusInternalServerError, "database corrupted")
		return
	}
	roomsJSON, err := json.MarshalIndent(rooms, "", "    ")
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}

	c.String(http.StatusOK, string(roomsJSON))
}

func (s *RoomMgmtService) getRoomsByRoomid(c *gin.Context) {
	roomid := c.Param("roomid")
	log.Infof("GET /rooms/%s", roomid)

	dbRecords := s.redisDB.Get(roomid)
	if dbRecords == "" {
		log.Warnf(s.roomNotFound(roomid))
		c.String(http.StatusBadRequest, s.roomNotFound(roomid))
		return
	}

	var get_room Get_Room
	err := json.Unmarshal([]byte(dbRecords), &get_room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		c.String(http.StatusInternalServerError, "database corrupted")
		return
	}

	if get_room.Status == ROOM_STARTED {
		get_room.Users = append(get_room.Users, s.getPeers(roomid)...)
	}

	roomsJSON, err := json.MarshalIndent(get_room, "", "    ")
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}

	c.String(http.StatusOK, string(roomsJSON))
}

func (s *RoomMgmtService) patchRoomsByRoomid(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("PATCH /rooms/%s", roomId)

	var patch_room Patch_Room
	if err := c.ShouldBindJSON(&patch_room); err != nil {
		log.Warnf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	roomJSON, err := json.MarshalIndent(patch_room, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	log.Infof("request:\n%s", string(roomJSON))

	dbRecords := s.redisDB.Get(roomId)
	if dbRecords == "" {
		log.Warnf(s.roomNotFound(roomId))
		c.String(http.StatusBadRequest, s.roomNotFound(roomId))
		return
	}

	var room Room
	err = json.Unmarshal([]byte(dbRecords), &room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		c.String(http.StatusInternalServerError, "database corrupted")
		return
	}

	err = s.patchRoom(&room, patch_room)
	if err != nil {
		log.Warnf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	roomJSON, err = json.Marshal(room)
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}
	s.redisDB.Set(roomId, roomJSON, 0)
	var get_room Get_Room
	err = json.Unmarshal([]byte(roomJSON), &get_room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		c.String(http.StatusInternalServerError, "database corrupted")
		return
	}

	if get_room.Status == ROOM_STARTED {
		get_room.Users = append(get_room.Users, s.getPeers(roomId)...)
	}

	roomsJSON, err := json.MarshalIndent(get_room, "", "    ")
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}

	s.onChanges <- roomId
	log.Infof("patched room:\n%s", s.redisDB.Get(roomId))
	c.String(http.StatusOK, string(roomsJSON))
}

func (s *RoomMgmtService) deleteRoomsByRoomId(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("DELETE /rooms/%s", roomId)

	var delete_room Delete_Room
	if err := c.ShouldBindJSON(&delete_room); err != nil {
		log.Warnf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	roomJSON, err := json.MarshalIndent(delete_room, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	log.Infof("request:\n%s", string(roomJSON))

	dbRecords := s.redisDB.Get(roomId)
	if dbRecords == "" {
		log.Warnf(s.roomNotFound(roomId))
		c.String(http.StatusBadRequest, s.roomNotFound(roomId))
		return
	}

	var room Room
	err = json.Unmarshal([]byte(dbRecords), &room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		c.String(http.StatusInternalServerError, "database corrupted")
		return
	}

	var timeLeftInSeconds int64
	if delete_room.TimeLeftInSeconds == nil {
		timeLeftInSeconds = 0
	} else {
		timeLeftInSeconds = *delete_room.TimeLeftInSeconds
	}
	room.EndTime = time.Now().Add(time.Second * time.Duration(timeLeftInSeconds))
	if delete_room.Reason == nil {
		room.EarlyEndReason = "session terminated"
	} else if *delete_room.Reason == "" {
		room.EarlyEndReason = "session terminated"
	} else {
		room.EarlyEndReason = *delete_room.Reason
	}
	roomJSON, err = json.Marshal(room)
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}
	s.redisDB.Set(roomId, roomJSON, 0)

	s.onChanges <- roomId
	log.Infof("deleted room:\n%s", s.redisDB.Get(roomId))
	c.String(http.StatusOK, "Ending roomId '%s' in %d minutes", roomId, timeLeftInSeconds/60)
}

func (s *RoomMgmtService) deleteUsersByUserId(c *gin.Context) {
	roomId := c.Param("roomid")
	userId := c.Param("userid")
	log.Infof("DELETE /rooms/%s/users/%s", roomId, userId)

	dbRecords := s.redisDB.Get(roomId)
	if dbRecords == "" {
		log.Warnf(s.roomNotFound(roomId))
		c.String(http.StatusBadRequest, s.roomNotFound(roomId))
		return
	}

	var room Room
	err := json.Unmarshal([]byte(dbRecords), &room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		c.String(http.StatusInternalServerError, "database corrupted")
		return
	}

	if room.Status != ROOM_STARTED {
		log.Warnf(s.roomNotStarted(roomId))
		c.String(http.StatusBadRequest, s.roomNotStarted(roomId))
		return
	}
	warn, err := s.kickUser(roomId, userId)
	if err != nil {
		log.Errorf(err.Error())
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if warn != nil {
		log.Warnf(warn.Error())
		c.String(http.StatusBadRequest, warn.Error())
		return
	}
	c.String(http.StatusOK, "Kicked userId '%s' from roomId '%s'", userId, roomId)
}

func (s *RoomMgmtService) putAnnouncementsByRoomId(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("PUT /rooms/%s/announcements", roomId)

	var patch_room Patch_Room
	if err := c.ShouldBindJSON(&patch_room); err != nil {
		log.Warnf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	roomJSON, err := json.MarshalIndent(patch_room, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	log.Infof("request:\n%s", string(roomJSON))

	dbRecords := s.redisDB.Get(roomId)
	if dbRecords == "" {
		log.Warnf(s.roomNotFound(roomId))
		c.String(http.StatusBadRequest, s.roomNotFound(roomId))
		return
	}

	var room Room
	err = json.Unmarshal([]byte(dbRecords), &room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		c.String(http.StatusInternalServerError, "database corrupted")
		return
	}

	err = s.putAnnouncement(&room, patch_room)
	if err != nil {
		log.Warnf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	roomJSON, err = json.Marshal(room)
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}
	s.redisDB.Set(roomId, roomJSON, 0)
	var get_room Get_Room
	err = json.Unmarshal([]byte(roomJSON), &get_room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		c.String(http.StatusInternalServerError, "database corrupted")
		return
	}

	if get_room.Status == ROOM_STARTED {
		get_room.Users = append(get_room.Users, s.getPeers(roomId)...)
	}

	roomsJSON, err := json.MarshalIndent(get_room, "", "    ")
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}

	s.onChanges <- roomId
	log.Infof("put announcements:\n%s", s.redisDB.Get(roomId))
	c.String(http.StatusOK, string(roomsJSON))
}

func (s *RoomMgmtService) deleteAnnouncementsByRoomId(c *gin.Context) {
	roomId := c.Param("roomid")
	log.Infof("DELETE /rooms/%s/announcements", roomId)

	var delete_announce Delete_Announcement
	if err := c.ShouldBindJSON(&delete_announce); err != nil {
		log.Warnf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	announceHSON, err := json.MarshalIndent(delete_announce, "", "    ")
	if err != nil {
		log.Errorf(err.Error())
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	log.Infof("request:\n%s", string(announceHSON))

	dbRecords := s.redisDB.Get(roomId)
	if dbRecords == "" {
		log.Warnf(s.roomNotFound(roomId))
		c.String(http.StatusBadRequest, s.roomNotFound(roomId))
		return
	}

	var room Room
	err = json.Unmarshal([]byte(dbRecords), &room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		c.String(http.StatusInternalServerError, "database corrupted")
		return
	}
	if len(room.Announcements) == 0 {
		warnString := fmt.Sprintf("roomid '%s' has no announcements in database", roomId)
		log.Warnf(warnString)
		c.String(http.StatusBadRequest, warnString)
		return
	}

	isDeleteAll := false
	if delete_announce.AnnounceId == nil {
		isDeleteAll = true
	} else if len(delete_announce.AnnounceId) == 0 {
		isDeleteAll = true
	}
	if isDeleteAll {
		room.Announcements = make([]Announcement, 0)
	} else {
		toDelete := make([]int, 0)
		for _, announceId := range delete_announce.AnnounceId {
			isFound := false
			for id, announcements := range room.Announcements {
				if announcements.AnnounceId == announceId {
					toDelete = append(toDelete, id)
					isFound = true
					break
				}
			}
			if !isFound {
				warnString := fmt.Sprintf("roomid '%s' does not have announceId '%d' in database", roomId, announceId)
				log.Warnf(warnString)
				c.String(http.StatusBadRequest, warnString)
				return
			}
		}
		for _, id := range toDelete {
			room.Announcements = removeSlice(room.Announcements, id)
		}
	}
	roomJSON, err := json.Marshal(room)
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}
	s.redisDB.Set(roomId, roomJSON, 0)
	var get_room Get_Room
	err = json.Unmarshal([]byte(roomJSON), &get_room)
	if err != nil {
		log.Errorf("could not decode booking records: %s", err)
		c.String(http.StatusInternalServerError, "database corrupted")
		return
	}

	if get_room.Status == ROOM_STARTED {
		get_room.Users = append(get_room.Users, s.getPeers(roomId)...)
	}

	roomsJSON, err := json.MarshalIndent(get_room, "", "    ")
	if err != nil {
		log.Errorf("could not encode room to JSON: %s", err)
		c.String(http.StatusInternalServerError, "JSON encoding error")
		return
	}

	s.onChanges <- roomId
	log.Infof("deleted announcements:\n%s", s.redisDB.Get(roomId))
	c.String(http.StatusOK, string(roomsJSON))
}

func (s *RoomMgmtService) roomNotFound(roomId string) string {
	return "RoomId " + roomId + " not found in database"
}

func (s *RoomMgmtService) roomNotStarted(roomId string) string {
	return "RoomId " + roomId + " session not started"
}

func (s *RoomMgmtService) patchRoom(room *Room, patch_room Patch_Room) error {
	if patch_room.RoomName != nil {
		room.RoomName = *patch_room.RoomName
	}
	if patch_room.StartTime != nil {
		room.StartTime = *patch_room.StartTime
	}
	if patch_room.EndTime != nil {
		room.EndTime = *patch_room.EndTime
	}
	idMap := make(map[int64]int)
	for _, patch := range patch_room.Announcements {
		if patch.AnnounceId == nil {
			return errors.New(MISS_ANNOUNCE_ID)
		}
		_, exist := idMap[*patch.AnnounceId]
		if exist {
			return errors.New(DUP_ANNOUNCE_ID)
		}
		idMap[*patch.AnnounceId] = 1

		isPatched := false
		for id := range room.Announcements {
			if room.Announcements[id].AnnounceId == *patch.AnnounceId {
				isPatched = true
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
				break
			}
		}
		if isPatched {
			continue
		}

		var announcement Announcement
		announcement.Status = ANNOUNCEMENT_QUEUED
		announcement.AnnounceId = *patch.AnnounceId
		if patch.Message == nil || *patch.Message == "" {
			return errors.New(MISS_ANNOUNCE_MSG)
		}
		announcement.Message = *patch.Message
		if patch.RelativeFrom != nil &&
			*patch.RelativeFrom != FROM_START &&
			*patch.RelativeFrom != FROM_END {
			return errors.New(MISS_ANNOUNCE_REL)
		}
		if patch.RelativeFrom == nil {
			announcement.RelativeFrom = FROM_END
		} else {
			announcement.RelativeFrom = *patch.RelativeFrom
		}
		if patch.RelativeTimeInSeconds == nil {
			return errors.New(MISS_ANNOUNCE_TIME)
		}
		announcement.RelativeTimeInSeconds = *patch.RelativeTimeInSeconds
		announcement.UserId = make([]string, 0)
		announcement.UserId = append(announcement.UserId, patch.UserId...)
		room.Announcements = append(room.Announcements, announcement)
	}

	return nil
}

func (s *RoomMgmtService) putAnnouncement(room *Room, patch_room Patch_Room) error {
	idMap := make(map[int64]int)
	for _, patch := range patch_room.Announcements {
		if patch.AnnounceId == nil {
			return errors.New(MISS_ANNOUNCE_ID)
		}
		_, exist := idMap[*patch.AnnounceId]
		if exist {
			return errors.New(DUP_ANNOUNCE_ID)
		}
		idMap[*patch.AnnounceId] = 1

		var announcement Announcement
		announcement.Status = ANNOUNCEMENT_QUEUED
		announcement.AnnounceId = *patch.AnnounceId
		if patch.Message == nil || *patch.Message == "" {
			return errors.New(MISS_ANNOUNCE_MSG)
		}
		announcement.Message = *patch.Message
		if patch.RelativeFrom != nil &&
			*patch.RelativeFrom != FROM_START &&
			*patch.RelativeFrom != FROM_END {
			return errors.New(MISS_ANNOUNCE_REL)
		}
		if patch.RelativeFrom == nil {
			announcement.RelativeFrom = FROM_END
		} else {
			announcement.RelativeFrom = *patch.RelativeFrom
		}
		if patch.RelativeTimeInSeconds == nil {
			return errors.New(MISS_ANNOUNCE_TIME)
		}
		announcement.RelativeTimeInSeconds = *patch.RelativeTimeInSeconds
		announcement.UserId = make([]string, 0)
		announcement.UserId = append(announcement.UserId, patch.UserId...)

		isPatched := false
		for id := range room.Announcements {
			if room.Announcements[id].AnnounceId == *patch.AnnounceId {
				isPatched = true
				room.Announcements[id] = announcement
				break
			}
		}
		if isPatched {
			continue
		}

		room.Announcements = append(room.Announcements, announcement)
	}

	return nil
}

func removeSlice[T any](slice []T, id int) []T {
	copy(slice[id:], slice[id+1:])
	return slice[:len(slice)-1]
}

type Terminations struct {
	timeTick time.Time
	reason   string
}

type Announcements struct {
	timeTick time.Time
	message  string
	userId   []string
}

type AnnounceKey struct {
	roomId     string
	announceId int64
}

type RoomMgmtService struct {
	conf Config

	timeLive     string
	timeReady    string
	roomService  *sdk.Room
	redisDB      *db.Redis
	onChanges    chan string
	pollInterval time.Duration

	roomStarts       map[string]time.Time
	roomStartKeys    []string
	roomEnds         map[string]Terminations
	roomEndKeys      []string
	announcements    map[AnnounceKey]Announcements
	announcementKeys []AnnounceKey
}

func NewRoomMgmtService(config Config) *RoomMgmtService {
	timeLive := time.Now().Format(time.RFC3339)

	log.Infof("--- Testing Redis Connectionr ---")
	redis_db := db.NewRedis(config.Redis)
	if redis_db == nil {
		log.Panicf("connection to %s fail", config.Redis.Addrs)
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
		conf:         config,
		timeLive:     timeLive,
		timeReady:    timeReady,
		roomService:  roomService,
		redisDB:      redis_db,
		onChanges:    make(chan string, 2048),
		pollInterval: time.Duration(config.Http.PollInSeconds) * time.Second,
	}
	go s.RoomMgmtSentinel()
	<-s.onChanges
	go s.start()
	return s
}

func (s *RoomMgmtService) start() {
	defer s.redisDB.Close()
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

	if s.conf.Http.Cert != "" && s.conf.Http.Key != "" {
		log.Infof("HTTP service starting at %s", s.conf.Http.Addr)
		log.Panicf("%s", router.RunTLS(s.conf.Http.Addr, s.conf.Http.Cert, s.conf.Http.Key))
	} else {
		log.Infof("HTTP service starting at %s", s.conf.Http.Addr)
		log.Panicf("%s", router.Run(s.conf.Http.Addr))
	}
}

func (s *RoomMgmtService) testAPI(router *gin.Engine) {
	// tests
	router.POST("/rooms/:roomid", func(c *gin.Context) {
		roomid := c.Param("roomid")
		log.Infof("POST /rooms/%s/", roomid)
		err := s.createRoom(roomid)
		if err != nil {
			c.String(http.StatusInternalServerError, "POST /rooms/%s [ERR]%s", roomid, err.Error())
			return
		}
		c.String(http.StatusOK, "POST /rooms/%s", roomid)
	})
	router.POST("/rooms/:roomid/messages/:message/:from/:to", func(c *gin.Context) {
		roomid := c.Param("roomid")
		message := c.Param("message")
		from := c.Param("from")
		to := c.Param("to")
		log.Infof("POST /rooms/%s/messages/%s/users/%s", roomid, message, to)
		userids := make([]string, 0)
		userids = append(userids, to)
		err := s.postMessage(roomid, message, from, userids)
		if err != nil {
			c.String(http.StatusInternalServerError,
				"POST /rooms/%s/messages/%s/%s/%s [ERR]%s", roomid, message, from, to, err.Error())
			return
		}
		c.String(http.StatusOK, "POST /rooms/%s/messages/%s/%s/%s", roomid, message, from, to)
	})
	router.DELETE("/rooms/:roomid/reasons/:reason", func(c *gin.Context) {
		roomid := c.Param("roomid")
		reason := c.Param("reason")
		log.Infof("DELETE /rooms/%s/reasons/%s", roomid, reason)
		err := s.endRoom(roomid, reason)
		if err != nil {
			c.String(http.StatusInternalServerError, "DELETE /rooms/%s/reasons/%s [ERR]%s", roomid, reason, err.Error())
			return
		}
		c.String(http.StatusOK, "DELETE /rooms/%s/reasons/%s", roomid, reason)
	})
	router.GET("/rooms/:roomid/users", func(c *gin.Context) {
		roomid := c.Param("roomid")
		log.Infof("GET /rooms/%s/users", roomid)
		users := s.getPeers(roomid)
		c.String(http.StatusOK, "%v", users)
	})
	router.POST("/rooms/:roomid/users/:userid/:username", func(c *gin.Context) {
		roomid := c.Param("roomid")
		userid := c.Param("userid")
		username := c.Param("username")
		log.Infof("POST /rooms/%s/users/%s/%s", roomid, userid, username)
		var peerInfo sdk.PeerInfo
		peerInfo.Sid = roomid
		peerInfo.Uid = userid
		peerInfo.DisplayName = username
		err := s.roomService.AddPeer(peerInfo)
		if err != nil {
			c.String(http.StatusInternalServerError, "POST /rooms/%s/users/%s [ERR]%s", roomid, userid, err.Error())
			return
		}
		c.String(http.StatusOK, "POST /rooms/%s/users/%s", roomid, userid)
	})
	router.POST("/kick/rooms/:roomid/users/:userid", func(c *gin.Context) {
		roomid := c.Param("roomid")
		userid := c.Param("userid")
		log.Infof("POST /kick/rooms/%s/users/%s", roomid, userid)
		warn, err := s.kickUser(roomid, userid)
		if err != nil {
			log.Errorf(err.Error())
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		if warn != nil {
			log.Warnf(warn.Error())
			c.String(http.StatusBadRequest, warn.Error())
			return
		}
		c.String(http.StatusOK, "Kicked userId '%s' from roomId '%s'", userid, roomid)
	})
}
