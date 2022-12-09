package constants

import (
	"errors"
	"time"

	"github.com/pion/webrtc/v3"
)

const (
	RETRY_COUNT int           = 3
	RETRY_DELAY time.Duration = 5 * time.Second

	DUP_PK       string = "duplicate key value violates unique constraint"
	NOT_FOUND_PK string = "no rows in result set"

	ATTACHMENT_FOLDERNAME string = "/attachment/"
	VIDEO_FOLDERNAME      string = "/video/"
	AUDIO_FOLDERNAME      string = "/audio/"

	UPDATEROOM_TOPIC     string = "roomUpdates."
	STARTRECORDING_TOPIC string = "recordingStart"
	ENDRECORDING_TOPIC   string = "recordingEnd."
	STARTPLAYBACK_TOPIC  string = "playbackStart"
	ENDPLAYBACK_TOPIC    string = "playbackEnd."
	DELETEPLAYBACK_TOPIC string = "playbackDelete."
	PAUSEPLAYBACK_TOPIC  string = "playbackPause."
	PLAYBACK_TOPIC       string = "playback."

	ANNOUNCEMENT_QUEUED string = "Queued"
	ANNOUNCEMENT_SENT   string = "Sent"
	ROOM_BOOKED         string = "Booked"
	ROOM_STARTED        string = "Started"
	ROOM_ENDED          string = "Ended"
	FROM_START          string = "start"
	FROM_END            string = "end"

	RECONNECTION_INTERVAL  time.Duration = 2 * time.Second
	ROOMEMPTY_INTERVAL     time.Duration = time.Minute
	ROOMEMPTYWAIT_INTERVAL time.Duration = 10 * time.Minute

	PLAYBACK_PREFIX string = "[RECORDED]"

	PLAYBACK_SPEED10 time.Duration = 10

	MIME_VIDEO   string = "VIDEO"
	MIME_AUDIO   string = "AUDIO"
	MIME_MESSAGE string = "message"
	MIME_VP8     string = webrtc.MimeTypeVP8
)

var (
	ErrQueryDB             error = errors.New("could not query database")
	ErrNotReady            error = errors.New("service is not yet ready")
	ErrMissRequestor       error = errors.New("missing mandatory field 'requestor'")
	ErrMissStartTime       error = errors.New("missing mandatory field 'startTime' in ISO9601 format")
	ErrMissEndTime         error = errors.New("missing mandatory field 'endTime' in ISO9601 format")
	ErrMissAnnounceId      error = errors.New("missing mandatory field 'announcements.announceId'")
	ErrMissAnnounceMsg     error = errors.New("missing mandatory non-empty string field 'announcements.message'")
	ErrMissAnnounceTime    error = errors.New("missing mandatory field 'announcements.relativeTimeInSeconds'")
	ErrMissAnnounceRel     error = errors.New("missing valid field 'announcements.relativeFrom' (accepts only: NULL|'start'|'end')")
	ErrDuplicateAnnounceId error = errors.New("forbidden duplication of 'announcements.announceId'")
)
