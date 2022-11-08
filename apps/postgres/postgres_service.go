package postgres

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	log "github.com/pion/ion-log"
)

const (
	RETRY_COUNT int           = 3
	RETRY_DELAY time.Duration = 5 * time.Second
)

type PostgresConf struct {
	Addr             string `mapstructure:"addr"`
	User             string `mapstructure:"user"`
	Password         string `mapstructure:"password"`
	Database         string `mapstructure:"database"`
	RoomMgmtSchema   string `mapstructure:"roomMgmtSchema"`
	RoomRecordSchema string `mapstructure:"roomRecordSchema"`
}

func GetPostgresDB(config PostgresConf) *sql.DB {
	log.Infof("--- Connecting to PostgreSql ---")
	addrSplit := strings.Split(config.Addr, ":")
	port, err := strconv.Atoi(addrSplit[1])
	if err != nil {
		log.Errorf("invalid port number: %s\n", addrSplit[1])
		os.Exit(1)
	}
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		addrSplit[0],
		port,
		config.User,
		config.Password,
		config.Database)
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
	createStmt := `CREATE SCHEMA IF NOT EXISTS "` + config.RoomMgmtSchema + `"`
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
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomMgmtSchema + `"."room"(
					"id"             UUID PRIMARY KEY,
					"name"           TEXT NOT NULL,
					"status"         TEXT NOT NULL,
					"httpEndpoint"	 TEXT NOT NULL,
					"signalEndpoint" TEXT NOT NULL,
					"startTime"      TIMESTAMPTZ NOT NULL,
					"endTime"        TIMESTAMPTZ NOT NULL,
					"allowedUserId"  TEXT ARRAY NOT NULL,
					"earlyEndReason" TEXT NOT NULL,
					"createdBy"      TEXT NOT NULL,
					"createdAt"      TIMESTAMPTZ NOT NULL,
					"updatedBy"      TEXT NOT NULL,
					"updatedAt"      TIMESTAMPTZ NOT NULL)`
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
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomMgmtSchema + `"."announcement"(
					"id"                    UUID PRIMARY KEY,
					"roomId"                UUID NOT NULL,
					"status"                TEXT NOT NULL,
					"message"               TEXT NOT NULL,
					"relativeFrom"          TEXT NOT NULL,
					"relativeTimeInSeconds" INT NOT NULL,
					"userId"                TEXT ARRAY NOT NULL,
					"createdAt"             TIMESTAMPTZ NOT NULL,
					"createdBy"             TEXT NOT NULL,
					"updatedAt"             TIMESTAMPTZ NOT NULL,
					"updatedBy"             TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomMgmtSchema + `"."room"("id") ON DELETE CASCADE)`
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
	createStmt = `CREATE SCHEMA IF NOT EXISTS "` + config.RoomRecordSchema + `"`
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
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."room"(
					"id"        UUID PRIMARY KEY,
					"name"      TEXT NOT NULL,
					"startTime" TIMESTAMPTZ NOT NULL,
					"endTime"   TIMESTAMPTZ NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("id") REFERENCES "` + config.RoomMgmtSchema + `"."room"("id"))`
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
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomMgmtSchema + `"."playback"(
					"id"             UUID PRIMARY KEY,
					"roomId"         UUID NOT NULL,
					"name"           TEXT NOT NULL,
					"httpEndpoint"   TEXT NOT NULL,
					"signalEndpoint" TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id"))`
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
	// create table "peerEvent"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."peerEvent"(
					"id"        UUID PRIMARY KEY,
					"roomId"    UUID NOT NULL,
					"timestamp" TIMESTAMPTZ NOT NULL,
					"state"     INT NOT NULL,
					"peerId"    TEXT NOT NULL,
					"peerName"  TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
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
	// create table "chat"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."chat"(
					"id"        UUID PRIMARY KEY,
					"roomId"    UUID NOT NULL,
					"timestamp" TIMESTAMPTZ NOT NULL,
					"userId"    TEXT NOT NULL,
					"userName"  TEXT NOT NULL,
					"text"      TEXT NOT NULL,
					"fileName"  TEXT NOT NULL,
					"fileSize"  INT NOT NULL,
					"filePath"  TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
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
	// create table "trackEvent"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."trackEvent"(
					"id"             UUID PRIMARY KEY,
					"roomId"         UUID NOT NULL,
					"peerId"         TEXT NOT NULL,
					"trackRemoteIds" TEXT ARRAY NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
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
	// create table "track"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."track"(
					"id"            UUID PRIMARY KEY,
					"roomId"        UUID NOT NULL,
					"trackRemoteId" TEXT NOT NULL,
					"mimeType"      TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
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
	// create table "trackStream"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."trackStream"(
					"id"       UUID PRIMARY KEY,
					"trackId"  UUID NOT NULL,
					"roomId"   UUID NOT NULL,
					"filePath" TEXT NOT NULL,
					CONSTRAINT fk_track FOREIGN KEY("trackId") REFERENCES "` + config.RoomRecordSchema + `"."track"("id") ON DELETE CASCADE,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
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
