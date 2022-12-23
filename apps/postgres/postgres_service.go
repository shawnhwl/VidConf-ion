package postgres

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	log "github.com/pion/ion-log"
	constants "github.com/pion/ion/apps/constants"
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
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		postgresDB, err = sql.Open("postgres", psqlconn)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to connect to database: %s\n", err)
		os.Exit(1)
	}
	// postgresDB.Ping
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		err = postgresDB.Ping()
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to ping database: %s\n", err)
		os.Exit(1)
	}
	// create RoomMgmtSchema schema
	createStmt := `CREATE SCHEMA IF NOT EXISTS "` + config.RoomMgmtSchema + `"`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
		os.Exit(1)
	}
	// create table "room"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomMgmtSchema + `"."room"(
					"id"             UUID PRIMARY KEY,
					"name"           TEXT NOT NULL,
					"status"         TEXT NOT NULL,
					"startTime"      TIMESTAMPTZ NOT NULL,
					"endTime"        TIMESTAMPTZ NOT NULL,
					"allowedUserId"  JSONB NOT NULL,
					"earlyEndReason" TEXT NOT NULL,
					"createdBy"      TEXT NOT NULL,
					"createdAt"      TIMESTAMPTZ NOT NULL,
					"updatedBy"      TEXT NOT NULL,
					"updatedAt"      TIMESTAMPTZ NOT NULL)`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
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
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
		os.Exit(1)
	}

	// create RoomRecordSchema schema
	createStmt = `CREATE SCHEMA IF NOT EXISTS "` + config.RoomRecordSchema + `"`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
		os.Exit(1)
	}
	// create table "room"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."room"(
					"id"        UUID PRIMARY KEY,
					"name"      TEXT NOT NULL,
					"startTime" TIMESTAMPTZ NOT NULL,
					"endTime"   TIMESTAMPTZ NOT NULL)`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
		os.Exit(1)
	}
	// create table "playback"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomMgmtSchema + `"."playback"(
					"id"             UUID PRIMARY KEY,
					"roomId"         UUID NOT NULL,
					"name"           TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id"))`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
		os.Exit(1)
	}
	// create table "metadata"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."metadata"(
		"id"        UUID PRIMARY KEY,
		"roomId"    UUID NOT NULL,
		"timestamp" TIMESTAMPTZ NOT NULL,
		"data"      JSON NOT NULL,
		CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
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
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
		os.Exit(1)
	}
	// create table "chat"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."chat"(
					"id"        UUID PRIMARY KEY,
					"roomId"    UUID NOT NULL,
					"timestamp" TIMESTAMPTZ NOT NULL,
					"mimeType"  TEXT NOT NULL,
					"userId"    TEXT NOT NULL,
					"userName"  TEXT NOT NULL,
					"text"      TEXT NOT NULL,
					"fileName"  TEXT NOT NULL,
					"fileSize"  INT NOT NULL,
					"filePath"  TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
		os.Exit(1)
	}
	// create table "trackEvent"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."trackEvent"(
					"id"             UUID PRIMARY KEY,
					"roomId"         UUID NOT NULL,
					"peerId"         TEXT NOT NULL,
					"trackRemoteIds" TEXT ARRAY NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
		os.Exit(1)
	}
	// create table "track"
	createStmt = `CREATE TABLE IF NOT EXISTS "` + config.RoomRecordSchema + `"."track"(
					"id"            UUID PRIMARY KEY,
					"roomId"        UUID NOT NULL,
					"trackRemoteId" TEXT NOT NULL,
					"mimeType"      TEXT NOT NULL,
					CONSTRAINT fk_room FOREIGN KEY("roomId") REFERENCES "` + config.RoomRecordSchema + `"."room"("id") ON DELETE CASCADE)`
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
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
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = postgresDB.Exec(createStmt)
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("Unable to execute sql statement: %s\n", err)
		os.Exit(1)
	}

	return postgresDB
}

func StringArray2ByteArray(stringArray []string) ([]byte, error) {
	log.Infof("stringArray:%v", stringArray)
	var buffer bytes.Buffer
	err := json.NewEncoder(&buffer).Encode(stringArray)
	if err != nil {
		log.Errorf("could not encode []string to []byte: %s", err)
		return nil, err
	}
	byteArray, err := ioutil.ReadAll(&buffer)
	if err != nil {
		log.Errorf("could not encode []string to []byte: %s", err)
		return nil, err
	}

	log.Infof("byteArray:%s", byteArray)
	return byteArray, nil
}

func ByteArray2StringArray(byteArray []byte) ([]string, error) {
	log.Infof("byteArray:%s", byteArray)
	stringArray := make([]string, 0)
	err := json.NewDecoder(bytes.NewReader(byteArray)).Decode(&stringArray)
	if err != nil {
		log.Errorf("could not decode []byte to []string: %s", err)
		return stringArray, err
	}

	log.Infof("stringArray:%v", stringArray)
	return stringArray, nil
}
