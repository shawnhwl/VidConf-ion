package minio

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	log "github.com/pion/ion-log"
	constants "github.com/pion/ion/apps/constants"
)

type MinioConf struct {
	Endpoint        string `mapstructure:"endpoint"`
	UseSSL          bool   `mapstructure:"useSSL"`
	AccessKeyID     string `mapstructure:"username"`
	SecretAccessKey string `mapstructure:"password"`
	BucketName      string `mapstructure:"bucketName"`
}

func GetMinioClient(config MinioConf) *minio.Client {
	log.Infof("--- Connecting to MinIO ---")
	minioClient, err := minio.New(config.Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(config.AccessKeyID,
			config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		log.Errorf("Unable to connect to filestore: %s\n", err)
		os.Exit(1)
	}
	err = minioClient.MakeBucket(context.Background(),
		config.BucketName,
		minio.MakeBucketOptions{})
	if err != nil {
		exists, errBucketExists := minioClient.BucketExists(context.Background(),
			config.BucketName)
		if errBucketExists != nil || !exists {
			log.Errorf("Unable to create bucket: %s\n", err)
			os.Exit(1)
		}
	}

	testMinioClient(minioClient, config)
	return minioClient
}

type TestTrack struct {
	Timestamp time.Time
	Data      []byte
}

func testMinioClient(minioClient *minio.Client, config MinioConf) {
	testString := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	ptestString := &testString
	testStringData := bytes.NewReader([]byte(*ptestString))
	var err error
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		_, err = minioClient.PutObject(context.Background(),
			config.BucketName,
			"/test/testString",
			testStringData,
			int64(len(*ptestString)),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not upload attachment: %s", err)
		os.Exit(1)
	}

	var object *minio.Object
	for retry := 0; retry < constants.RETRY_COUNT; retry++ {
		object, err = minioClient.GetObject(context.Background(),
			config.BucketName,
			"/test/testString",
			minio.GetObjectOptions{})
		if err == nil {
			break
		}
		time.Sleep(constants.RETRY_DELAY)
	}
	if err != nil {
		log.Errorf("could not download attachment: %s", err)
		os.Exit(1)
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(object)
	if err != nil {
		log.Errorf("could not process downloaded attachment: %s", err)
		os.Exit(1)
	}
	if testString != buf.String() {
		log.Errorf("downloaded base64 string is different: %s", buf.String())
		os.Exit(1)
	}

	testTracks := make([]TestTrack, 0)
	for id := 0; id < 100; id++ {
		timeNow := time.Now()
		testTracks = append(testTracks, TestTrack{timeNow, []byte(testString)})
	}
	for id := 0; id < 100; id += 5 {
		var testTrackData bytes.Buffer
		err := gob.NewEncoder(&testTrackData).Encode(testTracks[id : id+5])
		if err != nil {
			log.Errorf("track encoding error:", err)
			os.Exit(1)
		}

		var track []TestTrack
		buf := bytes.NewBuffer(testTrackData.Bytes())
		err = gob.NewDecoder(buf).Decode(&track)
		if err != nil {
			log.Errorf("track decoding error:", err)
			os.Exit(1)
		}

		if string(testTracks[id].Data) != string(track[0].Data) {
			log.Errorf("codec tracks.Data is different: %s vs %s", string(testTracks[id].Data), string(track[0].Data))
			os.Exit(1)
		}

		for retry := 0; retry < constants.RETRY_COUNT; retry++ {
			_, err = minioClient.PutObject(context.Background(),
				config.BucketName,
				"/test/testTrack"+strconv.Itoa(id),
				&testTrackData,
				int64(testTrackData.Len()),
				minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err == nil {
				break
			}
			time.Sleep(constants.RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not upload file: %s", err)
			os.Exit(1)
		}
	}

	recvTracks := make([]TestTrack, 0)
	for id := 0; id < 100; id += 5 {
		for retry := 0; retry < constants.RETRY_COUNT; retry++ {
			object, err = minioClient.GetObject(context.Background(),
				config.BucketName,
				"/test/testTrack"+strconv.Itoa(id),
				minio.GetObjectOptions{})
			if err == nil {
				break
			}
			time.Sleep(constants.RETRY_DELAY)
		}
		if err != nil {
			log.Errorf("could not download attachment: %s", err)
			os.Exit(1)
		}

		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(object)
		if err != nil {
			log.Errorf("could not process download: %s", err)
			continue
		}
		var tracks []TestTrack
		gob.NewDecoder(buf).Decode(&tracks)
		recvTracks = append(recvTracks, tracks...)
	}

	for id := 0; id < 100; id++ {
		if string(testTracks[id].Data) != string(recvTracks[id].Data) {
			log.Errorf("downloaded tracks.data is different: %s vs %s", string(testTracks[id].Data), string(recvTracks[id].Data))
			os.Exit(1)
		}
	}

	log.Infof("testMinioClient OK")
}
