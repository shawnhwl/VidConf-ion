package minio

import (
	"context"
	"os"

	_ "github.com/lib/pq"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	log "github.com/pion/ion-log"
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
		log.Errorf("Unable to connect to filestore: %v\n", err)
		os.Exit(1)
	}
	err = minioClient.MakeBucket(context.Background(),
		config.BucketName,
		minio.MakeBucketOptions{})
	if err != nil {
		exists, errBucketExists := minioClient.BucketExists(context.Background(),
			config.BucketName)
		if errBucketExists != nil || !exists {
			log.Errorf("Unable to create bucket: %v\n", err)
			os.Exit(1)
		}
	}

	return minioClient
}
