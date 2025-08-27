package internal

import (
	"log"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var client *minio.Client

// GetS3Client returns the S3 client
func GetS3Client() *minio.Client {
	if client != nil {
		return client
	}

	endpoint := os.Getenv("S3_ENDPOINT")
	accessKeyID := os.Getenv("S3_ACCESS_TOKEN")
	secretAccessKey := os.Getenv("S3_ACCESS_KEY")
	useSSL := os.Getenv("S3_USE_SSL") == "true"

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	client = minioClient
	return minioClient
}