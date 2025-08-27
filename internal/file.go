package internal

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
)

type File struct {
	Data []byte
	ContentType string
}

func GetFile(bucket, key string) (*File, error) {
	before := time.Now()
	s3Client := GetS3Client()

	cache := GetFileStatCache()
	if cache == nil {
		return nil, fmt.Errorf("cache not initialized")
	}

	// Get file stat from cache if it exists
	entry, err := cache.Get(bucket, key)
	if err != nil {
		return nil, fmt.Errorf("error checking cache: %w", err)
	}

	if entry != nil {
		// Update last read time in cache
		cache.UpdateLastRead(bucket, key)

		// Get the file from the file system if it exists in cache
		filePath := "./cache/" + bucket + "/" + key	
		if _, err := os.Stat(filePath); err == nil {
			data, err := os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("error reading file data: %w", err)
			}
			fmt.Printf("CACHE HIT: %s/%s in %s\n", bucket, key, time.Since(before))
			return &File{
				Data: data,
				ContentType: entry.ContentType,
			}, nil
		}
	}

	// Get the file from S3
	reader, err := s3Client.GetObject(context.Background(), bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting file from S3: %w", err)
	}
	stat, err := reader.Stat()	
	if err != nil {
		return nil, fmt.Errorf("error getting file from S3: %w", err)
	}

	// Save the file to the file system
	os.MkdirAll("./cache/"+bucket, 0755)
	
	// Read the data from S3
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading file data: %w", err)
	}
	
	// Write to cache
	err = os.WriteFile("./cache/"+bucket+"/"+key, data, 0644)
	if err != nil {
		return nil, fmt.Errorf("error writing cache file: %w", err)
	}

	cache.Set(bucket, key, stat.ContentType)
	contentType := stat.ContentType

	// Return the file
	fmt.Printf("CACHE MISS: %s/%s in %s\n", bucket, key, time.Since(before))
	return &File{
		Data: data,
		ContentType: contentType,
	}, nil
}