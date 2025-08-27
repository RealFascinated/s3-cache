package internal

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

type File struct {
	Data        []byte
	ContentType string
	IsPartial   bool
	Start       int64
	End         int64
	TotalSize   int64
}

func GetFile(bucket, key string) (*File, error) {
	return GetFileWithRange(bucket, key, -1, -1)
}

func GetFileWithRange(bucket, key string, start, end int64) (*File, error) {
	before := time.Now()
	s3Client := GetS3Client()

	cache := GetFileStatCache()
	if cache == nil {
		return nil, fmt.Errorf("cache not initialized")
	}

	// Get file stat from cache if it exists
	entry, err := cache.Get(bucket, key)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	if entry != nil {
		// Update last read time in cache
		cache.UpdateLastRead(bucket, key)

		// Get the file from the file system if it exists in cache
		cacheDir := getCacheDir()
		filePath := cacheDir + "/" + bucket + "/" + key	
		if _, err := os.Stat(filePath); err == nil {
			return getFileFromCacheWithRange(filePath, entry.ContentType, start, end, before)
		}
	}

	// Get the file from S3 with range support
	return getFileFromS3WithRange(s3Client, bucket, key, start, end, before)
}

func getFileFromCacheWithRange(filePath, contentType string, start, end int64, before time.Time) (*File, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	fileSize := stat.Size()

	// Handle range request
	if start >= 0 || end >= 0 {
		if start < 0 {
			start = 0
		}
		if end < 0 || end >= fileSize {
			end = fileSize - 1
		}
		if start > end {
			return nil, fmt.Errorf("invalid range: start > end")
		}

		// Seek to start position
		_, err = file.Seek(start, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}

		// Read the requested range
		data := make([]byte, end-start+1)
		_, err = io.ReadFull(file, data)
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}

		fmt.Printf("CACHE HIT (RANGE): %s in %s\n", filePath, time.Since(before))
		return &File{
			Data:        data,
			ContentType: contentType,
			IsPartial:   true,
			Start:       start,
			End:         end,
			TotalSize:   fileSize,
		}, nil
	}

	// Read entire file
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	fmt.Printf("CACHE HIT: %s in %s\n", filePath, time.Since(before))
	return &File{
		Data:        data,
		ContentType: contentType,
		IsPartial:   false,
		TotalSize:   fileSize,
	}, nil
}

func getFileFromS3WithRange(s3Client *minio.Client, bucket, key string, start, end int64, before time.Time) (*File, error) {
	// Prepare S3 options
	opts := minio.GetObjectOptions{}
	if start >= 0 || end >= 0 {
		opts.SetRange(start, end)
	}

	// Get object from S3
	reader, err := s3Client.GetObject(context.Background(), bucket, key, opts)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	defer reader.Close()

	// Get object info
	stat, err := reader.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	// Read the data
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	// Save to cache if it's a full file request
	if start < 0 && end < 0 {
		// Create the full directory structure for the key
		cacheDir := getCacheDir()
		cachePath := cacheDir + "/" + bucket + "/" + key
		dir := cachePath[:strings.LastIndex(cachePath, "/")]
		os.MkdirAll(dir, 0755)

		// Create directory if it doesn't exist
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			os.MkdirAll(dir, 0755)
		}
		
		// Write to the filesystem
		err = os.WriteFile(cachePath, data, 0644)
		if err != nil {
			// Log error but don't fail the request
			fmt.Printf("Failed to write to cache: %v\n", err)
		}

		cache := GetFileStatCache()
		if cache != nil {
			cache.Set(bucket, key, stat.ContentType)
		}
	}

	isPartial := start >= 0 || end >= 0
	actualStart := start
	actualEnd := end
	if start < 0 {
		actualStart = 0
	}
	if end < 0 {
		actualEnd = stat.Size - 1
	}

	fmt.Printf("CACHE MISS%s: %s/%s in %s\n", map[bool]string{true: " (RANGE)", false: ""}[isPartial], bucket, key, time.Since(before))
	return &File{
		Data:        data,
		ContentType: stat.ContentType,
		IsPartial:   isPartial,
		Start:       actualStart,
		End:         actualEnd,
		TotalSize:   stat.Size,
	}, nil
}

// getCacheDir returns the cache directory path from environment variable or defaults to "./cache"
func getCacheDir() string {
	if cacheDir := os.Getenv("FILE_CACHE_DIR"); cacheDir != "" {
		return cacheDir
	}
	return "./cache"
}