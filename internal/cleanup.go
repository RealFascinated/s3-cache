package internal

import (
	"fmt"
	"os"
)

// CleanupFileStatCache deletes expired file stat cache entries and removes the files from the cache directory
func CleanupFileStatCache() {
	cache := GetFileStatCache()
	if cache == nil {
		return
	}

	expired, err := cache.GetExpired()
	if err != nil {
		return
	}

	for _, entry := range expired {
		cache.Delete(entry.Bucket, entry.Key)
		os.Remove("./cache/" + entry.Bucket + "/" + entry.Key)
	}

	fmt.Printf("Cleaned up file stat cache for %d files\n", len(expired))
}