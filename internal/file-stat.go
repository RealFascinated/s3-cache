package internal

import (
	"database/sql"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type CacheEntry struct {
	Bucket     string
	Key        string
	LastRead   time.Time
	CreatedAt  time.Time
	ContentType string
}

type FileStatCache struct {
	db *sql.DB
}

// newCache creates a new cache instance
func newFileStatCache(dbPath string) (*FileStatCache, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Create table if not exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS cache_entries (
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			last_read DATETIME NOT NULL,
			created_at DATETIME NOT NULL,
			content_type TEXT NOT NULL,
			PRIMARY KEY (bucket, key)
		)
	`)
	if err != nil {
		return nil, err
	}

	return &FileStatCache{db: db}, nil
}

// Get retrieves a cache entry
func (c *FileStatCache) Get(bucket, key string) (*CacheEntry, error) {
	var entry CacheEntry
	err := c.db.QueryRow(
		"SELECT bucket, key, last_read, created_at, content_type FROM cache_entries WHERE bucket = ? AND key = ?",
		bucket, key,
	).Scan(&entry.Bucket, &entry.Key, &entry.LastRead, &entry.CreatedAt, &entry.ContentType)
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &entry, err
}

// Set creates or updates a cache entry
func (c *FileStatCache) Set(bucket, key, contentType string) error {
	now := time.Now()
	_, err := c.db.Exec(`
		INSERT OR REPLACE INTO cache_entries (bucket, key, last_read, created_at, content_type)
		VALUES (?, ?, ?, ?, ?)
	`, bucket, key, now, now, contentType)
	return err
}

// Exists checks if an entry exists
func (c *FileStatCache) Exists(bucket, key string) (bool, error) {
	var exists int
	err := c.db.QueryRow(
		"SELECT 1 FROM cache_entries WHERE bucket = ? AND key = ?",
		bucket, key,
	).Scan(&exists)
	
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

// Delete deletes a cache entry
func (c *FileStatCache) Delete(bucket, key string) error {
	_, err := c.db.Exec("DELETE FROM cache_entries WHERE bucket = ? AND key = ?", bucket, key)
	return err
}

// UpdateLastRead updates only the last_read timestamp
func (c *FileStatCache) UpdateLastRead(bucket, key string) error {
	_, err := c.db.Exec(
		"UPDATE cache_entries SET last_read = ? WHERE bucket = ? AND key = ?",
		time.Now(), bucket, key,
	)
	return err
}


// GetExpired returns all expired cache entries
func (c *FileStatCache) GetExpired() ([]CacheEntry, error) {
	// Get all expired cache entries (older than 7 days)
	rows, err := c.db.Query("SELECT bucket, key, last_read, created_at, content_type FROM cache_entries WHERE last_read < ?", time.Now().Add(-time.Hour*24*7))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Scan the rows into a slice of CacheEntry
	var entries []CacheEntry
	for rows.Next() {
		var entry CacheEntry
		err := rows.Scan(&entry.Bucket, &entry.Key, &entry.LastRead, &entry.CreatedAt, &entry.ContentType)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// Close closes the database connection
func (c *FileStatCache) Close() error {
	return c.db.Close()
}

var (
	globalFileStatCache *FileStatCache
	fileStatCacheOnce   sync.Once
	fileStatCacheErr    error
)

// InitFileStatCache initializes the global cache instance
func InitFileStatCache(dbPath string) error {
	fileStatCacheOnce.Do(func() {
		globalFileStatCache, fileStatCacheErr = newFileStatCache(dbPath)
	})
	return fileStatCacheErr
}

// GetFileStatCache returns the global cache instance
func GetFileStatCache() *FileStatCache {
	return globalFileStatCache
}
