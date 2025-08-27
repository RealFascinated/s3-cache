package main

import (
	"os"
	"time"

	"cc.fascinated/s3-cache/internal"
	"cc.fascinated/s3-cache/internal/routes"
	"cc.fascinated/s3-cache/internal/utils"
	"github.com/labstack/echo/v4"
)

func main() {
	// Initialize environment variables if .env exists
	if _, err := os.Stat(".env"); err == nil {
		utils.LoadEnv()
	}

	// Initialize global cache
	if err := internal.InitFileStatCache(os.Getenv("FILE_CACHE_DB_PATH")); err != nil {
		panic("Failed to initialize cache: " + err.Error())
	}

	// Create Echo instance
	e := echo.New()

	// Register routes
	e.GET("/*", routes.FileHandler)	

	// Cleanup file cache every 6 hours
	go func() {
		for {
			internal.CleanupFileStatCache()
			time.Sleep(time.Hour * 6)
		}
	}()

	// Start server
	e.Logger.Fatal(e.Start(":8080"))
}