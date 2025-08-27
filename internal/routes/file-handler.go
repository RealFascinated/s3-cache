package routes

import (
	"net/http"
	"strings"

	"cc.fascinated/s3-cache/internal"
	"github.com/labstack/echo/v4"
)

// FileHandler handles the incoming file request
func FileHandler(ctx echo.Context) error {
	// Get the full path from the request
	path := ctx.Request().URL.Path
	
	// Remove leading slash if present
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	// Split the path into parts
	parts := strings.Split(path, "/")

	// Get the bucket and key
	bucket := parts[0]
	key := strings.Join(parts[1:], "/")

	// Get file from cache or S3
	file, err := internal.GetFile(bucket, key)
	if err != nil {
		return ctx.String(http.StatusInternalServerError, "Error getting file: "+err.Error())
	}

	return ctx.Blob(http.StatusOK, file.ContentType, file.Data)
}