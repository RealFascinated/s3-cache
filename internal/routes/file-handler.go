package routes

import (
	"fmt"
	"net/http"
	"strconv"
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

	if bucket == "" || key == "" {
		return ctx.String(http.StatusBadRequest, "No file found :(")
	}

	// Parse range header
	rangeHeader := ctx.Request().Header.Get("Range")
	var start, end int64 = -1, -1
	
	if rangeHeader != "" {
		// Parse "bytes=start-end" format
		if strings.HasPrefix(rangeHeader, "bytes=") {
			rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
			parts := strings.Split(rangeStr, "-")
			if len(parts) == 2 {
				if parts[0] != "" {
					if s, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
						start = s
					}
				}
				if parts[1] != "" {
					if e, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
						end = e
					}
				}
			}
		}
	}

	// Get file from cache or S3 with range support
	file, err := internal.GetFileWithRange(bucket, key, start, end)
	if err != nil {
		return ctx.String(http.StatusInternalServerError, err.Error())
	}

	// Set appropriate headers
	ctx.Response().Header().Set("Content-Type", file.ContentType)
	ctx.Response().Header().Set("Accept-Ranges", "bytes")
	
	if file.IsPartial {
		// Return partial content response
		ctx.Response().Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", file.Start, file.End, file.TotalSize))
		ctx.Response().Header().Set("Content-Length", strconv.FormatInt(file.End-file.Start+1, 10))
		return ctx.Blob(http.StatusPartialContent, file.ContentType, file.Data)
	}

	return ctx.Blob(http.StatusOK, file.ContentType, file.Data)
}