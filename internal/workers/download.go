package workers

import (
	"log/slog"
	"os"

	"github.com/deletescape/goop/internal/utils"
	"github.com/deletescape/jobtracker"
	"github.com/valyala/fasthttp"
)

type DownloadContext struct {
	C           *fasthttp.Client
	BaseURL     string
	BaseDir     string
	AllowHTML   bool
	AlllowEmpty bool
}

func DownloadWorker(jt *jobtracker.JobTracker, file string, context jobtracker.Context) {
	c := context.(DownloadContext)
	checkRatelimted()

	targetFile := utils.URL(c.BaseDir, file)
	if utils.Exists(targetFile) {
		slog.Info("already fetched, skipping redownload", "file", targetFile)
		return
	}
	uri := utils.URL(c.BaseURL, file)
	code, body, err := c.C.Get(nil, uri)
	if err == nil && code != 200 {
		if code == 429 {
			setRatelimited()
			jt.AddJob(file)
			return
		}
		slog.Warn("couldn't fetch file", "uri", uri, "code", code)
		return
	} else if err != nil {
		slog.Error("couldn't fetch file", "uri", uri, "code", code, "error", err)
		return
	}

	if !c.AllowHTML && utils.IsHTML(body) {
		slog.Warn("file appears to be html, skipping", "uri", uri)
		return
	}
	if !c.AlllowEmpty && utils.IsEmptyBytes(body) {
		slog.Warn("file appears to be empty, skipping", "uri", uri)
		return
	}
	if err := utils.CreateParentFolders(targetFile); err != nil {
		slog.Error("couldn't create parent directories", "uri", uri, "file", targetFile, "error", err)
		return
	}
	if err := os.WriteFile(targetFile, body, os.ModePerm); err != nil {
		slog.Error("couldn't write file", "uri", uri, "file", targetFile, "error", err)
		return
	}
	slog.Info("fetched file", "uri", uri, "file", file)
}
