package workers

import (
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/deletescape/goop/internal/utils"
	"github.com/deletescape/jobtracker"
	"github.com/valyala/fasthttp"
)

type RecursiveDownloadContext struct {
	C       *fasthttp.Client
	BaseURL string
	BaseDir string
}

func RecursiveDownloadWorker(jt *jobtracker.JobTracker, f string, context jobtracker.Context) {
	c := context.(RecursiveDownloadContext)

	checkRatelimted()

	filePath := utils.URL(c.BaseDir, f)
	isDir := strings.HasSuffix(f, "/")
	if !isDir && utils.Exists(filePath) {
		slog.Info("already fetched, skipping redownload", "file", filePath)
		return
	}
	uri := utils.URL(c.BaseURL, f)
	code, body, err := c.C.Get(nil, uri)
	if err == nil && code != 200 {
		if code == 429 {
			setRatelimited()
			jt.AddJob(f)
			return
		}
		slog.Warn("failed to fetch file", "uri", uri, "code", code)
		return
	} else if err != nil {
		slog.Error("failed to fetch file", "uri", uri, "code", code, "error", err)
		return
	}

	if isDir {
		if !utils.IsHTML(body) {
			slog.Warn("not a directory index, skipping", "uri", uri)
			return
		}

		lnk, _ := url.Parse(uri)
		indexedFiles, err := utils.GetIndexedFiles(body, lnk.Path)
		if err != nil {
			slog.Error("couldn't get list of indexed files", "uri", uri, "error", err)
			return
		}
		slog.Info("fetched directory listing", "uri", uri)
		for _, idxf := range indexedFiles {
			jt.AddJob(utils.URL(f, idxf))
		}
	} else {
		if err := utils.CreateParentFolders(filePath); err != nil {
			slog.Error("couldn't create parent directories", "file", filePath, "error", err)
			return
		}
		if err := os.WriteFile(filePath, body, os.ModePerm); err != nil {
			slog.Error("couldn't write file", "file", filePath, "error", err)
			return
		}
		slog.Info("fetched file", "uri", uri)
	}
}
