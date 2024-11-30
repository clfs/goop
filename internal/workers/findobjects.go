package workers

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/deletescape/goop/internal/utils"
	"github.com/deletescape/jobtracker"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/valyala/fasthttp"
)

var checkedObjs = make(map[string]bool)
var checkedObjsMutex sync.Mutex

type FindObjectsContext struct {
	C       *fasthttp.Client
	BaseURL string
	BaseDir string
	Storage *filesystem.ObjectStorage
}

func FindObjectsWorker(jt *jobtracker.JobTracker, obj string, context jobtracker.Context) {
	c := context.(FindObjectsContext)

	checkRatelimted()

	if obj == "" {
		return
	}

	checkedObjsMutex.Lock()
	if checked, ok := checkedObjs[obj]; checked && ok {
		// Obj has already been checked
		checkedObjsMutex.Unlock()
		return
	} else {
		checkedObjs[obj] = true
	}
	checkedObjsMutex.Unlock()

	file := fmt.Sprintf(".git/objects/%s/%s", obj[:2], obj[2:])
	fullPath := utils.URL(c.BaseDir, file)
	if utils.Exists(fullPath) {
		slog.Info("already fetched, skipping redownload", "obj", obj)
		encObj, err := c.Storage.EncodedObject(plumbing.AnyObject, plumbing.NewHash(obj))
		if err != nil {
			slog.Error("couldn't read object", "obj", obj, "error", err)
			return
		}
		decObj, err := object.DecodeObject(c.Storage, encObj)
		if err != nil {
			slog.Error("couldn't decode object", "obj", obj, "error", err)
			return
		}
		referencedHashes := utils.GetReferencedHashes(decObj)
		for _, h := range referencedHashes {
			jt.AddJob(h)
		}
		return
	}

	uri := utils.URL(c.BaseURL, file)
	code, body, err := c.C.Get(nil, uri)
	if err == nil && code != 200 {
		if code == 429 {
			setRatelimited()
			jt.AddJob(obj)
			return
		}
		slog.Warn("failed to fetch object", "obj", obj, "code", code)
		return
	} else if err != nil {
		slog.Error("failed to fetch object", "obj", obj, "code", code, "error", err)
		return
	}

	if utils.IsHTML(body) {
		slog.Warn("file appears to be html, skipping", "uri", uri)
		return
	}
	if utils.IsEmptyBytes(body) {
		slog.Warn("file appears to be empty, skipping", "uri", uri)
		return
	}
	if err := utils.CreateParentFolders(fullPath); err != nil {
		slog.Error("couldn't create parent directories", "uri", uri, "file", fullPath, "error", err)
		return
	}
	if err := os.WriteFile(fullPath, body, os.ModePerm); err != nil {
		slog.Error("couldn't write file", "uri", uri, "file", fullPath, "error", err)
		return
	}

	slog.Info("fetched object", "obj", obj)

	encObj, err := c.Storage.EncodedObject(plumbing.AnyObject, plumbing.NewHash(obj))
	if err != nil {
		slog.Error("couldn't read object", "obj", obj, "error", err)
		return
	}
	decObj, err := object.DecodeObject(c.Storage, encObj)
	if err != nil {
		slog.Error("couldn't decode object", "obj", obj, "error", err)
		return
	}
	referencedHashes := utils.GetReferencedHashes(decObj)
	for _, h := range referencedHashes {
		jt.AddJob(h)
	}
}
