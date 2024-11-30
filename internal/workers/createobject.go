package workers

import (
	"log/slog"
	"os"

	"github.com/deletescape/goop/internal/utils"
	"github.com/deletescape/jobtracker"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/storage/filesystem"
)

type CreateObjectContext struct {
	BaseDir string
	Storage *filesystem.ObjectStorage
	Index   *index.Index
}

func CreateObjectWorker(jt *jobtracker.JobTracker, f string, context jobtracker.Context) {
	c := context.(CreateObjectContext)

	fp := utils.URL(c.BaseDir, f)

	entry, err := c.Index.Entry(f)
	if err != nil {
		slog.Error("file is not in index", "file", f, "error", err)
		return
	}

	fMode, err := entry.Mode.ToOSFileMode()
	if err != nil {
		slog.Warn("failed to set filemode", "file", f, "error", err)
	} else {
		os.Chmod(fp, fMode)
	}
	os.Chown(fp, int(entry.UID), int(entry.GID))
	os.Chtimes(fp, entry.ModifiedAt, entry.ModifiedAt)
	slog.Debug("updated from index", "file", f)

	content, err := os.ReadFile(fp)
	if err != nil {
		slog.Error("failed to read file", "file", f, "error", err)
		return
	}

	hash := plumbing.ComputeHash(plumbing.BlobObject, content)
	if entry.Hash != hash {
		slog.Warn("hash does not match hash in index, skipping object creation", "file", f)
		return
	}

	obj := c.Storage.NewEncodedObject()
	obj.SetSize(int64(len(content)))
	obj.SetType(plumbing.BlobObject)

	ow, err := obj.Writer()
	if err != nil {
		slog.Error("failed to create object writer", "file", f, "error", err)
		return
	}
	defer ow.Close()
	ow.Write(content)

	_, err = c.Storage.SetEncodedObject(obj)
	if err != nil {
		slog.Error("failed to create object", "file", f, "error", err)
		return
	}
	slog.Debug("object created", "file", f)
}
