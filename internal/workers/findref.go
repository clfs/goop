package workers

import (
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/deletescape/goop/internal/utils"
	"github.com/deletescape/jobtracker"
	"github.com/valyala/fasthttp"
	"gopkg.in/ini.v1"
)

var refRegex = regexp.MustCompile(`(?m)(refs(/[a-zA-Z0-9\-\.\_\*]+)+)`)
var branchRegex = regexp.MustCompile(`(?m)branch ["'](.+)["']`)

var checkedRefs = make(map[string]bool)
var checkedRefsMutex sync.Mutex

type FindRefContext struct {
	C       *fasthttp.Client
	BaseURL string
	BaseDir string
}

func FindRefWorker(jt *jobtracker.JobTracker, path string, context jobtracker.Context) {
	c := context.(FindRefContext)

	checkRatelimted()

	checkedRefsMutex.Lock()
	if checked, ok := checkedRefs[path]; checked && ok {
		// Ref has already been checked
		checkedRefsMutex.Unlock()
		return
	} else {
		checkedRefs[path] = true
	}
	checkedRefsMutex.Unlock()

	targetFile := utils.URL(c.BaseDir, path)
	if utils.Exists(targetFile) {
		slog.Info("already fetched, skipping redownload", "file", targetFile)
		content, err := os.ReadFile(targetFile)
		if err != nil {
			slog.Error("error while reading file", "file", targetFile, "error", err)
			return
		}
		for _, ref := range refRegex.FindAll(content, -1) {
			jt.AddJob(utils.URL(".git", string(ref)))
			jt.AddJob(utils.URL(".git/logs", string(ref)))
		}
		if path == ".git/FETCH_HEAD" {
			// TODO figure out actual remote instead of just assuming origin here (if possible)
			for _, branch := range branchRegex.FindAllSubmatch(content, -1) {
				jt.AddJob(fmt.Sprintf(".git/refs/remotes/origin/%s", branch[1]))
				jt.AddJob(fmt.Sprintf(".git/logs/refs/remotes/origin/%s", branch[1]))
			}
		}
		if path == ".git/config" || path == ".git/config.worktree" {
			cfg, err := ini.Load(content)
			if err != nil {
				slog.Error("failed to parse git config", "file", targetFile, "error", err)
				return
			}
			for _, sec := range cfg.Sections() {
				if strings.HasPrefix(sec.Name(), "branch ") {
					parts := strings.SplitN(sec.Name(), " ", 2)
					branch := strings.Trim(parts[1], `"`)
					remote := sec.Key("remote").String()

					jt.AddJob(fmt.Sprintf(".git/refs/remotes/%s/%s", remote, branch))
					jt.AddJob(fmt.Sprintf(".git/logs/refs/remotes/%s/%s", remote, branch))
				}
			}
		}
		return
	}

	uri := utils.URL(c.BaseURL, path)
	code, body, err := c.C.Get(nil, uri)
	if err == nil && code != 200 {
		if code == 429 {
			setRatelimited()
			jt.AddJob(path)
			return
		}
		slog.Warn("failed to fetch ref", "uri", uri, "code", code)
		return
	} else if err != nil {
		slog.Error("failed to fetch ref", "uri", uri, "error", err)
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
	if err := utils.CreateParentFolders(targetFile); err != nil {
		slog.Error("couldn't create parent directories", "uri", uri, "file", targetFile, "error", err)
		return
	}
	if err := os.WriteFile(targetFile, body, os.ModePerm); err != nil {
		slog.Error("couldn't write file", "uri", uri, "file", targetFile, "error", err)
		return
	}

	slog.Info("fetched ref", "uri", uri)

	for _, ref := range refRegex.FindAll(body, -1) {
		jt.AddJob(utils.URL(".git", string(ref)))
		jt.AddJob(utils.URL(".git/logs", string(ref)))
	}
	if path == ".git/FETCH_HEAD" {
		// TODO figure out actual remote instead of just assuming origin here (if possible)
		for _, branch := range branchRegex.FindAllSubmatch(body, -1) {
			jt.AddJob(fmt.Sprintf(".git/refs/remotes/origin/%s", branch[1]))
			jt.AddJob(fmt.Sprintf(".git/logs/refs/remotes/origin/%s", branch[1]))
		}
	}
	if path == ".git/config" || path == ".git/config.worktree" {
		cfg, err := ini.Load(body)
		if err != nil {
			slog.Error("failed to parse git config", "file", targetFile, "error", err)
			return
		}
		for _, sec := range cfg.Sections() {
			if strings.HasPrefix(sec.Name(), "branch ") {
				parts := strings.SplitN(sec.Name(), " ", 2)
				branch := strings.Trim(parts[1], `"`)
				remote := sec.Key("remote").String()

				jt.AddJob(fmt.Sprintf(".git/refs/remotes/%s/%s", remote, branch))
				jt.AddJob(fmt.Sprintf(".git/logs/refs/remotes/%s/%s", remote, branch))
			}
		}
	}
}
