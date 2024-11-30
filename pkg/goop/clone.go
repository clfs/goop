// Package goop implements functionality for dumping git repositories.
package goop

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/deletescape/goop/internal/utils"
	"github.com/deletescape/goop/internal/workers"
	"github.com/deletescape/jobtracker"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/format/commitgraph"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/storage/filesystem/dotgit"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpproxy"
)

func proxyFromEnv() fasthttp.DialFunc {
	allProxy, okAll := os.LookupEnv("all_proxy")
	httpProxy, okHTTP := os.LookupEnv("http_proxy")
	httpsProxy, okHTTPS := os.LookupEnv("https_proxy")

	uriToDial := func(u string) fasthttp.DialFunc {
		pURI, err := url.Parse(u)
		if err != nil {
			panic(err) // TODO: uh, handle better
		}
		if pURI.Scheme == "socks5" {
			return fasthttpproxy.FasthttpSocksDialer(pURI.Host)
		}
		return fasthttpproxy.FasthttpHTTPDialer(pURI.Host) // this probly doesnt work for proxys with auth rn
	}

	if okAll {
		return uriToDial(allProxy)
	}
	if okHTTP {
		return uriToDial(httpProxy)
	}
	if okHTTPS {
		return uriToDial(httpsProxy)
	}

	return nil
}

var c = &fasthttp.Client{
	Name:            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36",
	MaxConnsPerHost: utils.MaxInt(maxConcurrency+250, fasthttp.DefaultMaxConnsPerHost),
	TLSConfig: &tls.Config{
		InsecureSkipVerify: true,
	},
	NoDefaultUserAgentHeader: true,
	MaxConnWaitTimeout:       10 * time.Second,
	Dial:                     proxyFromEnv(),
}

func CloneList(listFile, baseDir string, force, keep bool) error {
	lf, err := os.Open(listFile)
	if err != nil {
		return err
	}
	defer lf.Close()

	listScan := bufio.NewScanner(lf)
	for listScan.Scan() {
		u := listScan.Text()
		if u == "" {
			continue
		}
		dir := baseDir
		if dir != "" {
			parsed, err := url.Parse(u)
			if err != nil {
				slog.Error("couldn't parse uri", "uri", u, "error", err)
				continue
			}
			dir = utils.URL(dir, parsed.Host)
		}
		slog.Info("starting download", "target", u, "dir", dir, "force", force, "keep", keep)
		if err := Clone(u, dir, force, keep); err != nil {
			slog.Error("download failed", "target", u, "dir", dir, "force", force, "keep", keep, "error", err)
		}
	}
	return nil
}

func Clone(u, dir string, force, keep bool) error {
	baseURL := strings.TrimSuffix(u, "/")
	baseURL = strings.TrimSuffix(baseURL, "/HEAD")
	baseURL = strings.TrimSuffix(baseURL, "/.git")
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return err
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "http"
	}
	baseURL = parsed.String()
	parsed, err = url.Parse(baseURL)
	if err != nil {
		return err
	}
	baseDir := dir
	if baseDir == "" {
		baseDir = parsed.Host
	}

	if utils.Exists(baseDir) {
		if !utils.IsFolder(baseDir) {
			return fmt.Errorf("%s is not a directory", baseDir)
		}
		isEmpty, err := utils.IsEmpty(baseDir)
		if err != nil {
			return err
		}
		if !isEmpty {
			if force {
				if err := os.RemoveAll(baseDir); err != nil {
					return err
				}
			} else if !keep {
				return fmt.Errorf("%s is not empty", baseDir)
			}
		}
	}

	return FetchGit(baseURL, baseDir)
}

func FetchGit(baseURL, baseDir string) error {
	slog.Info("testing for .git/HEAD", "base", baseURL)
	code, body, err := c.Get(nil, utils.URL(baseURL, ".git/HEAD"))
	if err != nil {
		return err
	}

	if code != 200 {
		slog.Warn(".git/HEAD doesn't appear to exist, clone will most likely fail", "base", baseURL, "code", code)
	} else if !bytes.HasPrefix(body, refPrefix) {
		slog.Warn(".git/HEAD doesn't appear to be a git HEAD file, clone will most likely fail", "base", baseURL, "code", code)
	}

	slog.Info("testing if recursive download is possible", "base", baseURL)
	code, body, err = c.Get(body, utils.URL(baseURL, ".git/"))
	if err != nil {
		if utils.IgnoreError(err) {
			slog.Error("ignored error", "base", baseURL, "code", code, "error", err)
		} else {
			return err
		}
	}

	if code == 200 && utils.IsHTML(body) {
		lnk, _ := url.Parse(utils.URL(baseURL, ".git/"))
		indexedFiles, err := utils.GetIndexedFiles(body, lnk.Path)
		if err != nil {
			return err
		}
		if utils.StringsContain(indexedFiles, "HEAD") {
			slog.Info("fetching .git/ recursively", "base", baseURL)
			jt := jobtracker.NewJobTracker(workers.RecursiveDownloadWorker, maxConcurrency, jobtracker.DefaultNapper)
			jt.AddJobs(indexedFiles...)
			jt.StartAndWait(workers.RecursiveDownloadContext{C: c, BaseURL: utils.URL(baseURL, ".git/"), BaseDir: utils.URL(baseDir, ".git/")}, true)

			if err := checkout(baseDir); err != nil {
				slog.Error("failed to checkout", "dir", baseDir, "error", err)
			}
			if err := fetchIgnored(baseDir, baseURL); err != nil {
				return err
			}
		}
	}

	slog.Info("fetching common files", "base", baseURL)
	jt := jobtracker.NewJobTracker(workers.DownloadWorker, maxConcurrency, jobtracker.DefaultNapper)
	jt.AddJobs(commonFiles...)
	jt.StartAndWait(workers.DownloadContext{C: c, BaseDir: baseDir, BaseURL: baseURL}, false)

	slog.Info("finding refs", "base", baseURL)
	jt = jobtracker.NewJobTracker(workers.FindRefWorker, maxConcurrency, jobtracker.DefaultNapper)
	jt.AddJobs(commonRefs...)
	jt.StartAndWait(workers.FindRefContext{C: c, BaseURL: baseURL, BaseDir: baseDir}, true)

	slog.Info("finding packs", "base", baseURL)
	infoPacksPath := utils.URL(baseDir, ".git/objects/info/packs")
	if utils.Exists(infoPacksPath) {
		infoPacks, err := os.ReadFile(infoPacksPath)
		if err != nil {
			return err
		}
		hashes := packRegex.FindAllSubmatch(infoPacks, -1)
		jt = jobtracker.NewJobTracker(workers.DownloadWorker, maxConcurrency, jobtracker.DefaultNapper)
		for _, sha1 := range hashes {
			jt.AddJobs(
				fmt.Sprintf(".git/objects/pack/pack-%s.idx", sha1[1]),
				fmt.Sprintf(".git/objects/pack/pack-%s.pack", sha1[1]),
				fmt.Sprintf(".git/objects/pack/pack-%s.rev", sha1[1]),
			)
		}
		jt.StartAndWait(workers.DownloadContext{C: c, BaseURL: baseURL, BaseDir: baseDir}, false)
	}

	slog.Info("finding objects", "base", baseURL)
	objs := make(map[string]bool) // object "set"
	//var packed_objs [][]byte

	files := []string{
		utils.URL(baseDir, ".git/packed-refs"),
		utils.URL(baseDir, ".git/info/refs"),
		utils.URL(baseDir, ".git/info/grafts"),
		// utils.Url(baseDir, ".git/info/sparse-checkout"), // TODO: ?
		utils.URL(baseDir, ".git/FETCH_HEAD"),
		utils.URL(baseDir, ".git/ORIG_HEAD"),
		utils.URL(baseDir, ".git/HEAD"),
		utils.URL(baseDir, ".git/objects/loose-object-idx"), // TODO: is this even a text file?
		utils.URL(baseDir, ".git/objects/info/commit-graphs/commit-graph-chain"),
		utils.URL(baseDir, ".git/objects/info/alternates"),
		utils.URL(baseDir, ".git/objects/info/http-alternates"),
	}

	// TODO : fix if-else hell in the entire object hash collection code (and get rid of bad early returns)

	gitRefsDir := utils.URL(baseDir, ".git/refs")
	if utils.Exists(gitRefsDir) {
		if err := filepath.Walk(gitRefsDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				files = append(files, path)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	gitLogsDir := utils.URL(baseDir, ".git/logs")
	if utils.Exists(gitLogsDir) {
		refLogPrefix := utils.URL(gitLogsDir, "refs") + "/"
		if err := filepath.Walk(gitLogsDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				files = append(files, path)

				if strings.HasPrefix(path, refLogPrefix) {
					refName := strings.TrimPrefix(path, refLogPrefix)
					filePath := utils.URL(gitRefsDir, refName)
					if !utils.Exists(filePath) {
						slog.Info("generating ref file", "dir", baseDir, "ref", refName)
						content, err := os.ReadFile(path)
						if err != nil {
							slog.Error("couldn't read reflog file", "dir", baseDir, "ref", refName, "error", err)
							return nil
						}

						// Find the last reflog entry and extract the obj hash and write that to the ref file
						logObjs := refLogRegex.FindAllSubmatch(content, -1)
						lastEntryObj := logObjs[len(logObjs)-1][1]

						if err := utils.CreateParentFolders(filePath); err != nil {
							slog.Error("couldn't create parent directories", "file", filePath, "error", err)
							return nil
						}

						if err := os.WriteFile(filePath, lastEntryObj, os.ModePerm); err != nil {
							slog.Error("couldn't write to file", "file", filePath, "error", err)
						}
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, f := range files {
		if !utils.Exists(f) {
			continue
		}

		content, err := os.ReadFile(f)
		if err != nil {
			slog.Error("couldn't read reflog file", "file", f, "error", err)
			return err
		}

		for _, obj := range objRegex.FindAll(content, -1) {
			objs[strings.TrimSpace(string(obj))] = true
		}
	}

	indexPath := utils.URL(baseDir, ".git/index")
	if utils.Exists(indexPath) {
		f, err := os.Open(indexPath)
		if err != nil {
			return err
		}
		defer f.Close()
		var idx index.Index
		decoder := index.NewDecoder(f)
		if err := decoder.Decode(&idx); err != nil {
			slog.Error("couldn't decode git index", "dir", baseDir, "error", err)
		}
		for _, entry := range idx.Entries {
			objs[entry.Hash.String()] = true
		}
	}

	objStorage := filesystem.NewObjectStorage(dotgit.New(osfs.New(utils.URL(baseDir, ".git"))), &cache.ObjectLRU{MaxSize: 256})
	if err := objStorage.ForEachObjectHash(func(hash plumbing.Hash) error {
		objs[hash.String()] = true
		encObj, err := objStorage.EncodedObject(plumbing.AnyObject, hash)
		if err != nil {
			return err

		}
		decObj, err := object.DecodeObject(objStorage, encObj)
		if err != nil {
			return err
		}
		for _, hash := range utils.GetReferencedHashes(decObj) {
			objs[hash] = true
		}
		return nil
	}); err != nil {
		slog.Error("error while processing object files", "dir", baseDir, "error", err)
	}

	// Parse stand alone commit graph file
	parseGraphFile(baseDir, utils.URL(baseDir, ".git/objects/info/commit-graph"), objs)

	// Parse commit graph chains
	commitGraphList := utils.URL(baseDir, ".git/objects/info/commit-graphs/commit-graph-chain")
	if utils.Exists(commitGraphList) {
		var graphFiles []string
		jt = jobtracker.NewJobTracker(workers.DownloadWorker, maxConcurrency, jobtracker.DefaultNapper)
		f, err := os.Open(commitGraphList)
		if err != nil {
			slog.Error("failed to open commit graph chain", "dir", baseDir, "error", err)
		} else {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if !strings.HasPrefix(line, "#") {
					graphFile := fmt.Sprintf(".git/objects/info/commit-graphs/graph-%s.graph", line)
					graphFiles = append(graphFiles, graphFile)
					jt.AddJob(graphFile)
				}
			}
		}
		jt.StartAndWait(workers.DownloadContext{C: c, BaseDir: baseDir, BaseURL: baseURL}, false)
		for _, graphFile := range graphFiles {
			parseGraphFile(baseDir, utils.URL(baseDir, graphFile), objs)
		}
	}

	// TODO: find more objects to fetch in pack files and remove packed objects from list of objects to be fetched
	/* packs, err := objStorage.ObjectPacks()
	// TODO: handle error
	if err == nil {
		for _, pack := range packs {
			pf := utils.Url(baseDir, fmt.Sprintf(".git/objects/pack/pack-%s.pack", pack))
			r, err := os.Open(pf)
			if err != nil {
				log.Error().Str("dir", baseDir).Str("pack", pf).Err(err).Msg("failed to open pack file")
				continue
			}
			sc := packfile.NewScanner(r)
			for {
				oh, err := sc.NextObjectHeader()
				if err != nil {
					log.Error().Str("dir", baseDir).Str("pack", pf).Err(err).Msg("error while parsing pack file")
					break
				}
			}
		}
	} */

	slog.Info("fetching objects", "base", baseURL)
	jt = jobtracker.NewJobTracker(workers.FindObjectsWorker, maxConcurrency, jobtracker.DefaultNapper)
	for obj := range objs {
		jt.AddJob(obj)
	}
	jt.StartAndWait(workers.FindObjectsContext{C: c, BaseURL: baseURL, BaseDir: baseDir, Storage: objStorage}, true)

	// exit early if we haven't managed to dump anything
	if !utils.Exists(baseDir) {
		return nil
	}

	fetchMissing(baseDir, baseURL, objStorage)

	// TODO: disable lfs in checkout (for now lfs support depends on lfs NOT being setup on the system you use goop on)
	if err := checkout(baseDir); err != nil {
		slog.Error("failed to checkout", "dir", baseDir, "error", err)
	}

	// <fetch lfs objects and manually check them out>
	fetchLfs(baseDir, baseURL)

	if err := fetchIgnored(baseDir, baseURL); err != nil {
		return err
	}

	return nil
}

func checkout(baseDir string) error {
	slog.Info("running git checkout .", "dir", baseDir)
	cmd := exec.Command("git", "checkout", ".")
	cmd.Dir = baseDir
	return cmd.Run()
}

func fetchLfs(baseDir, baseURL string) {
	attrPath := utils.URL(baseDir, ".gitattributes")
	if utils.Exists(attrPath) {
		slog.Info("attempting to fetch potential git lfs objects", "dir", baseDir)
		f, err := os.Open(attrPath)
		if err != nil {
			slog.Error("couldn't read git attributes", "dir", baseDir, "error", err)
			return
		}
		defer f.Close()

		var globalFilters []string
		var filters []string
		var files []string
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.Contains(line, "filter=lfs") {
				pattern := strings.SplitN(line, " ", 2)[0]
				if strings.ContainsRune(pattern, '*') {
					if strings.ContainsRune(pattern, '/') {
						globalFilters = append(globalFilters, pattern)
					} else {
						filters = append(filters, pattern)
					}
				} else {
					files = append(files, pattern)
				}
			}
		}
		if err := scanner.Err(); err != nil {
			slog.Error("error while parsing git attributes file", "dir", baseDir, "error", err)
		}

		var hashes []string
		readStub := func(fp string) {
			f, err := os.Open(fp)
			if err != nil {
				slog.Error("couldn't open lfs stub file", "file", fp, "error", err)
				return
			}
			defer f.Close()
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				fmt.Println(line)
				if strings.HasPrefix(line, "oid ") {
					hash := strings.SplitN(line, " ", 2)[1]
					hash = strings.SplitN(hash, ":", 2)[1]
					hashes = append(hashes, hash)
					break
				}
			}
			if err := scanner.Err(); err != nil {
				slog.Error("error while parsing lfs stub file", "file", fp, "error", err)
			}
		}

		for _, file := range files {
			fp := utils.URL(baseDir, file)
			if utils.Exists(fp) {
				readStub(fp)
			}
		}

		err = filepath.Walk(baseDir,
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				for _, filter := range filters {
					match, err := filepath.Match(filter, filepath.Base(path))
					if err != nil {
						slog.Error("failed to apply filter", "dir", baseDir, "filter", filter, "error", err)
						continue
					}
					if match {
						readStub(path)
					}
				}
				return nil
			})
		if err != nil {
			slog.Error("error while testing git lfs filters", "dir", baseDir, "error", err)
		}

		// TODO: global filters
		_ = globalFilters

		jt := jobtracker.NewJobTracker(workers.DownloadWorker, maxConcurrency, jobtracker.DefaultNapper)
		for _, hash := range hashes {
			jt.AddJob(fmt.Sprintf(".git/lfs/objects/%s/%s/%s", hash[:2], hash[2:4], hash))
		}
		jt.StartAndWait(workers.DownloadContext{C: c, BaseURL: baseURL, BaseDir: baseDir}, false)
	}
}

// Iterate over index to find missing files
func fetchMissing(baseDir, baseURL string, objStorage *filesystem.ObjectStorage) {
	indexPath := utils.URL(baseDir, ".git/index")
	if utils.Exists(indexPath) {
		slog.Info("attempting to fetch potentially missing files", "base", baseURL, "dir", baseDir)

		var missingFiles []string
		var idx index.Index
		f, err := os.Open(indexPath)
		if err != nil {
			slog.Error("couldn't read git index", "dir", baseDir, "error", err)
			return
		}
		defer f.Close()
		decoder := index.NewDecoder(f)
		if err := decoder.Decode(&idx); err != nil {
			slog.Error("couldn't decode git index", "dir", baseDir, "error", err)
			return
		} else {
			jt := jobtracker.NewJobTracker(workers.DownloadWorker, maxConcurrency, jobtracker.DefaultNapper)
			for _, entry := range idx.Entries {
				if !strings.HasSuffix(entry.Name, ".php") && !utils.Exists(utils.URL(baseDir, fmt.Sprintf(".git/objects/%s/%s", entry.Hash.String()[:2], entry.Hash.String()[2:]))) {
					missingFiles = append(missingFiles, entry.Name)
					jt.AddJob(entry.Name)
				}
			}
			jt.StartAndWait(workers.DownloadContext{C: c, BaseURL: baseURL, BaseDir: baseDir, AllowHTML: true, AlllowEmpty: true}, false)

			jt = jobtracker.NewJobTracker(workers.CreateObjectWorker, maxConcurrency, jobtracker.DefaultNapper)
			for _, f := range missingFiles {
				if utils.Exists(utils.URL(baseDir, f)) {
					jt.AddJob(f)
				}
			}
			jt.StartAndWait(workers.CreateObjectContext{BaseDir: baseDir, Storage: objStorage, Index: &idx}, false)
		}
	}
}

func fetchIgnored(baseDir, baseURL string) error {
	ignorePath := utils.URL(baseDir, ".gitignore")
	if utils.Exists(ignorePath) {
		slog.Info("attempting to fetch ignored files", "base", baseDir)

		ignoreFile, err := os.Open(ignorePath)
		if err != nil {
			return err
		}
		defer ignoreFile.Close()

		jt := jobtracker.NewJobTracker(workers.DownloadWorker, maxConcurrency, jobtracker.DefaultNapper)

		scanner := bufio.NewScanner(ignoreFile)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			commentStrip := strings.SplitN(line, "#", 1)
			line = commentStrip[0]
			if line == "" || strings.HasPrefix(line, "!") || strings.HasSuffix(line, "/") || strings.ContainsRune(line, '*') || strings.HasSuffix(line, ".php") || strings.HasPrefix(line, "#") {
				continue
			}
			jt.AddJob(line)
		}

		if err := scanner.Err(); err != nil {
			return err
		}

		jt.StartAndWait(workers.DownloadContext{C: c, BaseURL: baseURL, BaseDir: baseDir, AllowHTML: true, AlllowEmpty: true}, false)
	}
	return nil
}

func parseGraphFile(baseDir, graphFile string, objs map[string]bool) {
	if utils.Exists(graphFile) {
		f, err := os.Open(graphFile)
		if err != nil {
			slog.Error("couldn't open commit graph", "dir", baseDir, "graph", graphFile, "error", err)
			return
		}
		graph, err := commitgraph.OpenFileIndex(f)
		if err != nil {
			slog.Error("failed to decode commit graph", "dir", baseDir, "graph", graphFile, "error", err)
			return
		}
		for _, hash := range graph.Hashes() {
			objs[hash.String()] = true
			i, err := graph.GetIndexByHash(hash)
			if err != nil {
				slog.Error("failed to get index from graph", "dir", baseDir, "graph", graphFile, "commit", hash.String(), "error", err)
				continue
			}
			data, err := graph.GetCommitDataByIndex(i)
			if err != nil {
				slog.Error("failed to get commit data from graph", "dir", baseDir, "graph", graphFile, "commit", hash.String(), "error", err)
				continue
			}
			objs[data.TreeHash.String()] = true

		}
	}

}
