package index

import (
	"github.com/ozkatz/cloudzip/pkg/mount/fs"
	"os"
	"path"
	"strings"
	"sync"
)

type Tree interface {
	// Index accepts a sorted list of paths.
	// it creates a mapping of directory memberships and makes up for missing directory entries
	// (some indices have no directory indices at all, so those need to be created)
	Index(infos []*fs.FileInfo)

	// Readdir returns the direct descendants of the given directory at entryPath
	Readdir(entryPath string) (fs.FileInfoList, error)

	// Stat returns in the FileInfo for the given file/directory at entryPath
	Stat(entryPath string) (*fs.FileInfo, error)
}

// helpers
type DirInfoGenerator func(filename string) *fs.FileInfo

// InMemoryTreeBuilder maintains a tree in memory
type InMemoryTreeBuilder struct {
	files       map[string]*fs.FileInfo
	dirs        map[string][]*fs.FileInfo
	directoryFn DirInfoGenerator
	l           *sync.Mutex
}

var _ Tree = &InMemoryTreeBuilder{}

func NewInMemoryTreeBuilder(directoryFn DirInfoGenerator) *InMemoryTreeBuilder {
	return &InMemoryTreeBuilder{
		files:       make(map[string]*fs.FileInfo),
		dirs:        make(map[string][]*fs.FileInfo),
		directoryFn: directoryFn,
		l:           &sync.Mutex{},
	}
}

func (t *InMemoryTreeBuilder) Index(infos []*fs.FileInfo) {
	t.l.Lock()
	defer t.l.Unlock()

	addedToParent := make(map[string]bool)
	for _, info := range infos {
		//depth := 0
		parts := DirParts(info.Name())
		for i, part := range parts {
			// current file, not its parents
			isLastEntry := i == len(parts)-1
			// determine file info for part
			var currentInfo *fs.FileInfo
			if !isLastEntry {
				currentInfo = t.directoryFn(part)
			} else {
				currentInfo = info
			}

			// add to parent directory
			if i > 0 { // we have a parent
				parent := parts[i-1]
				// if not already added to the parent
				if _, ok := addedToParent[part]; !ok {
					t.dirs[parent] = append(t.dirs[parent], currentInfo)
					addedToParent[part] = true
				}
			}
			// add to files
			t.files[part] = currentInfo
		}

	}
	// done!
	return
}

func DirParts(p string) []string {
	p = strings.Trim(p, fs.Delimiter)
	if p == "" || p == "." {
		return []string{""}
	}
	parts := []string{""}
	accum := ""
	for _, part := range strings.Split(p, fs.Delimiter) {
		parts = append(parts, accum+part)
		accum = accum + part + fs.Delimiter
	}
	return parts
}

func (t *InMemoryTreeBuilder) Readdir(entryPath string) (fs.FileInfoList, error) {
	t.l.Lock()
	defer t.l.Unlock()
	entryPath = strings.Trim(entryPath, fs.Delimiter)
	entries, dirExists := t.dirs[entryPath]
	if !dirExists {
		return nil, os.ErrNotExist
	}
	// ReadDir returns paths relative to the read directory, not absolute paths
	relativeNamedEntries := make(fs.FileInfoList, len(entries))
	for i, entry := range entries {
		relativeNamedEntries[i] = entry.AsPath(path.Base(entry.FullPath))
	}
	return relativeNamedEntries, nil
}

func (t *InMemoryTreeBuilder) Stat(entryPath string) (*fs.FileInfo, error) {
	t.l.Lock()
	defer t.l.Unlock()
	entryPath = strings.Trim(entryPath, fs.Delimiter)
	stat, ok := t.files[entryPath]
	if !ok {
		return nil, os.ErrNotExist
	}
	return stat, nil
}
