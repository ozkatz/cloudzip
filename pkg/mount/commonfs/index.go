package commonfs

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
)

var (
	ErrInvalidInput = errors.New("invalid input")
)

type Tree interface {
	// Index accepts a sorted list of paths.
	// it creates a mapping of directory memberships and makes up for missing directory entries, if any.
	// (some indices have no directory entries at all, so those need to be created)
	Index(infos []*FileInfo) error

	// Readdir returns the direct descendants of the given directory at entryPath
	Readdir(entryPath string) (FileInfoList, error)

	// Stat returns in the FileInfo for the given file/directory at entryPath
	Stat(entryPath string) (*FileInfo, error)
}

type DirInfoGenerator func(filename string) *FileInfo

// InMemoryTreeBuilder maintains a tree in memory
type InMemoryTreeBuilder struct {
	files       map[string]*FileInfo
	dirs        map[string][]*FileInfo
	directoryFn DirInfoGenerator
	l           *sync.Mutex
}

var _ Tree = &InMemoryTreeBuilder{}

func NewInMemoryTreeBuilder(directoryFn DirInfoGenerator) *InMemoryTreeBuilder {
	return &InMemoryTreeBuilder{
		files:       make(map[string]*FileInfo),
		dirs:        make(map[string][]*FileInfo),
		directoryFn: directoryFn,
		l:           &sync.Mutex{},
	}
}

func (t *InMemoryTreeBuilder) Index(infos []*FileInfo) error {
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
			var currentInfo *FileInfo
			if !isLastEntry {
				currentInfo = t.directoryFn(part)
			} else {
				currentInfo = info
			}

			explicitDirectory := isLastEntry && currentInfo.IsDir()

			// add to files
			_, fileRegistered := t.files[part]
			if explicitDirectory || !fileRegistered {
				t.files[part] = currentInfo
			}

			// add to parent directory
			if i > 0 { // we have a parent
				parent := parts[i-1]
				// if not already added to the parent
				_, alreadyAdded := addedToParent[part]
				if alreadyAdded && explicitDirectory {
					return fmt.Errorf("%w: entries should be sorted", ErrInvalidInput)
				} else if !alreadyAdded {
					t.dirs[parent] = append(t.dirs[parent], currentInfo)
					addedToParent[part] = true
				}
			}
		}
	}
	// done!
	return fsck("", t.files, t.dirs) // starting with root
}

var (
	ErrIntegrityError = errors.New("integrity error")
)

func fsck(currentPath string, files map[string]*FileInfo, dirs map[string][]*FileInfo) error {
	file, hasFile := files[currentPath]
	if !hasFile {
		return fmt.Errorf("%w: could not find file entry for '%s'", ErrIntegrityError, currentPath)
	}
	// non-root entry should have a parent.
	// that parent should contain this current path
	// the entry in the parent should have the same ID
	if currentPath != "" {
		parentDir := path.Dir(currentPath)
		if parentDir == "." { // children of root
			parentDir = ""
		}
		parentData, hasParent := dirs[parentDir]
		if !hasParent {
			return fmt.Errorf("%w: could not locate parent dir '%s' for '%s'",
				ErrIntegrityError, parentDir, currentPath)
		}
		var entryInParentDir *FileInfo
		for _, child := range parentData {
			if child.Name() == file.Name() {
				entryInParentDir = child
				break
			}
		}
		// can't find an entry with my name in my parent's listing
		if entryInParentDir == nil {
			return fmt.Errorf("%w: no dir entry in parent '%s' of file '%s'",
				ErrIntegrityError, parentDir, currentPath)
		}
		// parent's listing has a different ID from my entry
		if entryInParentDir.FileID() != file.FileID() {
			return fmt.Errorf("%w: dir entry for '%s' in parent '%s' has fileId=%d, file entry has fileId=%d",
				ErrIntegrityError, currentPath, parentDir, file.FileID(), entryInParentDir.FileID())
		}
	}

	// if we're a dir, iterate over our children, fsck'ing them
	// if they have subdirectories, fsck them too.
	if file.IsDir() {
		dirFiles, hasDirFiles := dirs[currentPath]
		if !hasDirFiles {
			return fmt.Errorf("%w: file '%s' is a directory, but no directory index fouud",
				ErrIntegrityError, currentPath)
		}
		for _, entry := range dirFiles {
			err := fsck(entry.FullPath(), files, dirs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func DirParts(p string) []string {
	p = strings.Trim(p, Delimiter)
	if p == "" || p == "." {
		return []string{""}
	}
	parts := []string{""}
	accum := ""
	for _, part := range strings.Split(p, Delimiter) {
		parts = append(parts, accum+part)
		accum = accum + part + Delimiter
	}
	return parts
}

func (t *InMemoryTreeBuilder) Readdir(entryPath string) (FileInfoList, error) {
	t.l.Lock()
	defer t.l.Unlock()
	entryPath = strings.Trim(entryPath, Delimiter)
	entries, dirExists := t.dirs[entryPath]
	if !dirExists {
		return nil, os.ErrNotExist
	}
	// ReadDir returns paths relative to the read directory, not absolute paths
	relativeNamedEntries := make(FileInfoList, len(entries))
	for i, entry := range entries {
		relativeNamedEntries[i] = entry.AsPath(path.Base(entry.FullPath()))
	}
	return relativeNamedEntries, nil
}

func (t *InMemoryTreeBuilder) Stat(entryPath string) (*FileInfo, error) {
	t.l.Lock()
	defer t.l.Unlock()
	entryPath = strings.Trim(entryPath, Delimiter)
	stat, ok := t.files[entryPath]
	if !ok {
		return nil, os.ErrNotExist
	}
	return stat, nil
}
