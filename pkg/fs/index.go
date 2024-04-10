package fs

import (
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	Delimiter = "/"
)

type Capability string

const (
	ReadCapability  Capability = "read"
	WriteCapability Capability = "write"
	SeekCapability  Capability = "seek"
)

type Tree interface {
	// Index accepts a sorted list of paths.
	// it creates a mapping of directory memberships and makes up for missing directory entries
	// (some indices have no directory indices at all, so those need to be created)
	Index(infos []os.FileInfo)

	// Readdir returns the direct descendants of the given directory at entryPath
	Readdir(entryPath string) ([]os.FileInfo, error)

	// Stat returns in the FileInfo for the given file/directory at entryPath
	Stat(entryPath string) (os.FileInfo, error)
}

type FileServerHandle interface {
	Name() string
	Lock() error
	Unlock() error
	Truncate(size int64) error
}

type FileLike interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Seeker
	io.Closer
	FileServerHandle
}

type Opener interface {
	Open(fullPath string) (FileLike, error)
	Can(capability Capability) bool
}

// helpers

var _ os.FileInfo = &IndexFileInfo{}

type IndexFileInfo struct {
	SetName    string
	SetSize    int64
	SetMode    fs.FileMode
	SetModTime time.Time
	SetSys     any
}

func (i *IndexFileInfo) Name() string {
	return i.SetName
}

func (i *IndexFileInfo) Size() int64 {
	return i.SetSize
}

func (i *IndexFileInfo) Mode() fs.FileMode {
	return i.SetMode
}

func (i *IndexFileInfo) ModTime() time.Time {
	return i.SetModTime
}

func (i *IndexFileInfo) IsDir() bool {
	return i.SetMode.IsDir()
}

func (i *IndexFileInfo) Sys() any {
	return i.SetSys
}

func fileInfoWithName(info os.FileInfo, name string) os.FileInfo {
	return &IndexFileInfo{
		SetName:    name,
		SetSize:    info.Size(),
		SetMode:    info.Mode(),
		SetModTime: info.ModTime(),
		SetSys:     info.Sys(),
	}
}

func fileInfoWithNFSMetadata(fullPath string, info os.FileInfo) os.FileInfo {
	hasher := fnv.New64()
	_, _ = hasher.Write([]byte(fullPath))
	fileId := hasher.Sum64()

	// see if existing Sys() returns a type we can use
	sys := info.Sys()
	s := &syscall.Stat_t{}
	if existing, ok := sys.(*syscall.Stat_t); ok {
		s.Nlink = existing.Nlink
		s.Uid = existing.Uid
		s.Gid = existing.Gid
		s.Rdev = existing.Rdev
	} else {
		s.Nlink = 1
		s.Uid = uint32(os.Getuid())
		s.Gid = uint32(os.Getgid())
		s.Size = info.Size()
	}
	s.Ino = fileId
	return &IndexFileInfo{
		SetName:    info.Name(),
		SetSize:    info.Size(),
		SetMode:    info.Mode(),
		SetModTime: info.ModTime(),
		SetSys:     sys,
	}
}

type ByName []os.FileInfo

func (a ByName) Len() int           { return len(a) }
func (a ByName) Less(i, j int) bool { return a[i].Name() < a[j].Name() }
func (a ByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type DirInfoGenerator func(filename string) os.FileInfo

// InMemoryTreeBuilder maintains a tree in memory
type InMemoryTreeBuilder struct {
	files       map[string]os.FileInfo
	dirs        map[string][]os.FileInfo
	directoryFn DirInfoGenerator
	l           *sync.Mutex
}

var _ Tree = &InMemoryTreeBuilder{}

func NewInMemoryTreeBuilder(directoryFn DirInfoGenerator) *InMemoryTreeBuilder {
	return &InMemoryTreeBuilder{
		files:       make(map[string]os.FileInfo),
		dirs:        make(map[string][]os.FileInfo),
		directoryFn: directoryFn,
		l:           &sync.Mutex{},
	}
}

func (t *InMemoryTreeBuilder) Index(infos []os.FileInfo) {
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
			var currentInfo os.FileInfo
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

func (t *InMemoryTreeBuilder) Readdir(entryPath string) ([]os.FileInfo, error) {
	t.l.Lock()
	defer t.l.Unlock()
	entryPath = strings.Trim(entryPath, Delimiter)
	entries, dirExists := t.dirs[entryPath]
	if !dirExists {
		return nil, os.ErrNotExist
	}
	// ReadDir returns paths relative to the read directory, not absolute paths
	relativeNamedEntries := make([]os.FileInfo, len(entries))
	for i, entry := range entries {
		relativeNamedEntries[i] = fileInfoWithNFSMetadata(entryPath, fileInfoWithName(entry, path.Base(entry.Name())))
	}
	return relativeNamedEntries, nil
}

func (t *InMemoryTreeBuilder) Stat(entryPath string) (os.FileInfo, error) {
	t.l.Lock()
	defer t.l.Unlock()
	entryPath = strings.Trim(entryPath, Delimiter)
	stat, ok := t.files[entryPath]
	if !ok {
		return nil, os.ErrNotExist
	}
	return fileInfoWithNFSMetadata(entryPath, stat), nil
}
