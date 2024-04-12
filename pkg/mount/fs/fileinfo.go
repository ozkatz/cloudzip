package fs

import (
	"hash/fnv"
	"io/fs"
	"os"
	"syscall"
	"time"
)

const (
	LinkCount = 1
	Delimiter = "/"
)

type FileInfoList []*FileInfo

func (a FileInfoList) Len() int           { return len(a) }
func (a FileInfoList) Less(i, j int) bool { return a[i].Name() < a[j].Name() }
func (a FileInfoList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a FileInfoList) ToOSFiles() []os.FileInfo {
	files := make([]os.FileInfo, len(a))
	for i, f := range a {
		files[i] = f
	}
	return files
}

func FileIDFromString(str string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(str))
	return h.Sum64()
}

type FileInfo struct {
	currentName string
	name        string
	mtime       time.Time
	mode        fs.FileMode
	id          uint64
	size        int64
	uid         uint32
	gid         uint32
	opener      Opener
}

func ImmutableDir(filename string, mtime time.Time) *FileInfo {
	return ImmutableInfo(filename, time.Now(), os.ModeDir|0644, 0, nil)
}

func ImmutableInfo(filename string, mtime time.Time, mode os.FileMode, size int64, opener Opener) *FileInfo {
	return &FileInfo{
		name:   filename,
		mtime:  mtime,
		mode:   mode,
		id:     FileIDFromString(filename),
		size:   size,
		uid:    uint32(os.Getuid()),
		gid:    uint32(os.Getuid()),
		opener: opener,
	}
}

func (f *FileInfo) AsPath(filename string) *FileInfo {
	return &FileInfo{
		currentName: filename,
		name:        f.name,
		mtime:       f.mtime,
		mode:        f.mode,
		id:          f.id,
		size:        f.size,
		uid:         f.uid,
		gid:         f.gid,
		opener:      f.opener,
	}
}

func (f *FileInfo) FullPath() string {
	return f.name
}

func (f *FileInfo) Name() string {
	if f.currentName == "" {
		return f.name
	}
	return f.currentName
}

func (f *FileInfo) Open(flag int, perm os.FileMode) (FileLike, error) {
	return f.opener.Open(f.name, flag, perm)
}

func (f *FileInfo) Size() int64 {
	return f.size
}

func (f *FileInfo) Mode() fs.FileMode {
	return f.mode
}

func (f *FileInfo) ModTime() time.Time {
	return f.mtime
}

func (f *FileInfo) IsDir() bool {
	return f.mode.IsDir()
}

func (f *FileInfo) FileID() uint64 {
	return f.id
}

func (f *FileInfo) Sys() any {
	return &syscall.Stat_t{
		Nlink: LinkCount,
		Ino:   f.id,
		Uid:   f.uid,
		Gid:   f.gid,
		Size:  f.Size(),
	}
}
