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
func (a FileInfoList) AsOSFiles() []os.FileInfo {
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
	FullPath    string
	FileMtime   time.Time
	FileMode    fs.FileMode
	FileId      uint64
	FileSize    int64
	FileUid     uint32
	FileGid     uint32
	Opener      Opener
}

func (f *FileInfo) AsPath(filename string) *FileInfo {
	return &FileInfo{
		currentName: filename,
		FullPath:    f.FullPath,
		FileMtime:   f.FileMtime,
		FileMode:    f.FileMode,
		FileId:      f.FileId,
		FileSize:    f.FileSize,
		FileUid:     f.FileUid,
		FileGid:     f.FileGid,
		Opener:      f.Opener,
	}
}

func (f *FileInfo) Name() string {
	if f.currentName == "" {
		return f.FullPath
	}
	return f.currentName
}

func (f *FileInfo) Open(flag int, perm os.FileMode) (FileLike, error) {
	return f.Opener.Open(f.FullPath, flag, perm)
}

func (f *FileInfo) Size() int64 {
	return f.FileSize
}

func (f *FileInfo) Mode() fs.FileMode {
	return f.FileMode
}

func (f *FileInfo) ModTime() time.Time {
	return f.FileMtime
}

func (f *FileInfo) IsDir() bool {
	return f.FileMode.IsDir()
}

func (f *FileInfo) Sys() any {
	return &syscall.Stat_t{
		Nlink: LinkCount,
		Ino:   f.FileId,
		Uid:   f.FileUid,
		Gid:   f.FileGid,
		Size:  f.Size(),
	}
}
