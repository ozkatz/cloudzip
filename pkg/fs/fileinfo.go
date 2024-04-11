package fs

import (
	"io/fs"
	"syscall"
	"time"
)

const (
	LinkCount = 1
)

type FileInfo struct {
	CurrentName string
	FullPath    string
	FileMtime   time.Time
	FileMode    fs.FileMode
	FileId      uint64
	FileSize    int64
	FileUid     uint32
	FileGid     uint32
}

func (f *FileInfo) AsPath(filename string) *FileInfo {
	return &FileInfo{
		CurrentName: filename,
		FullPath:    f.FullPath,
		FileMtime:   f.FileMtime,
		FileMode:    f.FileMode,
		FileId:      f.FileId,
		FileSize:    f.FileSize,
		FileUid:     f.FileUid,
		FileGid:     f.FileGid,
	}
}

func (f *FileInfo) Name() string {
	if f.CurrentName == "" {
		return f.FullPath
	}
	return f.CurrentName
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
