package procfs

import (
	"bytes"
	"github.com/ozkatz/cloudzip/pkg/mount/fs"
	"os"
	"time"
)

type InMemFile struct {
	reader *bytes.Reader
}

func (f *InMemFile) Read(p []byte) (n int, err error) {
	return f.reader.Read(p)
}

func (f *InMemFile) ReadAt(p []byte, off int64) (n int, err error) {
	return f.reader.ReadAt(p, off)
}

func (f *InMemFile) Write(p []byte) (n int, err error) {
	return 0, os.ErrPermission
}

func (f *InMemFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, os.ErrPermission
}

func (f *InMemFile) Seek(offset int64, whence int) (int64, error) {
	return f.reader.Seek(offset, whence)
}

func (f *InMemFile) Close() error {
	return nil
}

func (f *InMemFile) Size() int64 {
	return f.reader.Size()
}

func NewProcFile(path string, content []byte, modTime time.Time) *fs.FileInfo {
	f := &InMemFile{bytes.NewReader(content)}
	return &fs.FileInfo{
		FullPath:  path,
		FileMtime: modTime,
		FileMode:  0644,
		FileId:    fs.FileIDFromString(path),
		FileSize:  f.Size(),
		FileUid:   uint32(os.Getuid()),
		FileGid:   uint32(os.Getgid()),
		Opener: fs.OpenFn(func(fullPath string, flag int, perm os.FileMode) (fs.FileLike, error) {
			return f, nil
		}),
	}
}
