package procfs

import (
	"bytes"
	"os"
	"time"

	"github.com/ozkatz/cloudzip/pkg/mount/index"
)

const (
	ProcFileMode os.FileMode = 0644
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

func NewProcFile(path string, content []byte, modTime time.Time) *index.FileInfo {
	f := &InMemFile{bytes.NewReader(content)}
	opener := func(fullPath string, flag int, perm os.FileMode) (index.FileLike, error) {
		return f, nil
	}
	return index.ImmutableInfo(path, modTime, ProcFileMode, f.Size(), index.OpenFn(opener))
}
