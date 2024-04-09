package fs

import (
	"os"
	"path"

	"github.com/go-git/go-billy/v5"
)

type ZipFile struct {
	Remote ZipFileURI
	Path   string

	f *os.File // always backed by an actual filesystem file
}

func (z *ZipFile) Name() string {
	return path.Base(z.Path)
}

func (z *ZipFile) Write(p []byte) (n int, err error) {
	return n, billy.ErrReadOnly
}

func (z *ZipFile) Read(p []byte) (n int, err error) {
	return z.f.Read(p)
}

func (z *ZipFile) ReadAt(p []byte, off int64) (n int, err error) {
	return z.f.ReadAt(p, off)
}

func (z *ZipFile) Seek(offset int64, whence int) (int64, error) {
	return z.f.Seek(offset, whence)
}

func (z *ZipFile) Close() error {
	// TODO(ozkatz): release cache entry
	return z.f.Close()
}

func (z *ZipFile) Lock() error {
	return billy.ErrNotSupported
}

func (z *ZipFile) Unlock() error {
	return billy.ErrNotSupported
}

func (z *ZipFile) Truncate(size int64) error {
	return billy.ErrReadOnly
}
