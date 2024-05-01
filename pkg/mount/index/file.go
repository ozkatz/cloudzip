package index

import (
	"io"
	"os"
)

type FileLike interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Seeker
	io.Closer
}

type Opener interface {
	Open(fullPath string, flag int, perm os.FileMode) (FileLike, error)
}

type OpenFn func(fullPath string, flag int, perm os.FileMode) (FileLike, error)

func (o OpenFn) Open(fullPath string, flag int, perm os.FileMode) (FileLike, error) {
	return o(fullPath, flag, perm)
}
