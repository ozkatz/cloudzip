package nfs

import (
	"os"
	"path"

	"github.com/go-git/go-billy/v5"

	"github.com/ozkatz/cloudzip/pkg/mount/index"
)

type ZipFS struct {
	Tree index.Tree
}

func NewZipFS(tree index.Tree) billy.Filesystem {
	return &ZipFS{Tree: tree}
}

func (fs *ZipFS) Create(filename string) (billy.File, error) {
	return nil, billy.ErrReadOnly
}

func (fs *ZipFS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ZipFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	s, err := fs.Tree.Stat(filename)
	if err != nil {
		return nil, err
	}
	if s.IsDir() {
		return nil, billy.ErrNotSupported
	}
	f, err := s.Open(flag, perm)
	if err != nil {
		return nil, err
	}
	return fileLikeToBilly(f, filename), nil
}

func (fs *ZipFS) Stat(filename string) (os.FileInfo, error) {
	info, err := fs.Tree.Stat(filename)
	if err != nil {
		return nil, err
	}
	basename := path.Base(filename) // stat should always return the base name?
	return &nfsFileInfo{info.AsPath(basename)}, nil
}

func (fs *ZipFS) Rename(oldpath, newpath string) error {
	return billy.ErrReadOnly
}

func (fs *ZipFS) Remove(filename string) error {
	return billy.ErrReadOnly
}

func (fs *ZipFS) Join(elem ...string) string {
	return path.Join(elem...)
}

func (fs *ZipFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, billy.ErrReadOnly
}

func (fs *ZipFS) ReadDir(name string) ([]os.FileInfo, error) {
	dir, err := fs.Tree.Readdir(name)
	if err != nil {
		return nil, err
	}
	nfsDir := make([]os.FileInfo, len(dir))
	for i, e := range dir {
		nfsDir[i] = &nfsFileInfo{e}
	}
	return nfsDir, nil
}

func (fs *ZipFS) MkdirAll(filename string, perm os.FileMode) error {
	return billy.ErrReadOnly
}

func (fs *ZipFS) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

func (fs *ZipFS) Symlink(target, link string) error {
	return billy.ErrReadOnly
}

func (fs *ZipFS) Readlink(link string) (string, error) {
	return "", billy.ErrNotSupported
}

func (fs *ZipFS) Chroot(path string) (billy.Filesystem, error) {
	return nil, billy.ErrNotSupported
}

func (fs *ZipFS) Root() string {
	return ""
}

// Capabilities exports the filesystem as readonly
func (fs *ZipFS) Capabilities() billy.Capability {
	return billy.ReadCapability | billy.SeekCapability
}
