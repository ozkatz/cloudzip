package nfs

import (
	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs/file"

	"github.com/ozkatz/cloudzip/pkg/mount/fs"
)

type nfsFileInfo struct {
	*fs.FileInfo
}

func (f *nfsFileInfo) Sys() any {
	return &file.FileInfo{
		Nlink:  f.NLink(),
		UID:    f.Uid(),
		GID:    f.Gid(),
		Fileid: f.FileID(),
	}
}

type nfsFile struct {
	fs.FileLike
	name string
}

func (n *nfsFile) Name() string {
	return n.name
}

func (n *nfsFile) Lock() error {
	return billy.ErrNotSupported
}

func (n *nfsFile) Unlock() error {
	return billy.ErrNotSupported
}

func (n *nfsFile) Truncate(size int64) error {
	return billy.ErrNotSupported
}

func fileLikeToBilly(f fs.FileLike, filename string) billy.File {
	return &nfsFile{f, filename}
}
