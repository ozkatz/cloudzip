package dav

import (
	"github.com/ozkatz/cloudzip/pkg/mount/index"
	"golang.org/x/net/webdav"
	"io/fs"
)

var _ webdav.File = &treeFile{}

type treeFile struct {
	tree index.Tree
	fi   *index.FileInfo

	handle  index.FileLike
	voffset int64
}

func (f *treeFile) Close() error {
	if f.handle != nil {
		err := f.handle.Close()
		f.handle = nil
		return err
	}
	return nil
}

func (f *treeFile) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if f.handle == nil {
		f.handle, err = f.fi.Open(0, 0755)
		if err != nil {
			return
		}
	}
	return f.handle.Read(p)
}

func (f *treeFile) Seek(offset int64, whence int) (int64, error) {
	var err error
	if f.handle == nil {
		f.handle, err = f.fi.Open(0, 0755)
		if err != nil {
			return 0, err
		}
	}
	return f.handle.Seek(offset, whence)
}

func (f *treeFile) Readdir(count int) ([]fs.FileInfo, error) {
	fis, err := f.tree.Readdir(f.fi.Name())
	if err != nil {
		return nil, err
	}
	infos := make([]fs.FileInfo, len(fis))
	for i, fi := range fis {
		infos[i] = fi
	}
	if count > 0 {
		return infos[:count], nil
	}
	return infos, nil

}

func (f *treeFile) Stat() (fs.FileInfo, error) {
	return f.fi, nil
}

func (f *treeFile) Write(p []byte) (n int, err error) {
	if f.handle == nil {
		f.handle, err = f.fi.Open(0, 0755)
		if err != nil {
			return
		}
	}
	return f.handle.Write(p)
}
