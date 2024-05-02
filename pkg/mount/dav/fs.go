package dav

import (
	"context"
	"os"

	"golang.org/x/net/webdav"

	"github.com/ozkatz/cloudzip/pkg/mount/commonfs"
)

var _ webdav.FileSystem = &davFS{}

type davFS struct {
	tree commonfs.Tree
}

func NewDavFS(tree commonfs.Tree) webdav.FileSystem {
	return &davFS{tree}
}

func (fs *davFS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return os.ErrInvalid
}

func (fs *davFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	f, err := fs.tree.Stat(name)
	if err != nil {
		return nil, err
	}
	return &treeFile{
		tree: fs.tree,
		fi:   f,
	}, nil
}

func (fs *davFS) RemoveAll(ctx context.Context, name string) error {
	return os.ErrInvalid
}

func (fs *davFS) Rename(ctx context.Context, oldName, newName string) error {
	return os.ErrInvalid
}

func (fs *davFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	return fs.tree.Stat(name)
}
