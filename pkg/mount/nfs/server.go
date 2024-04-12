package nfs

import (
	"github.com/ozkatz/cloudzip/pkg/mount/index"
	"net"

	"github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
)

func Serve(listener net.Listener, handler nfs.Handler) error {
	return nfs.Serve(listener, handler)
}

type NFSOptions struct {
	HandleCacheSize int
}

const defaultHandleCacheSize = 1000000

var NFSDefaultOptions = &NFSOptions{HandleCacheSize: defaultHandleCacheSize}

func NewNFSServer(tree index.Tree, opts *NFSOptions) nfs.Handler {
	zipFs := NewZipFS(tree)
	if opts == nil {
		opts = NFSDefaultOptions
	}
	fsHandler := nfshelper.NewNullAuthHandler(zipFs)
	return nfshelper.NewCachingHandler(fsHandler, opts.HandleCacheSize)
}
