package nfs

import (
	"context"
	"github.com/ozkatz/cloudzip/pkg/mount/index"
	"net"

	"github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
)

func Serve(ctx context.Context, listener net.Listener, handler nfs.Handler) error {
	server := &nfs.Server{
		Handler: handler,
		Context: ctx,
	}
	return server.Serve(listener)
}

type NFSOptions struct {
	HandleCacheSize int
}

const defaultHandleCacheSize = 1000000

var NFSDefaultOptions = &NFSOptions{HandleCacheSize: defaultHandleCacheSize}

func NewNFSHandler(tree index.Tree, opts *NFSOptions) nfs.Handler {
	zipFs := NewZipFS(tree)
	if opts == nil {
		opts = NFSDefaultOptions
	}
	fsHandler := nfshelper.NewNullAuthHandler(zipFs)
	return nfshelper.NewCachingHandler(fsHandler, opts.HandleCacheSize)
}
