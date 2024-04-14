package nfs

import (
	"context"
	"net"

	"github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"

	"github.com/ozkatz/cloudzip/pkg/mount/index"
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

var DefaultOptions = &NFSOptions{HandleCacheSize: defaultHandleCacheSize}

func NewHandler(tree index.Tree, opts *NFSOptions) nfs.Handler {
	zipFs := NewZipFS(tree)
	if opts == nil {
		opts = DefaultOptions
	}
	fsHandler := nfshelper.NewNullAuthHandler(zipFs)
	return nfshelper.NewCachingHandler(fsHandler, opts.HandleCacheSize)
}
