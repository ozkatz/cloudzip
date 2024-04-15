package nfs

import (
	"context"
	"log/slog"
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

type Options struct {
	HandleCacheSize int
	Logger          *slog.Logger
}

const DefaultHandleCacheSize = 1000000

var DefaultOptions = &Options{
	HandleCacheSize: DefaultHandleCacheSize,
}

func NewHandler(ctx context.Context, tree index.Tree, opts *Options) nfs.Handler {
	zipFs := NewZipFS(tree)
	if opts == nil {
		opts = DefaultOptions
	}
	if opts.Logger != nil {
		zipFs = LoggingFS(ctx, zipFs, opts.Logger)
	}
	fsHandler := nfshelper.NewNullAuthHandler(zipFs)
	return nfshelper.NewCachingHandler(fsHandler, opts.HandleCacheSize)
}
