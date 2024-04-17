package dav

import (
	"log/slog"
	"net"
	"net/http"

	"golang.org/x/net/webdav"

	"github.com/ozkatz/cloudzip/pkg/mount/index"
)

func newHandler(fs webdav.FileSystem, prefix string) http.Handler {
	return &webdav.Handler{
		Prefix:     prefix,
		FileSystem: fs,
		LockSystem: webdav.NewMemLS(),
	}
}

func Serve(listener net.Listener, tree index.Tree, logger *slog.Logger) error {
	h := newHandler(NewDavFS(tree), "/mount")
	if logger != nil {
		h = &loggingHandler{
			logger: logger,
			next:   h,
		}
	}
	server := &http.Server{Handler: h}
	return server.Serve(listener)
}
