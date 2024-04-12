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

func NewNFSServer(tree index.Tree) nfs.Handler {
	zipFs := NewZipFS(tree)
	fsHandler := nfshelper.NewNullAuthHandler(zipFs)
	return nfshelper.NewCachingHandler(fsHandler, 1024)
}
