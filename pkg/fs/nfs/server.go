package nfs

import (
	"net"

	"github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
)

func Serve(listener net.Listener, handler nfs.Handler) error {
	return nfs.Serve(listener, handler)
}

func NewNFSServer(cacheDir, zipFileURI string) (nfs.Handler, error) {
	zipFs, err := NewZipFS(cacheDir, zipFileURI)
	if err != nil {
		return nil, err
	}
	fsHandler := nfshelper.NewNullAuthHandler(zipFs)
	//return fsHandler, nil
	return nfshelper.NewCachingHandler(fsHandler, 1024), nil
}
