package webdav

import (
	"context"
	"errors"
	dav "golang.org/x/net/webdav"
	"os"
	"strings"
)

type ZipROFilesystem struct {
	fileCache     *fileCache
	metadataCache *directoryCache
	registry      *MountRegistry
}

func extractMountIdAndPath(urlPath string) (string, string, error) {
	// structure of names is generally /<nonce>/<mountId>/[path]
	parts := strings.SplitN(urlPath, "/", 4)
	if len(parts) != 4 {
		return "", "", os.ErrNotExist
	}
	mountId := parts[2]
	path := parts[3]
	if mountId == "" {
		return "", "", os.ErrNotExist
	}
	return mountId, path, nil
}

func (fs *ZipROFilesystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return os.ErrInvalid // read-only
}

func (fs *ZipROFilesystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (dav.File, error) {
	// try reading file
	mountId, name, err := extractMountIdAndPath(name)
	if err != nil {
		return nil, err
	}
	zipFile, ok := fs.registry.GetID(mountId)
	if !ok {
		return nil, os.ErrNotExist
	}
	info, err := getFileInfo(ctx, fs.metadataCache, zipFile.Remote, name)
	if errors.Is(err, os.ErrNotExist) {
		// not a file, but perhaps a directory?
		dirInfo, err := getDirInfo(ctx, fs.metadataCache, zipFile.Remote, name)
		return &remoteFile{
			zipfile:       zipFile.Remote,
			info:          dirInfo,
			fileCache:     fs.fileCache,
			metadataCache: fs.metadataCache,
		}, err
	} else if err != nil {
		// something bad happened!
		return nil, err
	}

	// valid file!
	return &remoteFile{
		zipfile:       zipFile.Remote,
		info:          info,
		fileCache:     fs.fileCache,
		metadataCache: fs.metadataCache,
	}, err
}

func (fs *ZipROFilesystem) RemoveAll(ctx context.Context, name string) error {
	return dav.ErrNotImplemented // read-only
}

func (fs *ZipROFilesystem) Rename(ctx context.Context, oldName, newName string) error {
	return dav.ErrNotImplemented // read-only
}

func (fs *ZipROFilesystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	// try reading file
	mountId, name, err := extractMountIdAndPath(name)
	if err != nil {
		return nil, err
	}
	zipFile, ok := fs.registry.GetID(mountId)
	if !ok {
		return nil, os.ErrNotExist
	}
	info, err := getFileInfo(ctx, fs.metadataCache, zipFile.Remote, name)
	if errors.Is(err, os.ErrNotExist) {
		// not a file, but perhaps a directory?
		info, err := getDirInfo(ctx, fs.metadataCache, zipFile.Remote, name)
		if err != nil {
			return nil, err
		}
		return info, nil
	}
	return info, err
}
