package webdav

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"time"

	dav "golang.org/x/net/webdav"
)

type fileStat struct {
	sizeBytes uint64
	checksum  uint32
	Mtime     time.Time
	mode      fs.FileMode
}

type zipFileInfo struct {
	path  string
	stats *fileStat
}

func (fi *zipFileInfo) Name() string {
	return path.Base(fi.path)
}

func (fi *zipFileInfo) Size() int64 {
	if fi.Mode().IsDir() || fi.stats == nil {
		return 0
	}
	return int64(fi.stats.sizeBytes)
}

func (fi *zipFileInfo) Mode() fs.FileMode {
	return fi.stats.mode
}

func (fi *zipFileInfo) ModTime() time.Time {
	if fi.Mode().IsDir() || fi.stats == nil {
		return time.Now()
	}
	return fi.stats.Mtime
}

func (fi *zipFileInfo) IsDir() bool {
	return fi.stats.mode.IsDir()
}

func (fi *zipFileInfo) ETag() (string, error) {
	if fi.stats != nil {
		return fmt.Sprintf("%x", fi.stats.checksum), nil
	}
	return "", dav.ErrNotImplemented
}

func (fi *zipFileInfo) Sys() any {
	return nil
}

type remoteFile struct {
	zipfile       string
	info          *zipFileInfo
	handle        *os.File
	virtualOffset int64

	fileCache     *fileCache
	metadataCache *directoryCache
}

func (f *remoteFile) getFile() (*os.File, error) {
	if f.handle != nil {
		return f.handle, nil
	}
	key := fileCacheKey{
		zipfile:  f.zipfile,
		path:     f.info.path,
		checksum: f.info.stats.checksum,
	}
	expected := f.info.stats.sizeBytes
	file, err := f.fileCache.Get(key)
	if err == nil {
		return file, nil
	}
	reader, err := readFile(context.TODO(), f.zipfile, f.info.path)
	if err != nil {
		return nil, err
	}
	file, err = f.fileCache.Set(key, reader, int64(expected))
	if err != nil {
		return nil, err
	}
	f.handle = file
	return f.handle, nil
}

func (f *remoteFile) Close() error {
	if f.handle != nil {
		return f.handle.Close()
	}
	return nil
}

func (f *remoteFile) Read(p []byte) (n int, err error) {
	file, err := f.getFile()
	if err != nil {
		return 0, err
	}
	_, err = file.Seek(f.virtualOffset, io.SeekStart)
	if err != nil {
		return 0, err
	}
	n, err = file.Read(p)
	f.virtualOffset += int64(n)
	return n, err
}

func (f *remoteFile) Seek(offset int64, whence int) (int64, error) {
	file, err := f.getFile()
	if err != nil {
		return 0, err
	}
	offset, err = file.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	f.virtualOffset = offset
	return offset, err
}

func (f *remoteFile) Readdir(count int) ([]fs.FileInfo, error) {
	listing, err := listDirectory(context.TODO(), f.metadataCache, f.zipfile, f.info.path)
	if err != nil {
		return nil, err
	}
	results := make([]fs.FileInfo, len(listing))
	for i, entry := range listing {
		results[i] = entry
	}
	return results, nil
}

func (f *remoteFile) Stat() (fs.FileInfo, error) {
	if f.info != nil {
		return f.info, nil
	}
	return nil, os.ErrNotExist
}

func (f *remoteFile) Write(p []byte) (n int, err error) {
	return 0, dav.ErrNotImplemented
}
