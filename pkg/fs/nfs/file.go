package nfs

import (
	"context"
	"errors"
	"github.com/ozkatz/cloudzip/pkg/fs/base"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
	"io"
	"os"
	"path"
	"time"

	"github.com/go-git/go-billy/v5"
)

type ZipFile struct {
	Remote base.ZipFileURI
	Path   string

	f *os.File // always backed by an actual filesystem file
}

func (z *ZipFile) WriteAt(p []byte, off int64) (n int, err error) {
	return n, billy.ErrReadOnly
}

func (z *ZipFile) Name() string {
	return path.Base(z.Path)
}

func (z *ZipFile) Write(p []byte) (n int, err error) {
	return n, billy.ErrReadOnly
}

func (z *ZipFile) Read(p []byte) (n int, err error) {
	return z.f.Read(p)
}

func (z *ZipFile) ReadAt(p []byte, off int64) (n int, err error) {
	return z.f.ReadAt(p, off)
}

func (z *ZipFile) Seek(offset int64, whence int) (int64, error) {
	return z.f.Seek(offset, whence)
}

func (z *ZipFile) Close() error {
	// TODO(ozkatz): release cache entry
	return z.f.Close()
}

func (z *ZipFile) Lock() error {
	return billy.ErrNotSupported
}

func (z *ZipFile) Unlock() error {
	return billy.ErrNotSupported
}

func (z *ZipFile) Truncate(size int64) error {
	return billy.ErrReadOnly
}

var _ base.Opener = &ZipEntryOpener{}

type ZipEntryOpener struct {
	StartTime time.Time
	Remote    base.ZipFileURI
	Cache     *base.FileCache
	CDRs      []*zipfile.CDR
}

func NewZipOpener(startTime time.Time, remote base.ZipFileURI, directory []*zipfile.CDR, cache *base.FileCache) *ZipEntryOpener {
	return &ZipEntryOpener{
		StartTime: startTime,
		Remote:    remote,
		CDRs:      directory,
		Cache:     cache,
	}
}

func (z *ZipEntryOpener) findCdr(filename string) *zipfile.CDR {
	for _, f := range z.CDRs {
		if path.Clean(f.FileName) == path.Clean(filename) {
			return f
		}
	}
	return nil
}

func (z *ZipEntryOpener) Open(filename string) (base.FileLike, error) {
	if path.Clean(filename) == PidFilePath {
		return PidFile(PidFilePath, os.Getpid(), z.StartTime), nil
	}
	cdr := z.findCdr(filename)
	if cdr == nil {
		return nil, os.ErrNotExist
	}

	filename = path.Clean(filename)
	cacheKey := base.FileCacheKey{
		ZipFile:  z.Remote,
		Path:     filename,
		Checksum: cdr.CRC32Uncompressed,
	}
	f, err := z.Cache.Get(cacheKey)
	if errors.Is(err, os.ErrNotExist) {
		// cache miss!
		remoteZip, err := remote.Object(string(z.Remote))
		if err != nil {
			return nil, err
		}
		ctx := context.Background()
		fetcher := base.NewZipStorageAdapter(ctx, remoteZip)
		zip := zipfile.NewCentralDirectoryParser(fetcher)
		reader, err := zip.Read(filename)
		if err != nil {
			return nil, err
		}
		f, err := z.Cache.Set(cacheKey, io.NopCloser(reader), int64(cdr.UncompressedSizeBytes))
		return &ZipFile{
			Remote: z.Remote,
			Path:   filename,
			f:      f,
		}, nil
	} else if err != nil {
		return nil, err
	}
	// cache hit!
	return &ZipFile{
		Remote: z.Remote,
		Path:   filename,
		f:      f,
	}, nil
}

func (z *ZipEntryOpener) Can(capability base.Capability) bool {
	switch capability {
	case base.ReadCapability, base.SeekCapability:
		return true
	}
	return false
}
