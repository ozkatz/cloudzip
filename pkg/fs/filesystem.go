package fs

import (
	"archive/zip"
	"context"
	"errors"
	"io"
	"os"
	"path"
	"sort"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

const (
	PidFilePath = ".cz.nfs.pid"
)

type ZipFS struct {
	Remote    ZipFileURI
	Directory []*zipfile.CDR
	fileCache *FileCache
	startTime time.Time
}

func NewZipFS(cacheDir, remoteZipURI string) (billy.Filesystem, error) {
	obj, err := remote.Object(remoteZipURI)
	if err != nil {
		return nil, err
	}
	zip := &adapter{
		f:   obj,
		ctx: context.Background(),
	}
	parser := zipfile.NewCentralDirectoryParser(zip)
	cdr, err := parser.GetCentralDirectory()
	if err != nil {
		return nil, err
	}
	filesys := &ZipFS{
		Remote:    ZipFileURI(remoteZipURI),
		Directory: cdr,
		fileCache: NewFileCache(cacheDir),
		startTime: time.Now(),
	}
	return filesys, nil
}

func (fs *ZipFS) Create(filename string) (billy.File, error) {
	return nil, billy.ErrReadOnly
}

func (fs *ZipFS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *ZipFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	if path.Clean(filename) == PidFilePath {
		return pidFile(PidFilePath, os.Getpid(), fs.startTime), nil
	}
	info, err := fs.Stat(filename)
	if err != nil {
		return nil, err
	}
	filename = path.Clean(filename)
	zipInfo := info.(*ZipFileInfo)
	cacheKey := FileCacheKey{
		zipfile:  fs.Remote,
		path:     filename,
		checksum: zipInfo.cdr.CRC32Uncompressed,
	}
	f, err := fs.fileCache.Get(cacheKey)
	if errors.Is(err, os.ErrNotExist) {
		// cache miss!
		remoteZip, err := remote.Object(string(fs.Remote))
		if err != nil {
			return nil, err
		}
		zip := zipfile.NewCentralDirectoryParser(&adapter{
			f:   remoteZip,
			ctx: context.Background(),
		})
		reader, err := zip.Read(filename)
		if err != nil {
			return nil, err
		}
		f, err := fs.fileCache.Set(cacheKey, io.NopCloser(reader), zipInfo.Size())
		return &ZipFile{
			Remote: fs.Remote,
			Path:   filename,
			f:      f,
		}, nil
	} else if err != nil {
		return nil, err
	}
	// cache hit!
	return &ZipFile{
		Remote: fs.Remote,
		Path:   filename,
		f:      f,
	}, nil
}

func (fs *ZipFS) Stat(filename string) (os.FileInfo, error) {
	base := path.Base(filename) // stat should always return the base name?
	if filename == "" {         // root
		return &ZipFileInfo{
			Remote: fs.Remote,
			Path:   base,
			cdr: &zipfile.CDR{
				CompressionMethod:     zip.Store,
				Modified:              fs.startTime,
				Mode:                  os.ModeDir,
				LocalFileHeaderOffset: 0,
				FileName:              base,
			},
		}, nil
	}
	if path.Clean(filename) == PidFilePath {
		return pidFile(base, os.Getpid(), fs.startTime).Stat(), nil
	}
	for _, f := range fs.Directory {
		if path.Clean(f.FileName) == path.Clean(filename) {
			return &ZipFileInfo{
				Remote: fs.Remote,
				Path:   base,
				cdr:    f,
			}, nil
		}
	}
	return nil, os.ErrNotExist
}

func (fs *ZipFS) Rename(oldpath, newpath string) error {
	return billy.ErrReadOnly
}

func (fs *ZipFS) Remove(filename string) error {
	return billy.ErrReadOnly
}

func (fs *ZipFS) Join(elem ...string) string {
	return path.Join(elem...)
}

func (fs *ZipFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, billy.ErrReadOnly
}

type ByName []os.FileInfo

func (a ByName) Len() int           { return len(a) }
func (a ByName) Less(i, j int) bool { return a[i].Name() < a[j].Name() }
func (a ByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (fs *ZipFS) ReadDir(name string) ([]os.FileInfo, error) {
	results := make([]os.FileInfo, 0)
	for _, f := range fs.Directory {
		current := path.Clean(f.FileName)
		// filter only direct descendants of filePath
		if !IsDirectDescendant(current, name) {
			continue
		}
		results = append(results, &ZipFileInfo{
			Remote: fs.Remote,
			Path:   current,
			cdr:    f,
		})

	}
	// add pid file if root directory
	if name == "" {
		pf := pidFile(PidFilePath, os.Getpid(), fs.startTime)
		results = append(results, pf.Stat())
	}
	sort.Sort(ByName(results))
	return results, nil
}

func (fs *ZipFS) MkdirAll(filename string, perm os.FileMode) error {
	return billy.ErrReadOnly
}

func (fs *ZipFS) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

func (fs *ZipFS) Symlink(target, link string) error {
	return billy.ErrReadOnly
}

func (fs *ZipFS) Readlink(link string) (string, error) {
	return "", billy.ErrNotSupported
}

func (fs *ZipFS) Chroot(path string) (billy.Filesystem, error) {
	return nil, billy.ErrNotSupported
}

func (fs *ZipFS) Root() string {
	return ""
}

// Capabilities exports the filesystem as readonly
func (fs *ZipFS) Capabilities() billy.Capability {
	return billy.ReadCapability | billy.SeekCapability
}
