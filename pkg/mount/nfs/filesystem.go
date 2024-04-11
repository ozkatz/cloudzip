package nfs

import (
	"context"
	"github.com/ozkatz/cloudzip/pkg/mount/fs"
	"github.com/ozkatz/cloudzip/pkg/mount/index"
	"github.com/ozkatz/cloudzip/pkg/mount/procfs"
	"hash/fnv"
	"os"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

const (
	ProcPidFilePath = ".cz/server.pid"
	DefaultDirMask  = 0755
)

type ZipFS struct {
	Remote string
	Tree   index.Tree

	startTime time.Time
}

func stringToInt64Hash(str string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(str))
	return h.Sum64()
}

func NewZipFS(cacheDir, remoteZipURI string) (billy.Filesystem, error) {
	ctx := context.Background()
	obj, err := remote.Object(remoteZipURI)
	if err != nil {
		return nil, err
	}
	zip := zipfile.NewStorageAdapter(ctx, obj)
	parser := zipfile.NewCentralDirectoryParser(zip)
	cdr, err := parser.GetCentralDirectory()
	if err != nil {
		return nil, err
	}
	startTime := time.Now()

	// build index
	infos := make(fs.FileInfoList, 0)
	cache := fs.NewFileCache(cacheDir)
	for _, f := range cdr {
		infos = append(infos, &fs.FileInfo{
			FullPath:  f.FileName,
			FileMtime: f.Modified,
			FileMode:  f.Mode,
			FileId:    stringToInt64Hash(f.FileName),
			FileSize:  int64(f.UncompressedSizeBytes),
			FileUid:   uint32(os.Getuid()),
			FileGid:   uint32(os.Getgid()),
			Opener:    getOpenerFor(remoteZipURI, f, cache),
		})
	}

	// "proc" filesystem exposed to users
	infos = append(infos, procfs.NewProcFile(".cz/server.pid", []byte(strconv.Itoa(os.Getpid())), startTime))
	infos = append(infos, procfs.NewProcFile(".cz/cachedir", []byte(cacheDir), startTime))
	infos = append(infos, procfs.NewProcFile(".cz/source", []byte(remoteZipURI), startTime))

	// sort it
	sort.Sort(infos)
	tree := index.NewInMemoryTreeBuilder(func(entry string) *fs.FileInfo {
		return &fs.FileInfo{
			FullPath:  entry,
			FileMtime: startTime,
			FileMode:  os.ModeDir | DefaultDirMask,
			FileId:    stringToInt64Hash(entry),
			FileSize:  64,
			FileUid:   uint32(os.Getuid()),
			FileGid:   uint32(os.Getgid()),
			Opener:    nil,
		}
	})
	err = tree.Index(infos)
	if err != nil {
		return nil, err
	}
	return &ZipFS{
		Remote:    remoteZipURI,
		Tree:      tree,
		startTime: startTime,
	}, nil
}

func (fs *ZipFS) Create(filename string) (billy.File, error) {
	return nil, billy.ErrReadOnly
}

func (fs *ZipFS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

type nfsFile struct {
	fs.FileLike
	name string
}

func (n *nfsFile) Name() string {
	return n.name
}

func (n *nfsFile) Lock() error {
	return billy.ErrNotSupported
}

func (n *nfsFile) Unlock() error {
	return billy.ErrNotSupported
}

func (n *nfsFile) Truncate(size int64) error {
	return billy.ErrNotSupported
}

func fileLikeToBilly(f fs.FileLike, filename string) billy.File {
	return &nfsFile{f, filename}
}

func (fs *ZipFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	s, err := fs.Tree.Stat(filename)
	if err != nil {
		return nil, err
	}
	if s.IsDir() {
		return nil, billy.ErrNotSupported
	}
	f, err := s.Open(flag, perm)
	if err != nil {
		return nil, err
	}
	return fileLikeToBilly(f, filename), nil
}

func (fs *ZipFS) Stat(filename string) (os.FileInfo, error) {
	info, err := fs.Tree.Stat(filename)
	if err != nil {
		return nil, err
	}
	basename := path.Base(filename) // stat should always return the base name?
	return info.AsPath(basename), nil
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

func (fs *ZipFS) ReadDir(name string) ([]os.FileInfo, error) {
	dir, err := fs.Tree.Readdir(name)
	if err != nil {
		return nil, err
	}
	return dir.AsOSFiles(), nil
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
