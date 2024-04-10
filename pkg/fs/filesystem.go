package fs

import (
	"context"
	"os"
	"path"
	"sort"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

const (
	PidFilePath    = ".cz.nfs.pid"
	DefaultDirMask = 0755
)

type ZipFS struct {
	Remote ZipFileURI
	Tree   Tree
	Opener Opener

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
	remoteUri := ZipFileURI(remoteZipURI)
	startTime := time.Now()

	// build index
	infos := make([]os.FileInfo, 0)
	for _, f := range cdr {
		infos = append(infos, &ZipFileInfo{
			Remote: remoteUri,
			Path:   f.FileName,
			CDR:    f,
		})
	}
	// add pid file
	infos = append(infos, pidFile(PidFilePath, os.Getpid(), startTime).Stat())

	// sort it
	sort.Sort(ByName(infos))
	tree := NewInMemoryTreeBuilder(func(entry string) os.FileInfo {
		return &IndexFileInfo{
			SetName:    entry,
			SetMode:    os.ModeDir | DefaultDirMask,
			SetModTime: startTime,
		}
	})
	tree.Index(infos)

	// build opener
	cache := NewFileCache(cacheDir)
	opener := NewZipOpener(startTime, remoteUri, cdr, cache)

	filesys := &ZipFS{
		Remote:    remoteUri,
		Tree:      tree,
		Opener:    opener,
		startTime: startTime,
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
	return fs.Opener.Open(filename)
}

func (fs *ZipFS) Stat(filename string) (os.FileInfo, error) {
	info, err := fs.Tree.Stat(filename)
	if err != nil {
		return nil, err
	}
	base := path.Base(filename) // stat should always return the base name?
	return fileInfoWithName(info, base), nil
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
	return fs.Tree.Readdir(name)
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
