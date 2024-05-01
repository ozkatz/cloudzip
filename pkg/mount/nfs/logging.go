package nfs

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/go-git/go-billy/v5"
)

type loggingFs struct {
	ctx  context.Context
	log  *slog.Logger
	next billy.Filesystem
}

type logOp struct {
	startTime time.Time
	ctx       context.Context
	opName    string
	log       *slog.Logger
	extra     []any
}

func (op *logOp) Log(args ...any) {
	args = append(args, "took_us", time.Since(op.startTime).Microseconds())
	if op.extra != nil {
		args = append(args, op.extra...)
	}
	op.log.DebugContext(op.ctx, op.opName, args...)
}

var _ billy.File = &loggingFile{}

type loggingFile struct {
	ctx  context.Context
	log  *slog.Logger
	next billy.File
}

func (f *loggingFile) start(op string) *logOp {
	return &logOp{
		startTime: time.Now(),
		ctx:       f.ctx,
		opName:    op,
		log:       f.log,
		extra:     []any{"name", f.next.Name()},
	}
}

func (f *loggingFile) Name() string {
	return f.next.Name()
}

func (f *loggingFile) Write(p []byte) (n int, err error) {
	op := f.start("file.Write")
	n, err = f.next.Write(p)
	op.Log("n", n, "p_len", len(p), "error", err)
	return
}

func (f *loggingFile) Read(p []byte) (n int, err error) {
	op := f.start("file.Read")
	n, err = f.next.Read(p)
	op.Log("n", n, "p_len", len(p), "error", err)
	return
}

func (f *loggingFile) ReadAt(p []byte, off int64) (n int, err error) {
	op := f.start("file.ReadAt")
	n, err = f.next.ReadAt(p, off)
	op.Log("n", n, "p_len", len(p), "offset", off, "error", err)
	return
}

func (f *loggingFile) Seek(offset int64, whence int) (int64, error) {
	op := f.start("file.Seek")
	pos, err := f.next.Seek(offset, whence)
	op.Log("offset", offset, "whence", whence, "pos", pos, "error", err)
	return pos, err
}

func (f *loggingFile) Close() error {
	op := f.start("file.Close")
	err := f.next.Close()
	op.Log("error", err)
	return err
}

func (f *loggingFile) Lock() error {
	op := f.start("file.Lock")
	err := f.next.Lock()
	op.Log("error", err)
	return err
}

func (f *loggingFile) Unlock() error {
	op := f.start("file.Unlock")
	err := f.next.Unlock()
	op.Log("error", err)
	return err
}

func (f *loggingFile) Truncate(size int64) error {
	op := f.start("file.Truncate")
	err := f.next.Truncate(size)
	op.Log("size", size, "error", err)
	return err
}

func (fs *loggingFs) start(op string) *logOp {
	return &logOp{
		startTime: time.Now(),
		ctx:       fs.ctx,
		opName:    op,
		log:       fs.log,
	}
}

func (fs *loggingFs) Create(filename string) (billy.File, error) {
	op := fs.start("fs.Create")
	f, err := fs.next.Create(filename)
	op.Log("filename", filename, "error", err)
	return &loggingFile{fs.ctx, fs.log, f}, err
}

func (fs *loggingFs) Open(filename string) (billy.File, error) {
	op := fs.start("fs.Open")
	f, err := fs.next.Open(filename)
	op.Log("filename", filename, "error", err)
	return &loggingFile{fs.ctx, fs.log, f}, err
}

func (fs *loggingFs) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	op := fs.start("fs.OpenFile")
	f, err := fs.next.OpenFile(filename, flag, perm)
	op.Log("filename", filename, "flag", flag, "perm", perm, "error", err)
	return &loggingFile{fs.ctx, fs.log, f}, err
}

func (fs *loggingFs) Stat(filename string) (os.FileInfo, error) {
	op := fs.start("fs.Stat")
	inf, err := fs.next.Stat(filename)
	op.Log("filename", filename, "error", err)
	return inf, err
}

func (fs *loggingFs) Rename(oldpath, newpath string) error {
	op := fs.start("fs.Rename")
	err := fs.next.Rename(oldpath, newpath)
	op.Log("oldpath", oldpath, "newpath", newpath, "error", err)
	return err
}

func (fs *loggingFs) Remove(filename string) error {
	op := fs.start("fs.Remove")
	err := fs.next.Remove(filename)
	op.Log("filename", filename, "error", err)
	return err
}

func (fs *loggingFs) Join(elem ...string) string {
	return fs.next.Join(elem...)
}

func (fs *loggingFs) TempFile(dir, prefix string) (billy.File, error) {
	op := fs.start("fs.TempFile")
	f, err := fs.next.TempFile(dir, prefix)
	op.Log("dir", dir, "prefix", prefix, "error", err)
	return &loggingFile{fs.ctx, fs.log, f}, err
}

func (fs *loggingFs) ReadDir(path string) ([]os.FileInfo, error) {
	op := fs.start("fs.Readdir")
	files, err := fs.next.ReadDir(path)
	op.Log("path", path, "error", err, "file_count", len(files))
	return files, err
}

func (fs *loggingFs) MkdirAll(filename string, perm os.FileMode) error {
	op := fs.start("fs.MkdirAll")
	err := fs.next.MkdirAll(filename, perm)
	op.Log("filename", filename, "perm", perm, "error", err)
	return err
}

func (fs *loggingFs) Lstat(filename string) (os.FileInfo, error) {
	op := fs.start("fs.Lstat")
	info, err := fs.next.Lstat(filename)
	op.Log("filename", filename, "error", err)
	return info, err
}

func (fs *loggingFs) Symlink(target, link string) error {
	op := fs.start("fs.Symlink")
	err := fs.next.Symlink(target, link)
	op.Log("target", target, "link", link, "error", err)
	return err
}

func (fs *loggingFs) Readlink(link string) (string, error) {
	op := fs.start("fs.Readlink")
	target, err := fs.next.Readlink(link)
	op.Log("link", link, "target", target, "error", err)
	return target, err
}

func (fs *loggingFs) Chroot(path string) (billy.Filesystem, error) {
	op := fs.start("fs.Chroot")
	f, err := fs.next.Chroot(path)
	op.Log("path", path, "error", err)
	return f, err
}

func (fs *loggingFs) Root() string {
	return fs.next.Root()
}

func LoggingFS(ctx context.Context, next billy.Filesystem, log *slog.Logger) billy.Filesystem {
	return &loggingFs{ctx, log, next}
}
