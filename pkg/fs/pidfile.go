package fs

import (
	"bytes"
	"github.com/go-git/go-billy/v5"
	"io/fs"
	"os"
	"strconv"
	"time"
)

type billyPidFileStat struct {
	name      string
	pid       int
	sizeBytes int
	startTime time.Time
}

func (b *billyPidFileStat) Name() string {
	return b.name
}

func (b *billyPidFileStat) Size() int64 {
	return int64(b.sizeBytes)
}

func (b *billyPidFileStat) Mode() fs.FileMode {
	return fs.ModePerm
}

func (b *billyPidFileStat) ModTime() time.Time {
	return b.startTime
}

func (b *billyPidFileStat) IsDir() bool {
	return false
}

func (b *billyPidFileStat) Sys() any {
	return nil
}

type billyPidFile struct {
	name string
	pid  int
	r    *bytes.Reader
	stat *billyPidFileStat
}

func (b *billyPidFile) Name() string {
	return b.name
}

func (b *billyPidFile) Write(p []byte) (n int, err error) {
	return 0, billy.ErrReadOnly
}

func (b *billyPidFile) Read(p []byte) (n int, err error) {
	return b.r.Read(p)
}

func (b *billyPidFile) ReadAt(p []byte, off int64) (n int, err error) {
	return b.r.ReadAt(p, off)
}

func (b *billyPidFile) Seek(offset int64, whence int) (int64, error) {
	return b.r.Seek(offset, whence)
}

func (b *billyPidFile) Close() error {
	return nil
}

func (b *billyPidFile) Lock() error {
	return billy.ErrNotSupported
}

func (b *billyPidFile) Unlock() error {
	return billy.ErrNotSupported
}

func (b *billyPidFile) Truncate(size int64) error {
	return billy.ErrReadOnly
}

func (b *billyPidFile) Stat() os.FileInfo {
	return b.stat
}

func pidFile(name string, pid int, startTime time.Time) *billyPidFile {
	data := []byte(strconv.Itoa(pid))

	return &billyPidFile{
		name: name,
		pid:  pid,
		r:    bytes.NewReader(data),
		stat: &billyPidFileStat{
			name:      name,
			pid:       pid,
			sizeBytes: len(data),
			startTime: startTime,
		},
	}
}
