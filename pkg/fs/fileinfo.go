package fs

import (
	"crypto/sha256"
	"encoding/binary"
	"io/fs"
	"os"
	"path"
	"syscall"
	"time"

	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

type ZipFileInfo struct {
	Remote ZipFileURI
	Path   string
	cdr    *zipfile.CDR
}

func (z *ZipFileInfo) Name() string {
	return path.Base(z.Path)
}

func (z *ZipFileInfo) Size() int64 {
	return int64(z.cdr.UncompressedSizeBytes)
}

func (z *ZipFileInfo) Mode() fs.FileMode {
	return z.cdr.Mode
}

func (z *ZipFileInfo) ModTime() time.Time {
	return z.cdr.Modified
}

func (z *ZipFileInfo) IsDir() bool {
	return z.cdr.Mode.IsDir()
}

func (z *ZipFileInfo) ino() uint64 {
	h := sha256.New()
	h.Write([]byte(z.Name()))
	data := h.Sum(nil)
	return binary.LittleEndian.Uint64(data[:8])
}

func (z *ZipFileInfo) Sys() any {
	return &syscall.Stat_t{
		Uid:   uint32(os.Getuid()),
		Gid:   uint32(os.Getgid()),
		Size:  int64(z.cdr.UncompressedSizeBytes),
		Nlink: 1,
		Ino:   z.ino(),
	}
}
