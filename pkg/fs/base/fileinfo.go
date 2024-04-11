package base

import (
	"io/fs"
	"time"

	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

type ZipFileInfo struct {
	Remote ZipFileURI
	Path   string
	CDR    *zipfile.CDR
}

func (z *ZipFileInfo) Name() string {
	return z.Path
}

func (z *ZipFileInfo) Size() int64 {
	return int64(z.CDR.UncompressedSizeBytes)
}

func (z *ZipFileInfo) Mode() fs.FileMode {
	return z.CDR.Mode
}

func (z *ZipFileInfo) ModTime() time.Time {
	return z.CDR.Modified
}

func (z *ZipFileInfo) IsDir() bool {
	return z.CDR.Mode.IsDir()
}

func (z *ZipFileInfo) Sys() any {
	return nil
}
