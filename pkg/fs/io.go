package fs

import (
	"context"
	"io"
	"path"

	"github.com/ozkatz/cloudzip/pkg/remote"
)

type adapter struct {
	f   remote.Fetcher
	ctx context.Context
}

func (a *adapter) Fetch(start, end *int64) (io.Reader, error) {
	return a.f.Fetch(a.ctx, start, end)
}

func IsDirectDescendant(filePath, baseDir string) bool {
	return path.Dir(path.Clean(filePath)) == path.Clean(baseDir)
}
