package download

import (
	"context"
	"io"
)

type Downloader interface {
	Download(ctx context.Context, uri string, offsetStart int64, offsetEnd int64) (io.ReadCloser, error)
	SizeOf(ctx context.Context, uri string) (int64, error)
}
