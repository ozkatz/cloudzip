package remote

import (
	"context"
	"io"
)

type Fetcher interface {
	Fetch(ctx context.Context, startOffset *int64, endOffset *int64) (io.ReadCloser, error)
}
