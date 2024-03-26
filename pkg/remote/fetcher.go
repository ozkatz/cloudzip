package remote

import (
	"context"
	"fmt"
	"io"
)

type Fetcher interface {
	Fetch(ctx context.Context, startOffset *int64, endOffset *int64) (io.ReadCloser, error)
}

func strPtr(s string) *string {
	return &s
}

func buildRange(offsetStart *int64, offsetEnd *int64) *string {
	if offsetStart != nil && offsetEnd != nil {
		return strPtr(fmt.Sprintf("bytes=%d-%d", *offsetStart, *offsetEnd))
	} else if offsetStart != nil {
		return strPtr(fmt.Sprintf("bytes=%d-", *offsetStart))
	} else if offsetEnd != nil {
		return strPtr(fmt.Sprintf("bytes=-%d", *offsetEnd))
	}
	return nil
}
