package base

import (
	"context"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"io"
)

type ZipStorageAdapter struct {
	f   remote.Fetcher
	ctx context.Context
}

func NewZipStorageAdapter(ctx context.Context, f remote.Fetcher) *ZipStorageAdapter {
	return &ZipStorageAdapter{
		f:   f,
		ctx: ctx,
	}
}

func (z *ZipStorageAdapter) Fetch(start, end *int64) (io.Reader, error) {
	return z.f.Fetch(z.ctx, start, end)
}
