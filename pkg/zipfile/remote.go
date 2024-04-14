package zipfile

import (
	"context"
	"io"

	"github.com/ozkatz/cloudzip/pkg/remote"
)

type StorageAdapter struct {
	f   remote.Fetcher
	ctx context.Context
}

func NewStorageAdapter(ctx context.Context, f remote.Fetcher) *StorageAdapter {
	return &StorageAdapter{
		f:   f,
		ctx: ctx,
	}
}

func (z *StorageAdapter) Fetch(start, end *int64) (io.Reader, error) {
	return z.f.Fetch(z.ctx, start, end)
}
