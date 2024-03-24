package remote_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"io"
	"testing"
)

func TestLocalDownloader_SizeOf(t *testing.T) {
	ctx := context.Background()
	l := remote.NewLocalDownloader()
	t.Run("non empty file", func(t *testing.T) {
		size, err := l.SizeOf(ctx, "file://testdata/lorem.txt")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		if size != 446 {
			t.Errorf("expected size 446, got %d", size)
		}
	})
	t.Run("empty file", func(t *testing.T) {
		size, err := l.SizeOf(ctx, "file://testdata/empty.txt")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		if size != 0 {
			t.Errorf("expected size 0, got %d", size)
		}
	})
	t.Run("non-existent file", func(t *testing.T) {
		_, err := l.SizeOf(ctx, "file://testdata/no_such_file.txt")
		if !errors.Is(err, remote.ErrDoesNotExist) {
			t.Errorf("unexpected error, %v", err)
		}
	})
}

func TestLocalDownloader_Download(t *testing.T) {
	ctx := context.Background()
	l := remote.NewLocalDownloader()
	t.Run("full file", func(t *testing.T) {
		r, err := l.Download(ctx, "file://testdata/lorem.txt", 0, 445)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Errorf("could not read file: %v", err)
			return
		}
		if len(data) != 446 {
			t.Errorf("expected size 446, got %d", len(data))
		}
	})
	t.Run("file part", func(t *testing.T) {
		r, err := l.Download(ctx, "file://testdata/lorem.txt", 5, 15)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Errorf("could not read file: %v", err)
			return
		}
		if !bytes.Equal(data, []byte(" ipsum dolo")) {
			t.Errorf("wrong body returned: %s\n", data)
		}

	})
	t.Run("file part (beginning)", func(t *testing.T) {
		r, err := l.Download(ctx, "file://testdata/lorem.txt", 0, 10)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Errorf("could not read file: %v", err)
			return
		}
		if !bytes.Equal(data, []byte("Lorem ipsum")) {
			t.Errorf("wrong body returned: %s\n", data)
		}
	})
	t.Run("non-existent file", func(t *testing.T) {
		_, err := l.Download(ctx, "file://testdata/no_such_file.txt", 0, 100)
		if !errors.Is(err, remote.ErrDoesNotExist) {
			t.Errorf("unexpected error, %v", err)
		}
	})
}
