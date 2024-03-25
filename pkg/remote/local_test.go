package remote_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"io"
	"testing"
)

func int64p(n int64) *int64 {
	return &n
}

func TestLocalDownloader_Fetch(t *testing.T) {
	t.Run("full file", func(t *testing.T) {
		r, err := remote.NewLocalFetcher("file://testdata/lorem.txt")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		reader, err := r.Fetch(context.Background(), nil, nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Errorf("could not read file: %v", err)
			return
		}
		if len(data) != 446 {
			t.Errorf("expected size 446, got %d", len(data))
		}
	})
	t.Run("file part", func(t *testing.T) {
		r, err := remote.NewLocalFetcher("file://testdata/lorem.txt")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		reader, err := r.Fetch(context.Background(), int64p(5), int64p(15))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Errorf("could not read file: %v", err)
			return
		}
		if !bytes.Equal(data, []byte(" ipsum dolo")) {
			t.Errorf("wrong body returned: %s\n", data)
		}

	})
	t.Run("file part (beginning)", func(t *testing.T) {
		r, err := remote.NewLocalFetcher("file://testdata/lorem.txt")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		reader, err := r.Fetch(context.Background(), int64p(0), int64p(10))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Errorf("could not read file: %v", err)
			return
		}
		if !bytes.Equal(data, []byte("Lorem ipsum")) {
			t.Errorf("wrong body returned: %s\n", data)
		}
	})
	t.Run("file part (end)", func(t *testing.T) {
		r, err := remote.NewLocalFetcher("file://testdata/lorem.txt")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		reader, err := r.Fetch(context.Background(), nil, int64p(5))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Errorf("could not read file: %v", err)
			return
		}
		if !bytes.Equal(data, []byte("rum.\n")) {
			t.Errorf("wrong body returned: %s\n", data)
		}
	})
	t.Run("non-existent file", func(t *testing.T) {
		_, err := remote.NewLocalFetcher("file://testdata/lorem_does_not_exist.txt")
		if !errors.Is(err, remote.ErrDoesNotExist) {
			t.Errorf("unexpected error, %v", err)
		}
	})
}
