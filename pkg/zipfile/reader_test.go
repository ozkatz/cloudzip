package zipfile_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"github.com/ozkatz/cloudzip/pkg/download"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
	"testing"
)

func TestRemoteZipReader_ListFiles(t *testing.T) {
	dl := download.NewLocalDownloader()

	t.Run("compressed file", func(t *testing.T) {
		r := zipfile.NewRemoteZipReader(dl, "file://testdata/regular.zip")
		listing, err := r.ListFiles(context.Background())
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		if len(listing) != 7 {
			t.Errorf("unexpected listing size: %d, expected 7", len(listing))
			return
		}
	})

	t.Run("uncompressed file", func(t *testing.T) {
		r := zipfile.NewRemoteZipReader(dl, "file://testdata/uncompressed.zip")
		listing, err := r.ListFiles(context.Background())
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		if len(listing) != 7 {
			t.Errorf("unexpected listing size: %d, expected 7", len(listing))
			return
		}
	})

	t.Run("malformed file", func(t *testing.T) {
		r := zipfile.NewRemoteZipReader(dl, "file://testdata/malformed.zip")
		_, err := r.ListFiles(context.Background())
		if !errors.Is(err, zip.ErrFormat) {
			t.Errorf("unexpected error: %v - expectged zip.ErrFormat", err)
		}
	})

	t.Run("non existent file", func(t *testing.T) {
		r := zipfile.NewRemoteZipReader(dl, "file://testdata/missing.zip")
		_, err := r.ListFiles(context.Background())
		if !errors.Is(err, download.ErrDoesNotExist) {
			t.Errorf("unexpected error: %v - expectged download.ErrDoesNotExist", err)
		}
	})

}

func TestRemoteZipReader_CopyFile(t *testing.T) {
	dl := download.NewLocalDownloader()

	t.Run("compressed file", func(t *testing.T) {
		r := zipfile.NewRemoteZipReader(dl, "file://testdata/regular.zip")
		buf := bytes.NewBuffer(nil)
		n, err := r.CopyFile(context.Background(), "a/b/c/d.txt", buf)
		if err != nil {
			t.Errorf("unexpected error reading file: %v", err)
			return
		}
		if n != 30 {
			t.Errorf("unexpected file length: %d", n)
			return
		}
		if buf.String() != "file inside a deep directory\n\n" {
			t.Errorf("unepxected file content: %s", buf.String())
		}
	})

	t.Run("uncompressed file", func(t *testing.T) {
		r := zipfile.NewRemoteZipReader(dl, "file://testdata/uncompressed.zip")
		buf := bytes.NewBuffer(nil)
		n, err := r.CopyFile(context.Background(), "a/b/c/d.txt", buf)
		if err != nil {
			t.Errorf("unexpected error reading file: %v", err)
			return
		}
		if n != 30 {
			t.Errorf("unexpected file length: %d", n)
			return
		}
		if buf.String() != "file inside a deep directory\n\n" {
			t.Errorf("unepxected file content: %s", buf.String())
		}
	})

	t.Run("malformed file", func(t *testing.T) {
		r := zipfile.NewRemoteZipReader(dl, "file://testdata/malformed.zip")
		buf := bytes.NewBuffer(nil)
		_, err := r.CopyFile(context.Background(), "a/b/c/d.txt", buf)
		if !errors.Is(err, zip.ErrFormat) {
			t.Errorf("unexpected error reading file: %v", err)
			return
		}
	})

	t.Run("missing inner file", func(t *testing.T) {
		r := zipfile.NewRemoteZipReader(dl, "file://testdata/regular.zip")
		buf := bytes.NewBuffer(nil)
		_, err := r.CopyFile(context.Background(), "a/b/c/d/e/f/g.txt", buf)
		if !errors.Is(err, zipfile.ErrFileNotFound) {
			t.Errorf("unexpected error reading file: %v", err)
			return
		}
	})

	t.Run("missing zip file", func(t *testing.T) {
		r := zipfile.NewRemoteZipReader(dl, "file://testdata/no_such_file.zip")
		buf := bytes.NewBuffer(nil)
		_, err := r.CopyFile(context.Background(), "a/b/c/d/e/f/g.txt", buf)
		if !errors.Is(err, download.ErrDoesNotExist) {
			t.Errorf("unexpected error reading file: %v", err)
			return
		}
	})

}
