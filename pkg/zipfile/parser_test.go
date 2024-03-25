package zipfile_test

import (
	"context"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"hash/crc32"
	"io"
	"testing"

	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

type adapter struct {
	f   remote.Fetcher
	ctx context.Context
}

func (a *adapter) Fetch(start, end *int64) (io.Reader, error) {
	return a.f.Fetch(a.ctx, start, end)
}

func parser(uri string) (*zipfile.CentralDirectoryParser, error) {
	fetcher, err := remote.Object(uri)
	if err != nil {
		return nil, err
	}
	return zipfile.NewCentralDirectoryParser(&adapter{
		ctx: context.Background(),
		f:   fetcher,
	}), nil
}

func TestCentralDirectoryParser_GetCentralDirectory(t *testing.T) {
	p, err := parser("file://testdata/big_directory.zip")
	if err != nil {
		t.Errorf("unexpected error opening zip file: %v", err)
		return
	}

	files, err := p.GetCentralDirectory()
	if len(files) != 150000 {
		t.Errorf("expected 150,000 files, got %d", len(files))
	}
}

func TestCentralDirectoryParser_GetCentralDirectory64(t *testing.T) {
	p, err := parser("file://testdata/huge.zip")
	if err != nil {
		t.Errorf("unexpected error opening zip file: %v", err)
		return
	}
	files, err := p.GetCentralDirectory()
	if len(files) != 1 {
		t.Errorf("expected 1 files, got %d", len(files))
		return
	}
}

func TestCentralDirectoryParser_Read(t *testing.T) {
	p, err := parser("file://testdata/regular.zip")
	if err != nil {
		t.Errorf("unexpected error opening zip file: %v", err)
		return
	}
	files, err := p.GetCentralDirectory()
	if len(files) != 7 {
		t.Errorf("expected 7 files, got %d", len(files))
		return
	}
	r, err := p.Read("foo/bar.txt")
	if err != nil {
		t.Errorf("unexpected error reading file: %v", err)
		return
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Errorf("unexpected error reading file: %v", err)
		return
	}
	if string(data) != "file in a directory!\n" {
		t.Errorf("got wrong string: %s\n", string(data))
	}
}

func TestCentralDirectoryParser_GetCentralDirectory64FromStdlib(t *testing.T) {
	p, err := parser("file://testdata/zip64.zip")
	if err != nil {
		t.Errorf("unexpected error opening zip file: %v", err)
		return
	}
	files, err := p.GetCentralDirectory()
	if len(files) != 1 {
		t.Errorf("expected 1 files, got %d", len(files))
		return
	}
	r, err := p.Read("README")
	if err != nil {
		t.Errorf("unexpected error reading file: %v", err)
		return
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Errorf("unexpected error reading file: %v", err)
		return
	}
	if string(data) != "This small file is in ZIP64 format.\n" {
		t.Errorf("got wrong string: %s\n", string(data))
	}
}

func TestNewRemoteZipReader(t *testing.T) {
	zipFiles := []string{
		"file://testdata/regular.zip",
		"file://testdata/huge.zip",
		"file://testdata/uncompressed.zip",
		"file://testdata/zip64.zip",
	}

	for _, zipFile := range zipFiles {
		testZip(t, zipFile)
	}
}

func testZip(t *testing.T, path string) {
	t.Run(path, func(t *testing.T) {
		p, err := parser(path)
		if err != nil {
			t.Errorf("unexpected error opening zip file: %v", err)
			return
		}
		files, err := p.GetCentralDirectory()
		if err != nil {
			t.Errorf("unexpected error listing zip file: %v", err)
			return
		}
		for _, f := range files {
			if f.Mode.IsDir() {
				continue
			}
			t.Run(f.FileName, func(t *testing.T) {
				r, err := p.Read(f.FileName)
				if err != nil {
					t.Errorf("could not open reader for file: %v", err)
					return
				}
				data, err := io.ReadAll(r)
				if err != nil {
					t.Errorf("could not read file: %v after %d bytes", err, len(data))
					return
				}
				h := crc32.NewIEEE()
				_, _ = h.Write(data)
				crc := h.Sum32()
				if crc != f.CRC32Uncompressed {
					t.Errorf("unepxected CRC32 - expected %d got %d", f.CRC32Uncompressed, crc)
				}
			})
		}
	})
}
