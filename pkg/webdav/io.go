package webdav

import (
	"context"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
	"io"
	"os"
	"path"
	"time"
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

func getCDRs(ctx context.Context, zipFilePath string, cache *directoryCache) ([]*zipfile.CDR, error) {
	files, ok := cache.GetCentralDirectory(zipFilePath)
	if !ok {
		obj, err := remote.Object(zipFilePath)
		if err != nil {
			return nil, err
		}
		zip := zipfile.NewCentralDirectoryParser(&adapter{
			f:   obj,
			ctx: ctx,
		})
		files, err = zip.GetCentralDirectory()
		if err != nil {
			return nil, err
		}
		cache.SetCentralDirectory(zipFilePath, files)
	}
	return files, nil
}

func listDirectory(ctx context.Context, cache *directoryCache, zipFilePath, filePath string) ([]*zipFileInfo, error) {
	files, err := getCDRs(ctx, zipFilePath, cache)
	if err != nil {
		return nil, err
	}
	// build records
	results := make([]*zipFileInfo, 0)
	for _, f := range files {
		current := path.Clean(f.FileName)
		// filter only direct descendants of filePath
		if !IsDirectDescendant(current, filePath) {
			continue
		}
		results = append(results, &zipFileInfo{
			path: current,
			stats: &fileStat{
				sizeBytes: f.UncompressedSizeBytes,
				checksum:  f.CRC32Uncompressed,
				Mtime:     f.Modified,
				mode:      f.Mode,
			},
		})
	}
	return results, nil
}

func getDirInfo(ctx context.Context, cache *directoryCache, zipFilePath, filePath string) (*zipFileInfo, error) {
	if filePath == "" {
		return &zipFileInfo{
			path: "",
			stats: &fileStat{
				Mtime: time.Now(),
				mode:  os.ModeDir,
			},
		}, nil
	}
	files, err := getCDRs(ctx, zipFilePath, cache)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		absolutePath := f.FileName
		if !path.IsAbs(absolutePath) {
			absolutePath = "/" + absolutePath
		}
		if path.Clean(filePath) == path.Clean(absolutePath) && f.Mode.IsDir() {
			return &zipFileInfo{
				path: path.Clean(absolutePath),
				stats: &fileStat{
					Mtime: time.Now(),
					mode:  f.Mode,
				},
			}, nil
		}
	}

	return nil, os.ErrNotExist
}

func getFileInfo(ctx context.Context, cache *directoryCache, zipFilePath, filePath string) (*zipFileInfo, error) {
	files, err := getCDRs(ctx, zipFilePath, cache)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		requested := path.Clean(filePath)
		current := path.Clean(f.FileName)
		if requested != current {
			continue
		}
		// found!
		return &zipFileInfo{
			path: current,
			stats: &fileStat{
				sizeBytes: f.UncompressedSizeBytes,
				checksum:  f.CRC32Uncompressed,
				Mtime:     f.Modified,
				mode:      f.Mode,
			},
		}, nil
	}
	return nil, os.ErrNotExist
}

func readFile(ctx context.Context, zipFilePath, filePath string) (io.ReadCloser, error) {
	if path.IsAbs(filePath) {
		filePath = filePath[0 : len(filePath)-1] // zip paths don't start with "/"
	}
	obj, err := remote.Object(zipFilePath)
	if err != nil {
		return nil, err
	}
	zip := zipfile.NewCentralDirectoryParser(&adapter{
		f:   obj,
		ctx: ctx,
	})
	r, err := zip.Read(path.Clean(filePath))
	if err != nil {
		return nil, err
	}
	return io.NopCloser(r), nil
}
