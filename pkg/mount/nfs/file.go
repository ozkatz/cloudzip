package nfs

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/ozkatz/cloudzip/pkg/mount/fs"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

func asKey(strs ...string) string {
	h := sha1.New()
	for _, str := range strs {
		_, _ = h.Write([]byte(str))
	}
	out := h.Sum(nil)
	return hex.EncodeToString(out)
}

func getOpenerFor(zipPath string, record *zipfile.CDR, cache *fs.FileCache) fs.OpenFn {
	return func(fullPath string, flag int, perm os.FileMode) (fs.FileLike, error) {
		filename := path.Clean(record.FileName)
		key := asKey(zipPath, filename, strconv.Itoa(int(record.CRC32Uncompressed)))
		f, err := cache.Get(key)
		if errors.Is(err, os.ErrNotExist) {
			// cache miss!
			remoteZip, err := remote.Object(zipPath)
			if err != nil {
				return nil, err
			}
			ctx := context.Background()
			fetcher := zipfile.NewStorageAdapter(ctx, remoteZip)
			zip := zipfile.NewCentralDirectoryParser(fetcher)
			reader, err := zip.Read(filename)
			if err != nil {
				return nil, err
			}
			f, err = cache.Set(key, io.NopCloser(reader), int64(record.UncompressedSizeBytes))
			return f, err
		} else if err != nil {
			return nil, err
		}
		// cache hit!
		return f, err
	}
}
