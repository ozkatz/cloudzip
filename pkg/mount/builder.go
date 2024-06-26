package mount

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/ozkatz/cloudzip/pkg/mount/commonfs"
	"github.com/ozkatz/cloudzip/pkg/mount/procfs"
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

func getOpenerFor(logger *slog.Logger, zipPath string, record *zipfile.CDR, cache *commonfs.FileCache) commonfs.OpenFn {
	return func(fullPath string, flag int, perm os.FileMode) (commonfs.FileLike, error) {
		filename := path.Clean(record.FileName)
		key := asKey(zipPath, filename, strconv.Itoa(int(record.CRC32Uncompressed)))
		f, err := cache.Get(key)
		if errors.Is(err, os.ErrNotExist) {
			// cache miss!
			remoteZip, err := remote.Object(zipPath, remote.WithLogger(logger))
			if err != nil {
				return nil, err
			}
			ctx := context.Background()
			fetcher := zipfile.NewStorageAdapter(ctx, remoteZip)
			reader, err := zipfile.ReaderForRecord(record, fetcher)
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

func BuildZipTree(ctx context.Context, logger *slog.Logger, cacheDir, remoteZipURI string, procAttrs map[string]interface{}) (commonfs.Tree, error) {
	obj, err := remote.Object(remoteZipURI, remote.WithLogger(logger))
	if err != nil {
		return nil, err
	}
	zip := zipfile.NewStorageAdapter(ctx, obj)
	parser := zipfile.NewCentralDirectoryParser(zip)
	cdr, err := parser.GetCentralDirectory()
	if err != nil {
		return nil, err
	}
	startTime := time.Now()

	// build index
	infos := make(commonfs.FileInfoList, 0)
	cache := commonfs.NewFileCache(cacheDir)
	for _, f := range cdr {
		infos = append(infos, commonfs.ImmutableInfo(
			f.FileName,
			f.Modified,
			f.Mode,
			int64(f.UncompressedSizeBytes),
			getOpenerFor(logger, remoteZipURI, f, cache),
		))
	}

	// "proc" filesystem exposed to users
	infos = append(infos, procfs.NewProcFile(".cz/server.pid", []byte(strconv.Itoa(os.Getpid())), startTime))
	infos = append(infos, procfs.NewProcFile(".cz/cachedir", []byte(cacheDir), startTime))
	infos = append(infos, procfs.NewProcFile(".cz/source", []byte(remoteZipURI), startTime))
	for k, v := range procAttrs {
		infos = append(infos, procfs.NewProcFile(fmt.Sprintf(".cz/%s", k),
			[]byte(fmt.Sprintf("%s", v)),
			startTime))
	}
	// sort it
	sort.Sort(infos)
	tree := commonfs.NewInMemoryTreeBuilder(func(entry string) *commonfs.FileInfo {
		return commonfs.ImmutableDir(entry, startTime)
	})
	err = tree.Index(infos)
	if err != nil {
		return nil, err
	}
	return tree, nil
}
