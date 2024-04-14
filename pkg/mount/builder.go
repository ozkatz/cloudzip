package mount

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/ozkatz/cloudzip/pkg/mount/fs"
	"github.com/ozkatz/cloudzip/pkg/mount/index"
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

func BuildZipTree(ctx context.Context, cacheDir, remoteZipURI string, procAttrs map[string]interface{}) (index.Tree, error) {
	obj, err := remote.Object(remoteZipURI)
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
	infos := make(fs.FileInfoList, 0)
	cache := fs.NewFileCache(cacheDir)
	for _, f := range cdr {
		infos = append(infos, fs.ImmutableInfo(
			f.FileName,
			f.Modified,
			f.Mode,
			int64(f.UncompressedSizeBytes),
			getOpenerFor(remoteZipURI, f, cache),
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
	tree := index.NewInMemoryTreeBuilder(func(entry string) *fs.FileInfo {
		return fs.ImmutableDir(entry, startTime)
	})
	err = tree.Index(infos)
	if err != nil {
		return nil, err
	}
	return tree, nil
}
