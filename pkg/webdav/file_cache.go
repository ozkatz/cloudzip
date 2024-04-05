package webdav

import (
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type fileCacheKey struct {
	zipfile  string
	path     string
	checksum uint32
}

func (k fileCacheKey) Id() string {
	keyHash := sha1.New()
	keyHash.Write([]byte(k.zipfile))
	keyHash.Write([]byte(k.path))
	keyHash.Write([]byte(strconv.Itoa(int(k.checksum))))
	hashBytes := keyHash.Sum(nil)
	return fmt.Sprintf("%x", hashBytes)
}

type fileCache struct {
	dir string
}

func newFileCache(dir string) *fileCache {
	return &fileCache{dir: dir}
}

func (c *fileCache) Get(key fileCacheKey) (*os.File, error) {
	path := filepath.Join(c.dir, key.Id())
	return os.Open(path)
}

func (c *fileCache) Set(key fileCacheKey, content io.ReadCloser, expected int64) (*os.File, error) {
	path := filepath.Join(c.dir, fmt.Sprintf("%s-w", key.Id()))
	out, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	n, err := io.Copy(out, content)
	if err != nil {
		// we now have a bad file on our hands
		_ = out.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if expected > 0 && n != expected {
		return nil, os.ErrInvalid
	}

	err = out.Close()
	if err != nil {
		return nil, err
	}
	// make available
	err = os.Rename(path, filepath.Join(c.dir, key.Id()))
	if err != nil {
		return nil, err
	}
	f, err := c.Get(key)
	return f, err
}
