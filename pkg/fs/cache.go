package fs

import (
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

type ZipFileURI string

type DirectoryCache struct {
	lock        sync.Mutex
	directories map[ZipFileURI][]*zipfile.CDR
}

func (c *DirectoryCache) Get(uri ZipFileURI) ([]*zipfile.CDR, bool) {
	c.lock.Lock()
	dir, ok := c.directories[uri]
	c.lock.Unlock()
	return dir, ok
}

func (c *DirectoryCache) Set(uri ZipFileURI, directory []*zipfile.CDR) {
	c.lock.Lock()
	c.directories[uri] = directory
	c.lock.Unlock()
}

type FileCacheKey struct {
	zipfile  ZipFileURI
	path     string
	checksum uint32
}

func (k FileCacheKey) Id() string {
	keyHash := sha1.New()
	keyHash.Write([]byte(k.zipfile))
	keyHash.Write([]byte(k.path))
	keyHash.Write([]byte(strconv.Itoa(int(k.checksum))))
	hashBytes := keyHash.Sum(nil)
	return fmt.Sprintf("%x", hashBytes)
}

type FileCache struct {
	dir string
}

func NewFileCache(dir string) *FileCache {
	return &FileCache{dir: dir}
}
func (c *FileCache) Get(key FileCacheKey) (*os.File, error) {
	path := filepath.Join(c.dir, key.Id())
	return os.Open(path)
}

func (c *FileCache) Set(key FileCacheKey, content io.ReadCloser, expected int64) (*os.File, error) {
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
