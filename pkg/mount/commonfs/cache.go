package commonfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type FileCache struct {
	dir string
}

func NewFileCache(dir string) *FileCache {
	return &FileCache{dir: dir}
}

func (c *FileCache) Get(key string) (*os.File, error) {
	path := filepath.Join(c.dir, key)
	return os.Open(path)
}

func (c *FileCache) Set(key string, content io.ReadCloser, expected int64) (*os.File, error) {
	path := filepath.Join(c.dir, fmt.Sprintf("%s-w", key))
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
	err = os.Rename(path, filepath.Join(c.dir, key))
	if err != nil {
		return nil, err
	}
	f, err := c.Get(key)
	return f, err
}
