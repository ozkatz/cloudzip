package webdav

import (
	"sync"

	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

// directoryCache stores Zip central directories in memory
// TODO(ozkatz): currently this assumes mounted zip files are never overwritten, so safe to cache indefinitely
type directoryCache struct {
	lock        *sync.RWMutex
	directories map[string][]*zipfile.CDR
}

func newMetadataCache() *directoryCache {
	return &directoryCache{
		directories: make(map[string][]*zipfile.CDR, 0),
		lock:        &sync.RWMutex{},
	}
}

func (c *directoryCache) SetCentralDirectory(zipFilePath string, directory []*zipfile.CDR) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.directories[zipFilePath] = directory
}

func (c *directoryCache) GetCentralDirectory(zipFilePath string) ([]*zipfile.CDR, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	directory, ok := c.directories[zipFilePath]
	return directory, ok
}
