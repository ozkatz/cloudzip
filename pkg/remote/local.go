package remote

import (
	"context"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"syscall"
)

type LocalFetcher struct {
	handle *os.File
}

func NewLocalFetcher(uri string) (*LocalFetcher, error) {
	filePath, err := localParseUri(uri)
	if err != nil {
		return nil, err
	}
	handle, err := os.Open(filePath)
	if os.IsNotExist(err) {
		return nil, ErrDoesNotExist
	} else if err != nil {
		return nil, err
	}

	return &LocalFetcher{
		handle: handle,
	}, nil
}

func (l *LocalFetcher) Fetch(_ context.Context, startOffset *int64, endOffset *int64) (io.ReadCloser, error) {
	if startOffset == nil && endOffset == nil {
		// no range, read the whole thing
		_, err := l.handle.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
		return l.handle, nil
	}

	if startOffset == nil && endOffset != nil {
		// only end offset, read the last endOffset bytes
		_, err := l.handle.Seek(*endOffset*-1, io.SeekEnd)
		if pathErr, ok := err.(*fs.PathError); ok && pathErr.Err == syscall.EINVAL {
			_, err := l.handle.Seek(0, io.SeekStart)
			if err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
		return l.handle, nil
	}

	if startOffset != nil && endOffset != nil {
		// start and end specified
		_, err := l.handle.Seek(*startOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		return &localReader{
			original:    l.handle,
			limitReader: io.LimitReader(l.handle, *endOffset+1-*startOffset),
		}, nil
	}

	//if startOffset != nil && endOffset == nil
	_, err := l.handle.Seek(*startOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return l.handle, nil

}

type localReader struct {
	original    io.Closer
	limitReader io.Reader
}

func (l *localReader) Read(p []byte) (n int, err error) {
	return l.limitReader.Read(p)
}

func (l *localReader) Close() error {
	return l.original.Close()
}

func localParseUri(uri string) (string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", ErrInvalidURI
	}
	return path.Clean(path.Join(parsed.Host, parsed.Path)), nil
}
