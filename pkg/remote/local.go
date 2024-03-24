package remote

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path"
)

var _ Downloader = &LocalDownloader{}

type LocalDownloader struct {
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

func NewLocalDownloader() *LocalDownloader {
	return &LocalDownloader{}
}

func (l *LocalDownloader) parseUri(uri string) (string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", ErrInvalidURI
	}
	return path.Clean(path.Join(parsed.Host, parsed.Path)), nil
}

func (l *LocalDownloader) Download(_ context.Context, uri string, offsetStart int64, offsetEnd int64) (io.ReadCloser, error) {
	filePath, err := l.parseUri(uri)
	if err != nil {
		return nil, err
	}
	reader, err := os.Open(filePath)
	if os.IsNotExist(err) {
		return nil, ErrDoesNotExist
	} else if err != nil {
		return nil, err
	}
	_, err = reader.Seek(offsetStart, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return &localReader{
		original:    reader,
		limitReader: io.LimitReader(reader, offsetEnd+1-offsetStart),
	}, nil
}

func (l *LocalDownloader) SizeOf(_ context.Context, uri string) (int64, error) {
	filePath, err := l.parseUri(uri)
	if err != nil {
		return 0, err
	}

	info, err := os.Stat(filePath)
	if errors.Is(err, os.ErrNotExist) {
		return 0, ErrDoesNotExist
	} else if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
