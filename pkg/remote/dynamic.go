package remote

import (
	"context"
	"fmt"
	"io"
	"net/url"
)

var _ Downloader = &DynamicDownloader{}

type DynamicDownloader struct {
	registeredDownloaders map[string]Downloader
}

func NewDynamicDownloader() *DynamicDownloader {
	s3Downloader := NewS3Downloader()
	localDownloader := NewLocalDownloader()
	return &DynamicDownloader{
		registeredDownloaders: map[string]Downloader{
			"s3":    s3Downloader,
			"s3a":   s3Downloader,
			"S3":    s3Downloader,
			"local": localDownloader,
			"file":  localDownloader,
		},
	}
}

func (d *DynamicDownloader) getDownloaderFor(uri string) (Downloader, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, ErrInvalidURI
	}
	dl, ok := d.registeredDownloaders[parsed.Scheme]
	if !ok {
		return nil, fmt.Errorf("%w: unknown scheme: %s", ErrInvalidURI, parsed.Scheme)
	}
	return dl, nil
}

func (d *DynamicDownloader) Download(ctx context.Context, uri string, offsetStart int64, offsetEnd int64) (io.ReadCloser, error) {
	dl, err := d.getDownloaderFor(uri)
	if err != nil {
		return nil, err
	}
	return dl.Download(ctx, uri, offsetStart, offsetEnd)
}

func (d *DynamicDownloader) SizeOf(ctx context.Context, uri string) (int64, error) {
	dl, err := d.getDownloaderFor(uri)
	if err != nil {
		return 0, err
	}
	return dl.SizeOf(ctx, uri)
}

type RemoteObject struct {
	uri       string
	ctx       context.Context
	downloder Downloader
}

func (r *RemoteObject) ReaderAt(start, end int64) (io.Reader, error) {
	return r.downloder.Download(r.ctx, r.uri, start, end)
}

func (r *RemoteObject) Size() (int64, error) {
	return r.downloder.SizeOf(r.ctx, r.uri)
}

func NewRemoteObject(uri string, ctx context.Context) *RemoteObject {
	downloader := NewDynamicDownloader()
	return &RemoteObject{
		uri:       uri,
		ctx:       ctx,
		downloder: downloader,
	}
}
