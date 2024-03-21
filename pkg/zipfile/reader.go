package zipfile

import (
	"archive/zip"
	"bufio"
	"compress/flate"
	"context"
	"errors"
	"github.com/ozkatz/cloudzip/pkg/download"
	"io"
	"log/slog"
)

const (
	DefaultDownloadBufferSize = 1024 * 1024 * 16 // 16MB
	FooterHeuristicSize       = 1024 * 1024 * 4  // 4MB
)

var (
	ErrFileNotFound = errors.New("file not found")
)

type RemoteZipReader struct {
	downloader download.Downloader
	remoteUri  string
}

func NewRemoteZipReader(downloader download.Downloader, remoteUri string) *RemoteZipReader {
	return &RemoteZipReader{
		downloader: downloader,
		remoteUri:  remoteUri,
	}
}

func (rzr *RemoteZipReader) StreamDownload(ctx context.Context, f *zip.File) (io.ReadCloser, error) {
	startOffset, err := f.DataOffset()
	if err != nil {
		slog.Error("could not get offset for file", "name", f.Name, "error", err)
		return nil, err
	}
	endOffset := startOffset + int64(f.CompressedSize64) - 1
	r, err := rzr.downloader.Download(ctx, rzr.remoteUri, startOffset, endOffset)
	if err != nil {
		slog.Error("could not download file", "name", f.Name, "error", err)
		return nil, err
	}

	if f.Method == zip.Deflate {
		r = flate.NewReader(bufio.NewReaderSize(r, DefaultDownloadBufferSize)) // use default buffer size
	} else if f.Method != zip.Store {
		slog.Error("unknown compression method", "method", f.Method, "name", f.Name)
		return nil, err
	}
	return r, nil
}

func (rzr *RemoteZipReader) ListFiles(ctx context.Context) ([]*zip.File, error) {
	totalSize, err := rzr.downloader.SizeOf(ctx, rzr.remoteUri)
	if err != nil {
		slog.Error("unable to get size of uri", "uri", rzr.remoteUri, "error", err)
		return nil, err
	}
	r := download.NewLazyReader(ctx, rzr.remoteUri, rzr.downloader)
	if err := r.PrefetchFooter(ctx, FooterHeuristicSize); err != nil {
		return nil, err
	}
	reader, err := zip.NewReader(r, totalSize)
	if err != nil {
		slog.Error("unable to open zip reader", "error", err)
		return nil, err
	}
	return reader.File, nil
}

func (rzr *RemoteZipReader) CopyFile(ctx context.Context, filePath string, writer io.Writer) (int64, error) {
	downloader := download.NewDynamicDownloader()
	totalSize, err := downloader.SizeOf(ctx, rzr.remoteUri)
	if err != nil {
		slog.Error("unable to get object size", "uri", rzr.remoteUri, "error", err)
		return 0, err
	}
	lr := download.NewLazyReader(ctx, rzr.remoteUri, downloader)
	if err := lr.PrefetchFooter(ctx, FooterHeuristicSize); err != nil {
		return 0, err
	}
	reader, err := zip.NewReader(lr, totalSize)
	if err != nil {
		slog.Error("unable to open zip reader", "uri", rzr.remoteUri, "error", err)
		return 0, err
	}
	for _, f := range reader.File {
		if f.Name == filePath {
			reader, err := rzr.StreamDownload(ctx, f)
			if err != nil {
				slog.Error("unable to start download stream", "uri", rzr.remoteUri, "error", err)
				return 0, err
			}
			return io.Copy(writer, reader)
		}
	}

	slog.Error("could not find file", "uri", rzr.remoteUri, "file_path", filePath)
	return 0, ErrFileNotFound
}
