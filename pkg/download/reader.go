package download

import (
	"bytes"
	"context"
	"io"
)

type LazyReaderAt struct {
	dl        Downloader
	uri       string
	totalSize int64
	ctx       context.Context

	footer *bytes.Buffer
}

func NewLazyReader(ctx context.Context, uri string, dl Downloader) *LazyReaderAt {
	return &LazyReaderAt{
		dl:     dl,
		uri:    uri,
		ctx:    ctx,
		footer: bytes.NewBuffer(nil),
	}
}

func (l *LazyReaderAt) getSize() (int64, error) {
	if l.totalSize == 0 {
		ts, err := l.dl.SizeOf(l.ctx, l.uri)
		if err != nil {
			return 0, err
		}
		l.totalSize = ts
	}
	return l.totalSize, nil
}

func (l *LazyReaderAt) PrefetchFooter(ctx context.Context, size int64) error {
	if int64(l.footer.Len()) >= size {
		// we're already covered
		return nil
	}
	totalSize, err := l.getSize()
	if err != nil {
		return err
	}
	lastByte := totalSize - 1
	firstByte := lastByte - size
	if firstByte < 0 {
		firstByte = 0
	}
	r, err := l.dl.Download(ctx, l.uri, firstByte, lastByte)
	if err != nil {
		return err
	}
	_, err = io.Copy(l.footer, r)
	return err
}

func (l *LazyReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	totalSize, err := l.getSize()
	if err != nil {
		return 0, err
	}

	startOffset := off
	endOffset := startOffset + int64(len(p))

	bufferSize := int64(l.footer.Len())
	var r io.Reader
	if startOffset > (totalSize - bufferSize) {
		// we can read this from buffer
		fr := bytes.NewReader(l.footer.Bytes())
		_, err = fr.Seek(off-(totalSize-bufferSize), io.SeekStart)
		if err != nil {
			return
		}
		r = fr
	} else {
		var rc io.ReadCloser
		rc, err = l.dl.Download(l.ctx, l.uri, startOffset, endOffset)
		if err != nil {
			return
		}
		defer func() {
			_ = rc.Close()
		}()
		r = rc
	}
	return r.Read(p)
}
