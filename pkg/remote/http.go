package remote

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type HttpFetcher struct {
	url string
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func NewHttpFetcher(uri string) (*HttpFetcher, error) {
	return &HttpFetcher{url: uri}, nil
}

func (h *HttpFetcher) Fetch(ctx context.Context, startOffset *int64, endOffset *int64) (io.ReadCloser, error) {
	rangeHeader := buildRange(startOffset, endOffset)
	req, err := http.NewRequest(http.MethodGet, h.url, nil)
	if err != nil {
		return nil, err
	}
	if rangeHeader != nil {
		req.Header.Set("Range", *rangeHeader)
	}
	req = req.WithContext(ctx)
	start := time.Now()
	response, err := http.DefaultClient.Do(req)
	tookMs := time.Since(start).Milliseconds()
	if err != nil {
		slog.Error("http.Get", "range", rangeHeader, "url", h.url, "took_ms", tookMs, "error", err)
		return nil, err
	}
	if response.StatusCode == http.StatusNotFound {
		slog.Warn("http.Get", "range", rangeHeader, "url", h.url, "took_ms", tookMs, "error", "NotFound")
		return nil, ErrDoesNotExist
	}
	slog.Debug("http.Get", "range", rangeHeader, "url", h.url, "took_ms", tookMs, "error", nil)
	return response.Body, nil
}
