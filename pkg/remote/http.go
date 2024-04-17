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
	url    string
	logger *slog.Logger
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func NewHttpFetcher(uri string) (*HttpFetcher, error) {
	return &HttpFetcher{
		url:    uri,
		logger: DummyLogger(),
	}, nil
}

func (h *HttpFetcher) setLogger(logger *slog.Logger) {
	h.logger = logger
}

func (h *HttpFetcher) Fetch(ctx context.Context, startOffset *int64, endOffset *int64) (io.ReadCloser, error) {
	rangeHeader := buildRange(startOffset, endOffset)
	req, err := http.NewRequest(http.MethodGet, h.url, nil)
	if err != nil {
		return nil, err
	}
	rangeHeaderStr := ""
	if rangeHeader != nil {
		rangeHeaderStr = *rangeHeader
		req.Header.Set("Range", rangeHeaderStr)
	}
	req = req.WithContext(ctx)
	start := time.Now()
	response, err := http.DefaultClient.Do(req)
	tookMs := time.Since(start).Milliseconds()
	if err != nil {
		h.logger.ErrorContext(ctx, "http.Get", "range", rangeHeaderStr, "url", h.url, "took_ms", tookMs, "error", err)
		return nil, err
	}
	if response.StatusCode == http.StatusNotFound {
		h.logger.WarnContext(ctx, "http.Get", "range", rangeHeaderStr, "url", h.url, "took_ms", tookMs, "error", "NotFound")
		return nil, ErrDoesNotExist
	}
	h.logger.DebugContext(ctx, "http.Get", "range", rangeHeaderStr, "url", h.url, "took_ms", tookMs, "error", nil)
	return response.Body, nil
}
