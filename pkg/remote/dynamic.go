package remote

import (
	"fmt"
	"io"
	"log/slog"
	"net/url"
)

func DummyLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.LevelError,
	}))
}

type ObjectOpt func(f Fetcher)

func WithLogger(logger *slog.Logger) ObjectOpt {
	return func(f Fetcher) {
		if lf, ok := f.(CanSetLogger); ok {
			lf.setLogger(logger)
		}
	}
}

type CanSetLogger interface {
	Fetcher
	setLogger(logger *slog.Logger)
}

func Object(uri string, opts ...ObjectOpt) (Fetcher, error) {
	f, err := getObject(uri)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(f)
	}
	return f, nil
}

func getObject(uri string) (Fetcher, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, ErrInvalidURI
	}
	switch parsed.Scheme {
	case "s3", "S3", "s3a":
		return NewS3ObjectFetcher(uri)
	case "local", "file":
		return NewLocalFetcher(uri)
	case "http", "https":
		return NewHttpFetcher(uri)
	case "kaggle":
		return NewKaggleFetcher(uri)
	case "lakefs":
		return NewLakeFSFetcher(uri)
	}

	return nil, fmt.Errorf("%w: unknown scheme: %s", ErrInvalidURI, parsed.Scheme)
}
