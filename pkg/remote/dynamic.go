package remote

import (
	"fmt"
	"net/url"
)

func Object(uri string) (Fetcher, error) {
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
	}

	return nil, fmt.Errorf("%w: unknown scheme: %s", ErrInvalidURI, parsed.Scheme)
}
