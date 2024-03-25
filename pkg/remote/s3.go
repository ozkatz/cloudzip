package remote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"net/url"
)

type S3Getter interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type s3ParsedUri struct {
	Bucket string
	Path   string
}

func s3getServiceForBucket(ctx context.Context, bucket string) (S3Getter, error) {
	const defaultRegion = "us-east-1"
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(defaultRegion))
	if err != nil {
		return nil, err
	}
	svc := s3.NewFromConfig(cfg)
	region, err := manager.GetBucketRegion(ctx, svc, bucket)
	if err != nil {
		if s3IsNotFoundErr(err) {
			return nil, ErrDoesNotExist
		}
		return nil, err
	}
	if region != defaultRegion {
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, err
		}
		svc = s3.NewFromConfig(cfg)
	}
	return svc, nil
}

func s3IsNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	var nf *types.NotFound
	return errors.As(err, &nf)
}

func s3parseUri(uri string) (*s3ParsedUri, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	path := parsed.Path
	if strings.HasPrefix(path, "/") {
		path = path[1:]
	}
	return &s3ParsedUri{
		Bucket: parsed.Host,
		Path:   path,
	}, nil
}

type S3ObjectFetcher struct {
	client S3Getter
	bucket string
	path   string
}

func NewS3ObjectFetcher(uri string) (*S3ObjectFetcher, error) {
	parsed, err := s3parseUri(uri)
	if err != nil {
		return nil, err
	}
	client, err := s3getServiceForBucket(context.Background(), parsed.Bucket)
	if err != nil {
		return nil, err
	}

	return &S3ObjectFetcher{
		client: client,
		bucket: parsed.Bucket,
		path:   parsed.Path,
	}, nil
}

func buildRange(offsetStart *int64, offsetEnd *int64) *string {
	if offsetStart != nil && offsetEnd != nil {
		return aws.String(fmt.Sprintf("bytes=%d-%d", *offsetStart, *offsetEnd))
	} else if offsetStart != nil {
		return aws.String(fmt.Sprintf("bytes=%d-", *offsetStart))
	} else if offsetEnd != nil {
		return aws.String(fmt.Sprintf("bytes=-%d", *offsetEnd))
	}
	return nil
}

func (s *S3ObjectFetcher) Fetch(ctx context.Context, startOffset *int64, endOffset *int64) (io.ReadCloser, error) {
	start := time.Now()
	rng := buildRange(startOffset, endOffset)
	response, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.path),
		Range:  rng,
	})
	tookMs := time.Since(start).Milliseconds()
	if s3IsNotFoundErr(err) {
		slog.Warn("s3.GetObject", "range", rng, "bucket", s.bucket, "key", s.path, "took_ms", tookMs, "error", "NotFound")
		return nil, ErrDoesNotExist
	} else if err != nil {
		slog.Error("s3.GetObject", "range", rng, "bucket", s.bucket, "key", s.path, "took_ms", tookMs, "error", err)
		return nil, err
	}
	slog.Debug("s3.GetObject", "range", rng, "bucket", s.bucket, "key", s.path, "took_ms", tookMs, "error", nil)
	return response.Body, nil
}
