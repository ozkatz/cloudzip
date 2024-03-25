package remote

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"io"
	"log/slog"
	"net/url"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3ParsedUri struct {
	Bucket string
	Path   string
}

var _ Downloader = &S3Downloader{}

type S3Client interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

type S3Downloader struct {
	lock         *sync.Mutex
	serviceCache map[string]S3Client
}

func NewS3Downloader() *S3Downloader {
	return &S3Downloader{
		lock:         &sync.Mutex{},
		serviceCache: make(map[string]S3Client),
	}
}

func buildRange(offsetStart int64, offsetEnd int64) *string {
	if offsetStart != 0 && offsetEnd != 0 {
		return aws.String(fmt.Sprintf("bytes=%d-%d", offsetStart, offsetEnd))
	} else if offsetStart != 0 {
		return aws.String(fmt.Sprintf("bytes=%d-", offsetStart))
	} else if offsetEnd != 0 {
		return aws.String(fmt.Sprintf("bytes=-%d", offsetEnd))
	}
	return nil
}

func (d *S3Downloader) getServiceForBucket(ctx context.Context, bucket string) (S3Client, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if svc, ok := d.serviceCache[bucket]; ok {
		return svc, nil
	}
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
	d.serviceCache[bucket] = svc
	return svc, nil
}

func s3IsNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	var nf *types.NotFound
	return errors.As(err, &nf)
}

func (d *S3Downloader) parseUri(uri string) (*s3ParsedUri, error) {
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

func (d *S3Downloader) Download(ctx context.Context, uri string, offsetStart int64, offsetEnd int64) (io.ReadCloser, error) {
	parsed, err := d.parseUri(uri)
	if err != nil {
		return nil, err
	}
	svc, err := d.getServiceForBucket(ctx, parsed.Bucket)
	if err != nil {
		return nil, err
	}

	rng := buildRange(offsetStart, offsetEnd)

	slog.Debug("s3:GetObject", "uri", uri, "range", rng)
	out, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(parsed.Bucket),
		Key:    aws.String(parsed.Path),
		Range:  rng,
	})

	if s3IsNotFoundErr(err) {
		return nil, ErrDoesNotExist
	} else if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (d *S3Downloader) SizeOf(ctx context.Context, uri string) (int64, error) {
	parsed, err := d.parseUri(uri)
	if err != nil {
		return 0, err
	}
	svc, err := d.getServiceForBucket(ctx, parsed.Bucket)
	if err != nil {
		return 0, err
	}
	out, err := svc.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(parsed.Bucket),
		Key:    aws.String(parsed.Path),
	})
	slog.Debug("s3:HeadObject", "uri", uri, "bucket", parsed.Bucket, "key", parsed.Path, "error", err)
	if s3IsNotFoundErr(err) {
		return 0, ErrDoesNotExist
	} else if err != nil {
		return 0, err
	}
	sizeBytes := aws.ToInt64(out.ContentLength)
	return sizeBytes, nil
}
