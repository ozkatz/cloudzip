package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mitchellh/go-homedir"
)

const (
	KaggleConfig        = "~/.kaggle/kaggle.json"
	KaggleKeyFileEnvVar = "KAGGLE_KEY_FILE"
	KaggleApiEndpoint   = "https://www.kaggle.com/api/v1"
)

type KaggleFetcher struct {
	uri             string
	cacheDatasetUrl string
	cacheExpiresAt  time.Time
	l               *sync.Mutex
}

type kaggleCredentials struct {
	Username string `json:"username"`
	Key      string `json:"key"`
}

func getKaggleCredentials() (*kaggleCredentials, error) {
	filePath := os.Getenv(KaggleKeyFileEnvVar)
	if filePath == "" {
		filePath = KaggleConfig
	}
	filePath, err := homedir.Expand(filePath)
	if err != nil {
		return nil, err
	}
	handle, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	creds := &kaggleCredentials{}
	if err := json.NewDecoder(handle).Decode(creds); err != nil {
		return nil, err
	}
	return creds, nil
}

var _ Fetcher = &KaggleFetcher{}

func NewKaggleFetcher(uri string) (*KaggleFetcher, error) {
	return &KaggleFetcher{
		uri:             uri,
		l:               &sync.Mutex{},
		cacheDatasetUrl: "",
	}, nil
}

func (k *KaggleFetcher) parseUri() (string, error) {
	parts, err := url.Parse(k.uri)
	if err != nil {
		return "", err
	}
	slug := parts.Host
	dataset := strings.TrimPrefix(parts.EscapedPath(), "/")
	return fmt.Sprintf("%s/datasets/download/%s/%s",
		KaggleApiEndpoint,
		url.QueryEscape(slug),
		url.QueryEscape(dataset)), nil
}

func (k *KaggleFetcher) fetchDatasetUrl() (string, error) {
	creds, err := getKaggleCredentials()
	if err != nil {
		return "", err
	}
	apiUrl, err := k.parseUri()
	req, err := http.NewRequest(http.MethodGet, apiUrl, nil)
	if err != nil {
		return "", err
	}
	auth := fmt.Sprintf("Basic %s", basicAuth(creds.Username, creds.Key))
	req.Header.Add("Authorization", auth)
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Do(req)
	if err != nil {
		return "", err
	}
	if response.StatusCode != http.StatusFound {
		return "", ErrDoesNotExist
	}
	return response.Header.Get("Location"), nil
}

func (k *KaggleFetcher) Fetch(ctx context.Context, startOffset *int64, endOffset *int64) (io.ReadCloser, error) {
	// ensure we have a valid url to use
	const defaultRedirectExpiry = time.Minute * 5 // real URLs typically last for much longer
	var datasetUrl string
	k.l.Lock()
	if k.cacheDatasetUrl != "" && !k.cacheExpiresAt.IsZero() {
		if time.Now().Add(defaultRedirectExpiry).Before(k.cacheExpiresAt) {
			datasetUrl = k.cacheDatasetUrl
		}
	}
	if datasetUrl == "" {
		var err error
		datasetUrl, err = k.fetchDatasetUrl()
		if err != nil {
			return nil, err
		}
		k.cacheDatasetUrl = datasetUrl
		k.cacheExpiresAt = time.Now().Add(defaultRedirectExpiry)
	}
	k.l.Unlock()

	// now fetch the redirected location
	req, err := http.NewRequest(http.MethodGet, datasetUrl, nil)
	if err != nil {
		return nil, err
	}
	rangeHeader := buildRange(startOffset, endOffset)
	if rangeHeader != nil {
		req.Header.Set("Range", *rangeHeader)
	}
	req = req.WithContext(ctx)
	start := time.Now()
	response, err := http.DefaultClient.Do(req)
	tookMs := time.Since(start).Milliseconds()
	if err != nil {
		slog.Error("kaggle.Get", "range", rangeHeader, "url", datasetUrl, "took_ms", tookMs, "error", err)
		return nil, err
	}
	if response.StatusCode == http.StatusNotFound {
		slog.Warn("kaggle.Get", "range", rangeHeader, "url", datasetUrl, "took_ms", tookMs, "error", "NotFound")
		return nil, ErrDoesNotExist
	}
	slog.Debug("kaggle.Get", "range", rangeHeader, "url", datasetUrl, "took_ms", tookMs, "error", nil)
	return response.Body, nil
}
