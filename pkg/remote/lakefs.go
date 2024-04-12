package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mitchellh/go-homedir"
	"gopkg.in/yaml.v3"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	lakeFSDefaultConfigLocation = "~/.lakectl.yaml"
	lakeFSConfigEnvVar          = "LAKECTL_CONFIG"
	lakeFSApiPrefix             = "/api/v1"
	lakeFSEnvAccessKeyId        = "LAKECTL_ACCESS_KEY_ID"
	lakeFSEnvSecretAccessKey    = "LAKECTL_SECRET_ACCESS_KEY"
	lakeFSEnvEndpointUrl        = "LAKECTL_ENDPOINT_URL"
)

var (
	ErrLakeFSError = errors.New("lakeFS API Error")
)

type lakeFSConfig struct {
	Credentials struct {
		AccessKeyId     string `yaml:"access_key_id"`
		SecretAccessKey string `yaml:"secret_access_key"`
	} `yaml:"credentials"`

	Server struct {
		EndpointURL string `yaml:"endpoint_url"`
	} `yaml:"server"`
}

type lakeFSInstallationConfig struct {
	StorageConfig struct {
		PreSignSupport bool `json:"pre_sign_support"`
	} `json:"storage_config"`
}

type lakeFSObjectStats struct {
	PhysicalAddress       string `json:"physical_address"`
	PhysicalAddressExpiry *int64 `json:"physical_address_expiry,omitempty"`
}

func loadLakefsConfigFromEnv() (*lakeFSConfig, error) {
	accessKeyId := os.Getenv(lakeFSEnvAccessKeyId)
	secretAccessKey := os.Getenv(lakeFSEnvSecretAccessKey)
	endpointUrl := os.Getenv(lakeFSEnvEndpointUrl)
	if accessKeyId != "" && secretAccessKey != "" && endpointUrl != "" {
		return &lakeFSConfig{
			Credentials: struct {
				AccessKeyId     string `yaml:"access_key_id"`
				SecretAccessKey string `yaml:"secret_access_key"`
			}{
				AccessKeyId:     accessKeyId,
				SecretAccessKey: secretAccessKey,
			},
			Server: struct {
				EndpointURL string `yaml:"endpoint_url"`
			}{
				EndpointURL: endpointUrl,
			},
		}, nil
	}
	return nil, fmt.Errorf("%w: no configuration found", ErrLakeFSError)
}

func loadLakefsConfig() (*lakeFSConfig, error) {
	configLocation := lakeFSDefaultConfigLocation
	configLocationFromEnv := os.Getenv(lakeFSConfigEnvVar)
	if configLocationFromEnv != "" {
		configLocation = configLocationFromEnv
	}
	// attempt loading
	configPath, err := homedir.Expand(configLocation)
	if err != nil {
		return nil, err
	}
	cfg := &lakeFSConfig{}
	data, err := os.ReadFile(configPath)
	if os.IsNotExist(err) {
		return loadLakefsConfigFromEnv()
	} else if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return nil, err
	}
	// handle suffix for endpoint
	cfg.Server.EndpointURL = strings.TrimSuffix(cfg.Server.EndpointURL, "/")
	if !strings.HasSuffix(cfg.Server.EndpointURL, lakeFSApiPrefix) {
		cfg.Server.EndpointURL += lakeFSApiPrefix
	}
	return cfg, nil
}

type LakeFSFetcher struct {
	uri              string
	preSignSupported bool

	// for refreshing pre-signed url
	cachedUrl string
	expires   time.Time
	l         *sync.Mutex
}

func NewLakeFSFetcher(uri string) (*LakeFSFetcher, error) {
	preSignSupported, err := canLakeFSPreSign()
	if err != nil {
		return nil, err
	}
	return &LakeFSFetcher{
		uri:              uri,
		preSignSupported: preSignSupported,
		l:                &sync.Mutex{},
	}, nil
}

func canLakeFSPreSign() (bool, error) {
	cfg, err := loadLakefsConfig()
	if err != nil {
		return false, err
	}

	// let's call config first
	auth := fmt.Sprintf("Basic %s", basicAuth(cfg.Credentials.AccessKeyId, cfg.Credentials.SecretAccessKey))
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/config", cfg.Server.EndpointURL), nil)
	if err != nil {
		return false, err
	}
	req.Header.Add("Authorization", auth)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("%w: got HTTP %d getting server config",
			ErrLakeFSError, response.StatusCode)
	}
	installationConfig := &lakeFSInstallationConfig{}
	err = json.NewDecoder(response.Body).Decode(installationConfig)
	if err != nil {
		return false, err
	}

	return installationConfig.StorageConfig.PreSignSupport, nil
}

type lakeFSUri struct {
	repo   string
	ref    string
	object string
}

func parseLakeFSUri(uri string) (*lakeFSUri, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	repo := parsed.Host
	pth := path.Clean(parsed.Path)
	pth = strings.TrimPrefix(pth, "/")
	pathParts := strings.SplitN(pth, "/", 2)
	if len(pathParts) != 2 || pathParts[0] == "" || pathParts[1] == "" {
		return nil, ErrInvalidURI
	}
	ref := pathParts[0]
	object := pathParts[1]
	return &lakeFSUri{
		repo:   repo,
		ref:    ref,
		object: object,
	}, nil
}

func getLakeFSUrl(cfg *lakeFSConfig, uri string) (string, time.Time, error) {
	addr, err := parseLakeFSUri(uri)
	if err != nil {
		return "", time.Time{}, err
	}
	// now stat object
	auth := fmt.Sprintf("Basic %s", basicAuth(cfg.Credentials.AccessKeyId, cfg.Credentials.SecretAccessKey))
	statUrl := fmt.Sprintf("%s/repositories/%s/refs/%s/objects/stat", cfg.Server.EndpointURL, addr.repo, addr.ref)
	req, err := http.NewRequest(http.MethodGet, statUrl, nil)
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Add("Authorization", auth)
	q := req.URL.Query()
	q.Add("path", addr.object)
	q.Add("presign", "true")
	req.URL.RawQuery = q.Encode()

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	if response.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("%w: got HTTP %d getting object URL",
			ErrLakeFSError, response.StatusCode)
	}
	stat := &lakeFSObjectStats{}
	if err = json.NewDecoder(response.Body).Decode(stat); err != nil {
		return "", time.Time{}, err
	}

	// check if pre-signed
	expiresTimestamp := intVal(stat.PhysicalAddressExpiry)
	if expiresTimestamp == 0 {
		return "", time.Time{}, fmt.Errorf("%w: could not get pre-signed URL", ErrLakeFSError)

	}
	return stat.PhysicalAddress, time.Unix(expiresTimestamp, 0).UTC(), nil
}

func intVal(i *int64) (x int64) {
	if i == nil {
		return x
	}
	return *i
}

func (f *LakeFSFetcher) getURL(cfg *lakeFSConfig) (string, error) {
	f.l.Lock()
	defer f.l.Unlock()
	// return a pre-signed url, if we have a fresh one cached
	if f.cachedUrl != "" {
		// there's something in cache! is it still good?
		if f.expires.IsZero() || f.expires.After(time.Now()) {
			return f.cachedUrl, nil
		}
	}

	// do the work to get one
	zipUrl, expires, err := getLakeFSUrl(cfg, f.uri)
	if err != nil {
		return "", err
	}

	f.cachedUrl = zipUrl
	f.expires = expires
	return f.cachedUrl, nil
}

func (f *LakeFSFetcher) directFetch(ctx context.Context, cfg *lakeFSConfig, startOffset *int64, endOffset *int64) (io.ReadCloser, error) {
	addr, err := parseLakeFSUri(f.uri)
	if err != nil {
		return nil, err
	}
	auth := fmt.Sprintf("Basic %s", basicAuth(cfg.Credentials.AccessKeyId, cfg.Credentials.SecretAccessKey))
	objectUrl := fmt.Sprintf("%s/repositories/%s/refs/%s/objects",
		cfg.Server.EndpointURL, addr.repo, addr.ref)
	req, err := http.NewRequest(http.MethodGet, objectUrl, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	req.Header.Add("Authorization", auth)
	q := req.URL.Query()
	q.Add("path", addr.object)
	q.Add("presign", "false")
	req.URL.RawQuery = q.Encode()
	return f.rangeRequest(req, startOffset, endOffset)
}

func (f *LakeFSFetcher) rangeRequest(req *http.Request, startOffset *int64, endOffset *int64) (io.ReadCloser, error) {
	rangeHeader := buildRange(startOffset, endOffset)
	if rangeHeader != nil {
		req.Header.Set("Range", *rangeHeader)
	}
	start := time.Now()
	response, err := http.DefaultClient.Do(req)
	tookMs := time.Since(start).Milliseconds()
	if err != nil {
		slog.Error("lakefs.Get", "range", rangeHeader, "url", f.uri, "took_ms", tookMs, "error", err)
		return nil, err
	}
	if response.StatusCode == http.StatusNotFound {
		slog.Warn("lakefs.Get", "range", rangeHeader, "url", f.uri, "took_ms", tookMs, "error", "NotFound")
		return nil, ErrDoesNotExist
	}
	slog.Debug("lakefs.Get", "range", rangeHeader, "url", f.uri, "took_ms", tookMs, "error", nil)
	return response.Body, nil
}

func (f *LakeFSFetcher) Fetch(ctx context.Context, startOffset *int64, endOffset *int64) (io.ReadCloser, error) {
	cfg, err := loadLakefsConfig()
	if err != nil {
		return nil, err
	}
	if !f.preSignSupported {
		return f.directFetch(ctx, cfg, startOffset, endOffset)
	}
	zipUrl, err := f.getURL(cfg)
	if err != nil {
		return nil, err
	}
	// make the actual request
	req, err := http.NewRequest(http.MethodGet, zipUrl, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	return f.rangeRequest(req, startOffset, endOffset)

}
