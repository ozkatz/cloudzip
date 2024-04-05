package webdav

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/google/uuid"
)

var (
	ErrMountServerError = errors.New("mount server error")
)

type MountServerClient interface {
	RegisterMount(mountId, remote, localPath string) error
	GetMounts() ([]MountInfo, error)
	Unmount(localPath string) error
	GetWebdavURL(remote string) string
}

var _ MountServerClient = &MountServerRestClient{}

func NewMountServerRestClient(addr string) *MountServerRestClient {
	return &MountServerRestClient{addr: addr}
}

type MountServerRestClient struct {
	addr string
}

func (c *MountServerRestClient) buildURL() *url.URL {
	return &url.URL{
		Scheme: "http",
		Host:   c.addr,
	}
}

func (c *MountServerRestClient) TerminateServer() error {
	serverUrl := c.buildURL()
	serverUrl.Path = "/terminate"
	response, err := http.Post(serverUrl.String(), "", nil)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: HTTP Error %d", ErrMountServerError, response.StatusCode)
	}
	return nil
}

func (c *MountServerRestClient) RegisterMount(mountId, remote, localPath string) error {
	serverUrl := c.buildURL()
	serverUrl.Path = "/mounts"
	data, err := json.Marshal(&MountInfo{
		Remote:    remote,
		Id:        mountId,
		LocalPath: localPath,
	})
	if err != nil {
		return err
	}
	response, err := http.Post(serverUrl.String(), "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("%w: HTTP Error %d", ErrMountServerError, response.StatusCode)
	}
	return nil
}

func (c *MountServerRestClient) GetMounts() ([]MountInfo, error) {
	serverUrl := c.buildURL()
	serverUrl.Path = "/mounts"
	response, err := http.Get(serverUrl.String())
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: HTTP Error %d", ErrMountServerError, response.StatusCode)
	}
	mounts := make([]MountInfo, 0)
	err = json.NewDecoder(response.Body).Decode(&mounts)
	return mounts, err
}

func (c *MountServerRestClient) Unmount(localPath string) error {
	serverUrl := c.buildURL()
	q := serverUrl.Query()
	q.Set("path", localPath)
	serverUrl.Path = "/mounts"
	serverUrl.RawQuery = q.Encode()
	req, err := http.NewRequest(http.MethodDelete, serverUrl.String(), nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: HTTP Error %d", ErrMountServerError, response.StatusCode)
	}
	return nil
}

func (c *MountServerRestClient) GetWebdavURL(remote string) string {
	nonce := uuid.New().String()
	return fmt.Sprintf("http://%s/wd/cloudzip/%s/%s/", c.addr, nonce, remote)
}
