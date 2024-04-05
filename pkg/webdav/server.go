package webdav

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	dav "golang.org/x/net/webdav"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	ErrAlreadyMounted = errors.New("already mounted")
)

type LoggingResponseWriter struct {
	n          int
	writer     http.ResponseWriter
	statusCode int
}

func (l *LoggingResponseWriter) Header() http.Header {
	return l.writer.Header()
}

func (l *LoggingResponseWriter) Write(bytes []byte) (int, error) {
	n, err := l.writer.Write(bytes)
	l.n += n
	return n, err
}

func (l *LoggingResponseWriter) WriteHeader(statusCode int) {
	l.writer.WriteHeader(statusCode)
	l.statusCode = statusCode
}

type LoggingMiddleware struct {
	next http.Handler
}

func (l *LoggingMiddleware) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	before := time.Now()
	loggingWriter := &LoggingResponseWriter{writer: writer}
	l.next.ServeHTTP(loggingWriter, request)
	took := time.Since(before)
	slog.DebugContext(request.Context(), "HTTP request done",
		"request_method", request.Method,
		"request_path", request.URL.Path,
		"response_status_code", loggingWriter.statusCode,
		"response_bytes", loggingWriter.n,
		"response_took_seconds", took.Seconds())
}

func NewLoggingMiddleware(next http.Handler) http.Handler {
	return &LoggingMiddleware{next: next}
}

type MountHTTPError struct {
	Error string `json:"error"`
}

type MountInfo struct {
	Remote    string `json:"remote"`
	LocalPath string `json:"local_path"`
	Id        string `json:"id"`
}

type MountRegistry struct {
	mounts map[string]*MountInfo
	lock   sync.RWMutex
}

func (r *MountRegistry) ListMounts() ([]*MountInfo, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	mounts := make([]*MountInfo, len(r.mounts))
	i := 0
	for _, v := range r.mounts {
		mounts[i] = v
		i++
	}
	sort.Slice(mounts, func(i, j int) bool {
		return mounts[i].LocalPath < mounts[j].LocalPath
	})
	return mounts, nil
}

func (r *MountRegistry) Unmount(localPath string) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	var found string
	for id, mount := range r.mounts {
		if mount.LocalPath == localPath {
			found = id
		}
	}
	if found != "" {
		delete(r.mounts, found)
	}
	return nil
}

func (r *MountRegistry) Mount(mountInfo *MountInfo) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, mount := range r.mounts {
		if path.Clean(mount.LocalPath) == path.Clean(mountInfo.LocalPath) {
			return ErrAlreadyMounted
		}
	}
	r.mounts[mountInfo.Id] = mountInfo
	return nil
}

func (r *MountRegistry) GetID(id string) (info *MountInfo, found bool) {
	r.lock.Lock()
	defer r.lock.Unlock()
	mount, ok := r.mounts[id]
	return mount, ok
}

func writeError(writer http.ResponseWriter, statusCode int, err error) {
	_ = writeJSON(writer, statusCode, MountHTTPError{Error: err.Error()})
}
func writeJSON(writer http.ResponseWriter, statusCode int, data any) error {
	serialized, _ := json.Marshal(data)
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_, err := writer.Write(serialized)
	return err
}

func readJSON(request *http.Request, target any) error {
	return json.NewDecoder(request.Body).Decode(target)
}

func NewServer(addr, cacheDirectory string) (*http.Server, error) {
	registry := &MountRegistry{
		mounts: make(map[string]*MountInfo),
		lock:   sync.RWMutex{},
	}
	router := chi.NewRouter()
	router.Use(middleware.Recoverer)

	webServer := &http.Server{
		Addr: addr,
	}

	// setup WebDAV Handler (read-only)
	readOnlyFS := &ZipROFilesystem{
		registry:      registry,
		fileCache:     newFileCache(cacheDirectory),
		metadataCache: newMetadataCache(),
	}

	router.Delete("/mounts", func(writer http.ResponseWriter, request *http.Request) {
		// delete mount
		localPath := request.URL.Query().Get("path")
		_ = registry.Unmount(localPath)
		writer.WriteHeader(http.StatusOK)
	})

	router.Post("/mounts", func(writer http.ResponseWriter, request *http.Request) {
		// create new mount
		mount := &MountInfo{}
		err := readJSON(request, mount)
		if err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}

		err = registry.Mount(mount)
		if errors.Is(err, ErrAlreadyMounted) {
			writeError(writer, http.StatusConflict, err)
		} else if err != nil {
			writeError(writer, http.StatusInternalServerError, err)
			return
		}
		_ = writeJSON(writer, http.StatusCreated, mount)
	})

	router.Get("/mounts", func(writer http.ResponseWriter, request *http.Request) {
		// list mounts
		mounts, err := registry.ListMounts()
		if err != nil {
			writeError(writer, http.StatusInternalServerError, err)
			return
		}
		_ = writeJSON(writer, http.StatusOK, mounts)
	})

	router.Post("/terminate", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		go func() {
			_ = webServer.Shutdown(context.TODO())
		}()
	})

	// webdav goes here
	const webdavPrefix = "/wd/cloudzip"
	webDavHandler := &dav.Handler{
		Prefix:     webdavPrefix,
		FileSystem: readOnlyFS,
		LockSystem: dav.NewMemLS(),
	}
	webServer.Handler = NewLoggingMiddleware(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, webdavPrefix) {
			webDavHandler.ServeHTTP(writer, request)
			return
		}

		// otherwise, serve API
		router.ServeHTTP(writer, request)

	}))

	return webServer, nil
}

func IsServerRunning(addr string) (bool, error) {
	ln, err := net.Listen("tcp4", addr)
	defer func() {
		if ln != nil {
			_ = ln.Close()
		}
	}()
	if err != nil && isErrorAddressAlreadyInUse(err) {
		return true, nil
	}
	return false, err
}

// taken from: https://stackoverflow.com/a/65865898
func isErrorAddressAlreadyInUse(err error) bool {
	var eOsSyscall *os.SyscallError
	if !errors.As(err, &eOsSyscall) {
		return false
	}
	var errErrno syscall.Errno // doesn't need a "*" (ptr) because it's already a ptr (uintptr)
	if !errors.As(eOsSyscall, &errErrno) {
		return false
	}
	if errErrno == syscall.EADDRINUSE {
		return true
	}
	const WSAEADDRINUSE = 10048
	if runtime.GOOS == "windows" && errErrno == WSAEADDRINUSE {
		return true
	}
	return false
}
