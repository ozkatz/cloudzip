package webdav

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

// fork crete a new process
func fork(args []string) (int, error) {
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = os.Environ()
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.ExtraFiles = nil
	//cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	// release
	if err := cmd.Process.Release(); err != nil {
		return pid, err
	}
	return pid, nil
}

func Daemonize(cmd ...string) (int, error) {
	return fork(cmd)
}

func RunServer(addr, cacheDir string, cleanUpOnExit bool) error {
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	defer func() {
		if cleanUpOnExit {
			_ = cleanUp(cacheDir)
		}
	}()

	httpServer, err := NewServer(addr, cacheDir)
	if err != nil {
		return err
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				panic(err)
			}
		}
		stop()
	}()

	<-signalCtx.Done()

	slog.Info("shutting down HTTP Server")
	// got SIGINT / SIGTERM
	err = httpServer.Shutdown(context.TODO())

	if err != nil {
		slog.Warn("HTTP server shutdown", "error", err)
		return err
	}
	slog.Info("HTTP server shutdown")
	return nil
}

func cleanUp(cacheDir string) error {
	if cacheDir == "" {
		return nil
	}
	return os.RemoveAll(cacheDir)
}
