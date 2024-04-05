package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/webdav"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
	"io"
	"log/slog"
	"path/filepath"
	"strings"

	"os"
	"time"
)

const (
	MountServerBindAddress = "127.0.0.1:6363"
	HelpText               = `cz - cloud zip
Efficiently list and read from remote zip files (without downloading the entire file)

cz ls:
Usage: cz ls <remote zip file URI>

List the files that exist in the remote zip archive.

Example:
	
	cz ls s3://example-bucket/path/to/object.zip


cz cat:
Usage: cz cat <remote zip file URI> <file path>

Write the contents of the file in the remote zip archive to stdout

Example:

	cz cat s3://example-bucket/path/to/object.zip images/file.png


cz mount:
Usage: cz mount <remote zip file URI> <local directory path>

Virtually mount the zip file onto a local directory

Example:

	cz mount s3://example-bucket/path/to/object.zip /my_zip


cz umount:
Usage: cz mount <local directory path>

Unmounts a currently mounted zip file at the given directory

Example:

	cz umount /my_zip

`
)

func expandStdin(arg string) (string, error) {
	if arg != "-" {
		return arg, nil
	}
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", err
	}
	expanded := string(data)
	return strings.Trim(expanded, "\n \t"), nil
}

func die(fstring string, args ...interface{}) {
	_, _ = os.Stderr.WriteString(fmt.Sprintf(fstring, args...))
	os.Exit(1)
}

func helpNoFail() {
	_, _ = os.Stderr.WriteString(HelpText)
}

func help() {
	helpNoFail()
	os.Exit(1)
}

func assertArgCount(args []string, required int) {
	if len(args) != required {
		help()
	}
}

func setupLogging() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelError,
	})))
	if os.Getenv("CLOUDZIP_LOGGING") == "DEBUG" {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelDebug,
		})))
	}
}

func isDir(path string) (bool, error) {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return stat.IsDir(), nil
}

type adapter struct {
	f   remote.Fetcher
	ctx context.Context
}

func (a *adapter) Fetch(start, end *int64) (io.Reader, error) {
	return a.f.Fetch(a.ctx, start, end)
}

func ls(remoteFile string) {
	zipfilePath, err := expandStdin(remoteFile)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not read stdin: %v\n", err))
		os.Exit(1)
	}
	ctx := context.Background()
	obj, err := remote.Object(zipfilePath)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not open remote zip file: %v\n", err))
		os.Exit(1)
	}
	zip := zipfile.NewCentralDirectoryParser(&adapter{
		f:   obj,
		ctx: ctx,
	})

	files, err := zip.GetCentralDirectory()
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not read zip file contents: %v\n", err))
		os.Exit(1)
	}
	for _, f := range files {
		fmt.Printf("%s\t%-12d\t%s\t%s\n",
			f.Mode, f.UncompressedSizeBytes, f.Modified.Format(time.RFC822Z), f.FileName)
	}
}

func cat(remoteFile, path string) {
	uri, err := expandStdin(remoteFile)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not read stdin: %v\n", err))
		os.Exit(1)
	}
	ctx := context.Background()
	obj, err := remote.Object(uri)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not open zip file: %v\n", err))
		os.Exit(1)
	}
	zip := zipfile.NewCentralDirectoryParser(&adapter{
		f:   obj,
		ctx: ctx,
	})
	reader, err := zip.Read(path)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not open zip file stream: %v\n", err))
		os.Exit(1)
	}
	_, err = io.Copy(os.Stdout, reader)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not download file: %v\n", err))
		os.Exit(1)
	}
}

func isMountServerAlive() bool {
	isRunning, err := webdav.IsServerRunning(MountServerBindAddress)
	if err != nil {
		die("could not check if mount server is running: %v\n", err)
	}
	return isRunning
}

func ensureMountServerRunning() {
	if !isMountServerAlive() {
		// no server, let's spawn one!
		pid, err := webdav.Daemonize("mount-server")
		if err != nil {
			die("could not daemonize mount server: %v\n", err)
		}
		fmt.Printf("started mount server with pid %d\n", pid)
		// wait for it to be up
		attempts := 5
		up := false
		for i := 0; i < attempts; i++ {
			if isMountServerAlive() {
				up = true
				break
			}
			time.Sleep(time.Second)
		}
		if !up {
			die("could not spin up local mount server")
		}
	}
}

func mountServer() {
	cleanUpOnExit := false
	cacheDir := os.Getenv("CLOUDZIP_CACHE_DIR")
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "lakefs-mount-cache")
		cleanUpOnExit = true // ephemeral directory
	}
	dirExists, err := isDir(cacheDir)
	if err != nil {
		die("could not check if cache directory '%s' exists: %v\n", cacheDir, err)
	}
	if !dirExists {
		err := os.MkdirAll(cacheDir, 0755)
		if err != nil {
			die("could not create local cache directory: %v\n", err)
		}
	}
	if err := webdav.RunServer(MountServerBindAddress, cacheDir, cleanUpOnExit); err != nil {
		die("could not start mount server on address '%s': %v\n", MountServerBindAddress, err)
	}
}

func mount(zipfile, directory string) {
	ensureMountServerRunning()

	// make an absolute path
	absolutePath, err := filepath.Abs(directory)
	if err != nil {
		die("could not find path :%v\n", err)
	}

	// now that we have a mount server, let's mount it!
	restClient := webdav.NewMountServerRestClient(MountServerBindAddress)

	// check if directory exists, if not - create it
	dirExists, err := isDir(absolutePath)
	if err != nil {
		die("could not check if local directory exists: %v\n", err)
	}
	// ensure dir
	if !dirExists {
		err := os.MkdirAll(absolutePath, 0755)
		if err != nil {
			die("could not create local directory: %v\n", err)
		}
	}
	mountId := uuid.New().String()
	err = restClient.RegisterMount(mountId, zipfile, absolutePath)
	if err != nil {
		die("could not track mount state: %v\n", err)
	}

	err = webdav.Mount(restClient.GetWebdavURL(mountId), absolutePath)
	if err != nil {
		die("could not mount zip file: %v\n", err)
	}
}

func umount(directory string) {
	absolutePath, err := filepath.Abs(directory)
	if err != nil {
		die("could not find path :%v\n", err)
	}
	if !isMountServerAlive() {
		return
	}
	if err := webdav.Umount(absolutePath); err != nil {
		die("could not unmount path: %v\n", err)
	}

	restClient := webdav.NewMountServerRestClient(MountServerBindAddress)
	if err := restClient.Unmount(absolutePath); err != nil {
		die("could not unmount path: %v\n", err)
	}

	currentMounts, err := restClient.GetMounts()
	if err != nil {
		die("could not get current mounts: %v\n", err)
	}
	if len(currentMounts) == 0 {
		// no mounts left, stop server
		if err := restClient.TerminateServer(); err != nil {
			die("error terminating mount server: %v\n", err)
		}
	}
}

func main() {
	args := os.Args
	if len(args) < 2 {
		help()
	}
	setupLogging()
	// run:
	sub := args[1]
	switch sub {
	case "ls":
		assertArgCount(args, 3)
		ls(args[2])
	case "cat":
		assertArgCount(args, 4)
		cat(args[2], args[3])
	case "mount":
		assertArgCount(args, 4)
		mount(args[2], args[3])
	case "umount":
		assertArgCount(args, 3)
		umount(args[2])
	case "mount-server":
		mountServer()
	case "help":
		helpNoFail()
	default:
		help()
	}
}
