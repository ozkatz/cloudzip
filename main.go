package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	mnt "github.com/ozkatz/cloudzip/pkg/mount"
	"github.com/ozkatz/cloudzip/pkg/mount/nfs"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

const (
	MountServerBindAddress = "127.0.0.1:0"
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
	if !strings.HasSuffix(fstring, "\n") {
		fstring += "\n"
	}
	_, _ = os.Stderr.WriteString(fmt.Sprintf(fstring, args...))
	os.Exit(1)
}

func emitAndDie(fstring string, args ...interface{}) {
	die("ERROR="+fstring, args...)
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
		fmt.Printf("%s\t%-12d\t%-12d\t%s\t%s\n",
			f.Mode, f.CompressedSizeBytes, f.UncompressedSizeBytes, f.Modified.Format(time.RFC822Z), f.FileName)
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

func mountServer(zipFileURI string) {
	ctx := context.Background()
	cacheDir := os.Getenv("CLOUDZIP_CACHE_DIR")
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "lakefs-mount-cache")
	}
	dirExists, err := isDir(cacheDir)
	if err != nil {
		emitAndDie("could not check if cache directory '%s' exists: %v\n", cacheDir, err)
	}
	if !dirExists {
		err := os.MkdirAll(cacheDir, 0755)
		if err != nil {
			emitAndDie("could not create local cache directory: %v\n", err)
		}
	}

	listener, err := net.Listen("tcp", MountServerBindAddress)
	if err != nil {
		die("could not listen on %s: %v\n", MountServerBindAddress, err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	tree, err := mnt.BuildZipTree(ctx, cacheDir, zipFileURI)
	if err != nil {
		emitAndDie("could not create filesystem: %v\n", err)
	}

	done, cancelFn := signal.NotifyContext(ctx, os.Interrupt, syscall.Signal(15)) // SIGTERM
	defer cancelFn()
	go func() {
		err = nfs.Serve(listener, nfs.NewNFSServer(tree, nil))
		if err != nil {
			emitAndDie("could not serve o n %s: %v\n", MountServerBindAddress, err)
		}
	}()
	// we output to stdout to signal back to the caller that this is the selected TCP port to connect to
	fmt.Printf("PORT=%d\n", port)
	<-done.Done()
	_ = listener.Close()
}

func mount(remoteFile, targetDirectory string) {
	uri, err := expandStdin(remoteFile)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not read stdin: %v\n", err))
		os.Exit(1)
	}
	pid, stdout, err := mnt.Daemonize("mount-server", uri)
	if err != nil {
		die("could not spawn NFS server: %v\n", err)
	}
	scanner := bufio.NewScanner(stdout)
	var serverPort int
	for scanner.Scan() {
		received := scanner.Text()
		if strings.HasPrefix(received, "PORT=") {
			received = strings.TrimPrefix(received, "PORT=")
			serverPort, err = strconv.Atoi(received)
			if err != nil {
				die("could not parse port from NFS server: %v\n", err)
			}
			break // we only care about first line
		} else if strings.HasPrefix(received, "ERROR=") {
			errMessage := strings.TrimPrefix(received, "ERROR=")
			die("could not start mount server: %s\n", errMessage)
		}
	}
	if err := scanner.Err(); err != nil {
		die("could not get back port from NFS server: %v", err)
	}
	slog.Info("NFS server started", "pid", pid, "port", serverPort)

	// create directory if it doesn't exist
	dirExists, err := isDir(targetDirectory)
	if err != nil {
		die("could not check if target directory '%s' exists: %v\n", targetDirectory, err)
	}
	if !dirExists {
		err := os.MkdirAll(targetDirectory, 0700)
		if err != nil {
			die("could not create target directory: %v\n", err)
		}
	}

	// now mount it
	if err := mnt.Mount(serverPort, targetDirectory); err != nil {
		die("could not run mount command: %v\n", err)
	}
}

func umount(directory string) {
	err := mnt.Umount(directory)
	if err != nil {
		die("could not unmount directory '%s': %v\n", directory, err)
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
	case "umount", "unmount":
		assertArgCount(args, 3)
		umount(args[2])
	case "mount-server":
		assertArgCount(args, 3)
		mountServer(args[2])
	case "help":
		helpNoFail()
	default:
		help()
	}
}
