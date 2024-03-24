package main

import (
	"context"
	"fmt"
	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
	"io"

	"log/slog"
	"os"
	"time"
)

var HelpText = `cz - cloud zip
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

`

func ls(args []string) {
	if len(args) != 1 {
		help()
	}
	uri := args[0]
	ctx := context.Background()
	obj := remote.NewRemoteObject(uri, ctx)
	zip, err := zipfile.NewCentralDirectoryParser(obj)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not open zip file: %v\n", err))
		os.Exit(1)
	}
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

func cat(args []string) {
	if len(args) != 2 {
		help()
	}
	uri := args[0]
	filePath := args[1]
	ctx := context.Background()
	obj := remote.NewRemoteObject(uri, ctx)
	zip, err := zipfile.NewCentralDirectoryParser(obj)
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not open zip file: %v\n", err))
		os.Exit(1)
	}
	reader, err := zip.Read(filePath)
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

func help() {
	_, _ = os.Stderr.WriteString(HelpText)
	os.Exit(1)
}

func main() {
	args := os.Args
	if len(args) < 2 {
		help()
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelError,
	})))
	if os.Getenv("CLOUDZIP_LOGGING") == "DEBUG" {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelDebug,
		})))
	}
	sub := args[1]
	switch sub {
	case "ls":
		ls(args[2:])
	case "cat":
		cat(args[2:])
	case "help":
		help()
	default:
		help()
	}
}
