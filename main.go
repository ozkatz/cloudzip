package main

import (
	"context"
	"fmt"
	"github.com/ozkatz/cloudzip/pkg/download"
	"github.com/ozkatz/cloudzip/pkg/zipfile"

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
	downloader := download.NewDynamicDownloader()
	reader := zipfile.NewRemoteZipReader(downloader, uri)
	files, err := reader.ListFiles(context.Background())
	if err != nil {
		os.Exit(1)
	}
	for _, f := range files {
		fmt.Printf("%s\t%-12d\t%s\t%s\n",
			f.Mode(), f.UncompressedSize64, f.Modified.Format(time.RFC822Z), f.Name)
	}
}

func cat(args []string) {
	if len(args) != 2 {
		help()
	}
	uri := args[0]
	filePath := args[1]
	downloader := download.NewDynamicDownloader()
	ctx := context.Background()
	reader := zipfile.NewRemoteZipReader(downloader, uri)
	_, err := reader.CopyFile(ctx, filePath, os.Stdout)
	if err != nil {
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
