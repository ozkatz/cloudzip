package cmd

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
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

func getCdr(remoteFile string) []*zipfile.CDR {
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
	zip := zipfile.NewCentralDirectoryParser(zipfile.NewStorageAdapter(ctx, f))
		f:   obj,
		ctx: ctx,
	})

	files, err := zip.GetCentralDirectory()
	if err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("could not read zip file contents: %v\n", err))
		os.Exit(1)
	}
	return files
}

func byteCountIEC(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
