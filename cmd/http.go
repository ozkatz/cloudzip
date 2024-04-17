package cmd

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
	"github.com/spf13/cobra"
)

var httpCmd = &cobra.Command{
	Use:     "http",
	Short:   "Run HTTP proxy server mode",
	Example: "http s3://example-bucket/path",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		remotePath := strings.TrimSuffix(args[0], "/")
		bindAddress, err := cmd.Flags().GetString("listen")
		if err != nil {
			die("Could not parse command flag listen: %v\n", err)
		}

		http.HandleFunc("/",
			func(w http.ResponseWriter, r *http.Request) {
				internalPath := r.URL.Query().Get("filename")
				slog.Debug("HTTP Handler", "objectPath", r.URL.Path, "internalPath", internalPath)
				obj, err := remote.Object(remotePath + r.URL.Path)
				if err != nil {
					slog.Warn("could not open zip file", "error", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				zip := zipfile.NewCentralDirectoryParser(zipfile.NewStorageAdapter(r.Context(), obj))
				reader, err := zip.Read(internalPath)
				if errors.Is(err, remote.ErrDoesNotExist) || errors.Is(err, zipfile.ErrFileNotFound) {
					w.WriteHeader(http.StatusNotFound)
					return
				} else if err != nil {
					w.WriteHeader(http.StatusBadGateway)
					slog.Warn("Error reading zip file from upstream", "error", err)
					return
				}
				io.Copy(w, reader)
			})

		listener, err := net.Listen("tcp", bindAddress)
		if err != nil {
			die("Failed to bind port: %v\n", err)
		}
		fmt.Printf("HTTP server listening on %s\n", listener.Addr().String())
		err = http.Serve(listener, nil)
		if err != nil {
			slog.Error("Error running HTTP server", "error", err)
		}
	},
}

func init() {
	httpCmd.Flags().StringP("listen", "l", "127.0.0.1:0", "address to listen on")
	rootCmd.AddCommand(httpCmd)
}
