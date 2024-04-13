package cmd

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/ozkatz/cloudzip/pkg/mount"
	"github.com/ozkatz/cloudzip/pkg/mount/nfs"
	"github.com/spf13/cobra"
	"net"
	"os"
	"os/signal"
	"path/filepath"
)

const (
	cacheDirEnvironmentVariableName = "CLOUDZIP_CACHE_DIR"
)

var mountServerCmd = &cobra.Command{
	Use:    "mount-server",
	Hidden: true,
	Args:   cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		remoteFile := args[0]
		cacheDir, err := cmd.Flags().GetString("cache-dir")
		if err != nil {
			die("could not parse command flags: %v\n", err)
		}
		listenAddr, err := cmd.Flags().GetString("listen")
		if err != nil {
			die("could not parse command flags: %v\n", err)
		}
		// handle cache dir
		if cacheDir == "" {
			cacheDir = os.Getenv(cacheDirEnvironmentVariableName)
		}
		if cacheDir == "" {
			cacheDir = filepath.Join(os.TempDir(), "cz-mount-cache", uuid.Must(uuid.NewV7()).String())
			// auto generated cache dir. Let's try and remove it when done:
			defer func() {
				err := os.RemoveAll(cacheDir)
				if err != nil {
					die("could not clear cache dir at %s: %v\n", cacheDir, err)
				}
			}()
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

		// bind to listen address
		listener, err := net.Listen("tcp4", listenAddr)
		if err != nil {
			die("could not listen on %s: %v\n", listenAddr, err)
		}
		boundAddr := listener.Addr()

		// build index for remote archive
		tree, err := mount.BuildZipTree(ctx, cacheDir, remoteFile, map[string]interface{}{
			"listen_addr": boundAddr,
		})
		if err != nil {
			emitAndDie("could not create filesystem: %v\n", err)
		}

		// setup signal handling
		ctx, cancelFn := signal.NotifyContext(ctx, os.Interrupt) // SIGTERM
		defer cancelFn()

		handler := nfs.NewNFSHandler(tree, nil)
		go func() {
			err = nfs.Serve(ctx, listener, handler)
			if err != nil {
				emitAndDie("could not serve NFS server on listener: %s: %v\n", listenAddr, err)
			}
		}()

		// we output to stdout to signal back to the caller that this is the selected TCP port to connect to
		fmt.Printf("LISTEN=%s\n", boundAddr.String())
		<-ctx.Done()
	},
}

func init() {
	mountServerCmd.Flags().String("cache-dir", "", "directory to cache read files in")
	mountServerCmd.Flags().StringP("listen", "l", MountServerBindAddress, "address to listen on")
	rootCmd.AddCommand(mountServerCmd)
}
