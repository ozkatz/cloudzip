package cmd

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ozkatz/cloudzip/pkg/mount"
)

type nfsServerCallbackStatus string

const (
	nfsServerCallbackStatusSuccess nfsServerCallbackStatus = "SUCCESS"
	nfsServerCallbackStatusError   nfsServerCallbackStatus = "ERROR"
)

type nfsServerCallback struct {
	Status  nfsServerCallbackStatus
	Message string
}

func getNFSServerCallback(callbackListener net.Listener) chan nfsServerCallback {
	statusUpdates := make(chan nfsServerCallback)
	go func() {
		conn, err := callbackListener.Accept()
		if err != nil {
			die("could not receive communications from mount server")
		}
		var zeroTime time.Time
		err = conn.SetReadDeadline(zeroTime)
		if err != nil {
			die("could not receive communications from mount server")
		}

		received, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			die("could not get back status from NFS server: %v (received = '%s')", err, received)
		}
		received = strings.TrimSuffix(received, "\n")
		parts := strings.SplitN(received, "=", 2)
		msg := nfsServerCallback{nfsServerCallbackStatus(parts[0]), parts[1]}
		statusUpdates <- msg
		close(statusUpdates)
		_ = conn.Close()
	}()
	return statusUpdates
}

var mountCmd = &cobra.Command{
	Use:     "mount",
	Short:   "Virtually mount the remote archive onto a local directory",
	Example: "cz mount s3://example-bucket/path/to/archive.zip data_dir/",
	Args:    cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		remoteFile := args[0]
		targetDirectory := args[1]
		uri, err := expandStdin(remoteFile)
		if err != nil {
			_, _ = os.Stderr.WriteString(fmt.Sprintf("could not read stdin: %v\n", err))
			os.Exit(1)
		}
		cacheDir, err := cmd.Flags().GetString("cache-dir")
		if err != nil {
			die("could not parse command flags: %v\n", err)
		}
		listenAddr, err := cmd.Flags().GetString("listen")
		if err != nil {
			die("could not parse command flags: %v\n", err)
		}
		noSpawn, err := cmd.Flags().GetBool("no-spawn")
		if err != nil {
			die("could not parse command flags: %v\n", err)
		}

		serverCmd := []string{"mount-server", uri}
		if cacheDir != "" {
			serverCmd = append(serverCmd, "--cache-dir", cacheDir)
		}
		if listenAddr != "" {
			serverCmd = append(serverCmd, "--listen", listenAddr)
		}

		var serverAddr string
		if !noSpawn {
			callbackListener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				die("could not spawn NFS server: %v\n", err)
			}
			callbackAddr := callbackListener.Addr().String()
			serverCmd = append(serverCmd, "--callback-addr", callbackAddr)
			serverStatus := getNFSServerCallback(callbackListener)
			pid, err := mount.Daemonize(serverCmd...)
			if err != nil {
				die("could not spawn NFS server: %v\n", err)
			}
			callback := <-serverStatus
			switch callback.Status {
			case nfsServerCallbackStatusSuccess:
				serverAddr = callback.Message
			case nfsServerCallbackStatusError:
				die("NFS server initialization error:\n%s\n", callback.Message)
			}
			_ = callbackListener.Close()
			slog.Info("NFS server started", "pid", pid, "listen_addr", serverAddr)
		} else {
			serverAddr = listenAddr
		}

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
		if err := mount.Mount(serverAddr, targetDirectory); err != nil {
			die("could not run mount command: %v\n", err)
		}
	},
}

func init() {
	mountCmd.Flags().String("cache-dir", "", "directory to cache read files in")
	mountCmd.Flags().StringP("listen", "l", MountServerBindAddress, "address to listen on")
	mountCmd.Flags().Bool("no-spawn", false, "will not spawn a new server, assume one is already running")
	_ = mountCmd.Flags().MarkHidden("no-spawn")
	rootCmd.AddCommand(mountCmd)
}
