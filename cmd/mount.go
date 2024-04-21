package cmd

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ozkatz/cloudzip/pkg/mount"
)

type mountServerStatus string

const (
	mountServerCallbackTimeout = 30 * time.Second
)

const (
	mountServerStatusSuccess mountServerStatus = "SUCCESS"
	mountServerStatusError   mountServerStatus = "ERROR"
)

type mountServerCallback struct {
	Status  mountServerStatus
	Message string
}

func getMountServerCallback(callbackListener net.Listener) chan mountServerCallback {
	statusUpdates := make(chan mountServerCallback)
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
			die("could not get back status from mount server: %v (received = '%s')", err, received)
		}
		received = strings.TrimSuffix(received, "\n")
		parts := strings.SplitN(received, "=", 2)
		msg := mountServerCallback{mountServerStatus(parts[0]), parts[1]}
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
		logFile, err := cmd.Flags().GetString("log")
		if err != nil {
			die("could not parse command flags: %v\n", err)
		}
		protocol, err := cmd.Flags().GetString("protocol")
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
				die("could not spawn mount server: %v\n", err)
			}
			callbackAddr := callbackListener.Addr().String()
			serverCmd = append(serverCmd, "--callback-addr", callbackAddr)
			if logFile != "" {
				serverCmd = append(serverCmd, "--log", logFile)
			}
			switch protocol {
			case "nfs", "webdav":
				serverCmd = append(serverCmd, "--protocol", protocol)
			default:
				die("unsupported protocol: '%s', select 'nfs' or 'webdav'", protocol)
			}
			serverStatus := getMountServerCallback(callbackListener)
			pid, err := mount.Daemonize(serverCmd...)
			if err != nil {
				die("could not spawn mount server: %v\n", err)
			}

			var callback mountServerCallback
			select {
			case callback = <-serverStatus:
				break
			case <-time.After(mountServerCallbackTimeout):
				// non-responsive: attempt to kill & die
				proc, err := os.FindProcess(pid)
				if err == nil {
					_ = proc.Kill()
				}
				die("timeout waiting for mount server\n")
			}

			switch callback.Status {
			case mountServerStatusSuccess:
				serverAddr = callback.Message
			case mountServerStatusError:
				die("mount server initialization error:\n%s\n", callback.Message)
			}
			_ = callbackListener.Close()
			slog.Info("mount server started", "pid", pid, "listen_addr", serverAddr, "protocol", protocol)
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
		switch protocol {
		case "nfs":
			if err := mount.NFSMount(serverAddr, targetDirectory); err != nil {
				die("could not run mount command: %v\n", err)
			}
		case "webdav":
			if err := mount.WebDavMount(serverAddr, targetDirectory); err != nil {
				die("could not run mount command: %v\n", err)
			}
		default:
			die("unsupported protocol: '%s', select 'nfs' or 'webdav'", protocol)
		}
	},
}

func init() {
	var defaultProtocol = "nfs"
	if runtime.GOOS == "windows" {
		defaultProtocol = "webdav"
	}
	mountCmd.Flags().String("cache-dir", "", "directory to cache read files in")
	mountCmd.Flags().StringP("listen", "l", MountServerBindAddress, "address to listen on")
	mountCmd.Flags().String("log", "", "log file for the server to write to")
	mountCmd.Flags().Bool("no-spawn", false, "will not spawn a new server, assume one is already running")
	mountCmd.Flags().String("protocol", defaultProtocol, "protocol to use (nfs | webdav)")
	_ = mountCmd.Flags().MarkHidden("no-spawn")
	rootCmd.AddCommand(mountCmd)
}
