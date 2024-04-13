package cmd

import (
	"bufio"
	"fmt"
	mnt "github.com/ozkatz/cloudzip/pkg/mount"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

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
			pid, stdout, err := mnt.Daemonize(serverCmd...)
			if err != nil {
				die("could not spawn NFS server: %v\n", err)
			}
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				received := scanner.Text()
				if strings.HasPrefix(received, "LISTEN=") {
					received = strings.TrimPrefix(received, "LISTEN=")
					serverAddr = strings.Trim(received, "\r\n")
					break // we only care about first line
				} else if strings.HasPrefix(received, "ERROR=") {
					errMessage := strings.TrimPrefix(received, "ERROR=")
					die("could not start mount server: %s\n", errMessage)
				}
			}
			if err := scanner.Err(); err != nil {
				die("could not get back port from NFS server: %v", err)
			}
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
		if err := mnt.Mount(serverAddr, targetDirectory); err != nil {
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
