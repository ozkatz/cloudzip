package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/ozkatz/cloudzip/pkg/remote"
	"github.com/ozkatz/cloudzip/pkg/zipfile"
)

var catCmd = &cobra.Command{
	Use:     "cat",
	Short:   "Extract a specific file from the remote archive to stdout",
	Example: "cz cat s3://example-bucket/path/to/archive.zip images/file.png > image.png",
	Args:    cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		remoteFile := args[0]
		internalPath := args[1]
		uri, err := expandStdin(remoteFile)
		if err != nil {
			_, _ = os.Stderr.WriteString(fmt.Sprintf("could not read stdin: %v\n", err))
			os.Exit(1)
		}
		ctx := cmd.Context()
		obj, err := remote.Object(uri)
		if err != nil {
			_, _ = os.Stderr.WriteString(fmt.Sprintf("could not open zip file: %v\n", err))
			os.Exit(1)
		}
		zip := zipfile.NewCentralDirectoryParser(&adapter{
			f:   obj,
			ctx: ctx,
		})
		reader, err := zip.Read(internalPath)
		if err != nil {
			_, _ = os.Stderr.WriteString(fmt.Sprintf("could not open zip file stream: %v\n", err))
			os.Exit(1)
		}
		_, err = io.Copy(os.Stdout, reader)
		if err != nil {
			_, _ = os.Stderr.WriteString(fmt.Sprintf("could not download file: %v\n", err))
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(catCmd)
}
