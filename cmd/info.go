package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var infoCmd = &cobra.Command{
	Use:     "info",
	Short:   "Display aggregate information about the remote archive (number of files, total size, etc)",
	Example: "cz info s3://example-bucket/path/to/archive.zip",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		remoteFile := args[0]
		var totalCompressed, totalUncompressed, totalFiles uint64
		for _, f := range getCdr(remoteFile) {
			if f.Mode.IsDir() {
				continue
			}
			totalCompressed += f.CompressedSizeBytes
			totalUncompressed += f.UncompressedSizeBytes
			totalFiles += 1
		}
		fmt.Printf("zip file: %s\n", remoteFile)
		fmt.Printf("files: %d\n", totalFiles)
		fmt.Printf("total bytes (compressed): %d\n", totalCompressed)
		fmt.Printf("total bytes (uncompressed): %d\n", totalUncompressed)
		fmt.Printf("total bytes (compressed, human readable): %s\n", byteCountIEC(totalCompressed))
		fmt.Printf("total bytes (uncompressed, human readable): %s\n", byteCountIEC(totalUncompressed))
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)
}
