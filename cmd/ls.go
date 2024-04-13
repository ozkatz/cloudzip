package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"time"
)

var lsCmd = &cobra.Command{
	Use:     "ls",
	Short:   "List the files that exist in the remote zip archive",
	Example: "ls s3://example-bucket/path/to/archive.zip",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		remoteFile := args[0]
		for _, f := range getCdr(remoteFile) {
			fmt.Printf("%s\t%-12d\t%-12d\t%s\t%s\n",
				f.Mode, f.CompressedSizeBytes, f.UncompressedSizeBytes, f.Modified.Format(time.RFC822Z), f.FileName)
		}
	},
}

func init() {
	rootCmd.AddCommand(lsCmd)
}
