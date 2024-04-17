package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const (
	CloudZipVersion        = "0.0.1dev"
	MountServerBindAddress = "127.0.0.1:0"
)

var rootCmd = &cobra.Command{
	Use:               "cz",
	Short:             "Efficiently interact with remote zip files (without downloading/extracting the entire file)",
	CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		setupLogging()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		_, err = fmt.Fprintln(os.Stderr, err)
		if err != nil {
			return
		}
		os.Exit(1)
	}
}
