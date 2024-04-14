package cmd

import (
	"github.com/spf13/cobra"

	"github.com/ozkatz/cloudzip/pkg/mount"
)

var umountCmd = &cobra.Command{
	Use:     "umount",
	Short:   "Unmounts a currently mounted remote archive from the given directory",
	Example: "cz umount data_dir/",
	Aliases: []string{"unmount"},
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		directory := args[0]
		err := mount.Umount(directory)
		if err != nil {
			die("could not unmount directory '%s': %v\n", directory, err)
		}
	},
}

func init() {
	rootCmd.AddCommand(umountCmd)
}
