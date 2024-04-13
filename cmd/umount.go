package cmd

import (
	mnt "github.com/ozkatz/cloudzip/pkg/mount"
	"github.com/spf13/cobra"
)

var umountCmd = &cobra.Command{
	Use:     "umount",
	Short:   "Unmounts a currently mounted remote archive from the given directory",
	Example: "cz umount data_dir/",
	Aliases: []string{"unmount"},
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		directory := args[0]
		err := mnt.Umount(directory)
		if err != nil {
			die("could not unmount directory '%s': %v\n", directory, err)
		}
	},
}

func init() {
	rootCmd.AddCommand(umountCmd)
}
