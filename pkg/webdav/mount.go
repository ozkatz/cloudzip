package webdav

import (
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

var (
	MountError = errors.New("mount command failed")
)

func execMountCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		cmdText := fmt.Sprintf("%s %s", name, strings.Join(args, " "))
		return fmt.Errorf("%w: \"%s\":\n%s\n%s", MountError, cmdText, out, err)
	}
	return nil
}

func Mount(mountUrl, location string) error {
	switch runtime.GOOS {
	case "windows":
		// TODO(ozkatz): will mount this to an available drive letter.
		//  we need to retrieve the selected drive and symlink it to `location`.
		return execMountCommand("net", "use", "*", mountUrl, location)
	case "darwin":
		return execMountCommand("mount_webdav", "-S", mountUrl, location)
	case "linux":
		return execMountCommand("mount", "-t", "davfs", mountUrl, location)
	}

	return MountError
}

func Umount(location string) error {
	switch runtime.GOOS {
	case "windows":
		// TODO(ozkatz): remove symlink and unmount
		return MountError
	case "darwin":
		return execMountCommand("umount", location)
	case "linux":
		return execMountCommand("umount", location)
	}

	return MountError
}
