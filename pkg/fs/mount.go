package fs

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

const (
	GOOSWindows = "windows"
	GOOSMacOS   = "darwin"
	GOOSLinux   = "linux"
)

var (
	MountError     = errors.New("mount command failed")
	ErrNotOurMount = errors.New("not a zip mount")
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

func readPidFile(root string) (int, error) {
	pidFilePath := filepath.Join(root, PidFilePath)
	data, err := os.ReadFile(pidFilePath)
	if os.IsNotExist(err) {
		return 0, fmt.Errorf("%s: %w", root, ErrNotOurMount)
	} else if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, fmt.Errorf("%w: could not read mount server pid file", MountError)
	}
	return pid, nil
}

func killPid(pid int) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return proc.Kill()
}

func tryThenSudo(cmd string, args ...string) error {
	var originalErr error
	originalErr = execMountCommand(cmd, args...)
	if originalErr == nil {
		return nil
	}
	sudoArgs := append([]string{cmd}, args...)
	err := execMountCommand("sudo", sudoArgs...)
	if err != nil {
		return originalErr
	}
	return nil // sudo was successful!
}

func Mount(port int, location string) error {
	switch runtime.GOOS {
	case GOOSMacOS:
		opts := fmt.Sprintf("nolocks,vers=3,tcp,rsize=1048576,actimeo=120,port=%d,mountport=%d",
			port, port)
		return tryThenSudo("mount_nfs", "-o", opts, "localhost:/", location)
	case GOOSLinux:
		opts := fmt.Sprintf(
			"user,noacl,nolock,tcp,vers=3,rsize=1048576,port=%d,mountport=%d",
			port, port)
		return tryThenSudo("mount", "-t", "nfs", "-o", opts, "localhost:/", location)
	case GOOSWindows:
		// TODO(ozkatz)
	}

	return MountError
}

func Umount(location string) error {
	pid, err := readPidFile(location)
	if err != nil {
		return err
	}

	switch runtime.GOOS {
	case GOOSMacOS, GOOSLinux:
		err := tryThenSudo("umount", location)
		if err != nil {
			return err
		}
		return killPid(pid)
	case GOOSWindows:
		// TODO(ozkatz)
	}
	return MountError
}
