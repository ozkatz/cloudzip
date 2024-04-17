package mount

import (
	"errors"
	"fmt"
	"net"
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
	ErrCommandError = errors.New("mount command failed")
	ErrNotOurMount  = errors.New("this mount is not managed by cz")
)

func execMountCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		cmdText := fmt.Sprintf("%s %s", name, strings.Join(args, " "))
		return fmt.Errorf("%w: \"%s\":\n%s\n%s", ErrCommandError, cmdText, out, err)
	}
	return nil
}

func readPidFile(filepath string) (int, error) {
	data, err := os.ReadFile(filepath)
	if os.IsNotExist(err) {
		return 0, fmt.Errorf("%s: %w", filepath, ErrNotOurMount)
	} else if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, fmt.Errorf("%w: could not read mount server pid file", ErrCommandError)
	}
	return pid, nil
}

func killPid(pid int) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	signal := os.Interrupt
	if runtime.GOOS == GOOSWindows {
		signal = os.Kill
	}
	return proc.Signal(signal)
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

func NFSMount(addr string, location string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%w: could not parse address: %s", ErrCommandError, addr)
	}
	switch runtime.GOOS {
	case GOOSMacOS:
		opts := fmt.Sprintf("nolocks,vers=3,tcp,rsize=1048576,actimeo=120,port=%s,mountport=%s",
			port, port)
		return tryThenSudo("mount_nfs", "-o", opts, fmt.Sprintf("%s:/", host), location)
	case GOOSLinux:
		opts := fmt.Sprintf(
			"user,noacl,nolock,tcp,vers=3,nconnect=8,rsize=1048576,port=%s,mountport=%s",
			port, port)
		return tryThenSudo("mount", "-t", "nfs", "-o", opts, fmt.Sprintf("%s:/", host), location)
	case GOOSWindows:
		// TODO(ozkatz)
	}
	return fmt.Errorf("%w: don't know how to mount on OS: %s", ErrCommandError, runtime.GOOS)
}

func Umount(location string) error {
	pid, err := readPidFile(filepath.Join(location, ".cz", "server.pid"))
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
		err := execMountCommand("cmd.exe", "/c", "rd", location)
		if err != nil {
			return err
		}
		return killPid(pid)
	}
	return fmt.Errorf("%w: don't know how to unmount on OS: %s", ErrCommandError, runtime.GOOS)
}

func WebDavMount(addr string, location string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%w: could not parse address: %s", ErrCommandError, addr)
	}
	switch runtime.GOOS {
	case GOOSMacOS:
		return tryThenSudo("mount_webdav", "-S",
			fmt.Sprintf("http://%s:%s/mount/", host, port),
			location)
	case GOOSWindows:
		// check if existing directory
		// try to remove if empty
		// otherwise, fail
		stat, err := os.Stat(location)
		if !os.IsNotExist(err) && err != nil {
			return err
		} else if err == nil {
			if stat.IsDir() {
				err = execMountCommand("cmd.exe", "/c", "rd", location)
			} else {
				return fmt.Errorf("%w: %s: path already exists", ErrCommandError, location)
			}
		}

		// create link to url
		mountUrl := fmt.Sprintf("\\\\%s@%s\\mount", host, port)
		return execMountCommand("cmd.exe", "/c", "mklink", "/d", location, mountUrl)
	}
	return fmt.Errorf("%w: don't know how to mount on OS: %s", ErrCommandError, runtime.GOOS)
}

// fork crete a new process
func fork(args []string) (int, error) {
	path, err := exec.LookPath(os.Args[0])
	if errors.Is(err, exec.ErrDot) {
		path = fmt.Sprintf("./%s", os.Args[0])
	}
	cmd := exec.Command(path, args...)
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	pid := cmd.Process.Pid
	// release
	if err := cmd.Process.Release(); err != nil {
		return pid, err
	}
	return pid, nil
}

func Daemonize(cmd ...string) (int, error) {
	return fork(cmd)
}
