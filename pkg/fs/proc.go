package fs

import (
	"io"
	"os"
	"os/exec"
)

// fork crete a new process
func fork(args []string) (int, io.Reader, error) {
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = os.Environ()
	cmd.Stdin = nil
	cmd.Stderr = nil
	cmd.ExtraFiles = nil
	out, err := cmd.StdoutPipe()
	if err != nil {
		return 0, nil, err
	}
	if err := cmd.Start(); err != nil {
		return 0, nil, err
	}
	pid := cmd.Process.Pid
	// release
	if err := cmd.Process.Release(); err != nil {
		return pid, out, err
	}
	return pid, out, nil
}

func Daemonize(cmd ...string) (int, io.Reader, error) {
	return fork(cmd)
}
