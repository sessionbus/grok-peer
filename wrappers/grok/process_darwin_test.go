// SPDX-License-Identifier: MIT

//go:build darwin

package grok

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

type processHandle struct {
	fd  int
	pid int
}

func pidfd(t *testing.T, pid int) processHandle {
	t.Helper()
	fd, err := unix.Kqueue()
	must(t, err)
	event := unix.Kevent_t{Ident: uint64(pid), Filter: unix.EVFILT_PROC, Flags: unix.EV_ADD | unix.EV_ENABLE, Fflags: unix.NOTE_EXIT}
	if _, err = unix.Kevent(fd, []unix.Kevent_t{event}, nil, nil); err != nil {
		_ = unix.Close(fd)
		must(t, err)
	}
	return processHandle{fd: fd, pid: pid}
}

func closeProcessHandle(handle processHandle) { _ = unix.Close(handle.fd) }

func processRunning(t *testing.T, handle processHandle) bool {
	t.Helper()
	err := unix.Kill(handle.pid, 0)
	if err != nil && !errors.Is(err, unix.ESRCH) {
		must(t, err)
	}
	return err == nil
}

func waitProcessExit(t *testing.T, handle processHandle) {
	t.Helper()
	events := make([]unix.Kevent_t, 1)
	var err error
	// kevent(2) can return EINTR when a signal interrupts the wait (the Go
	// runtime retries kevent on EINTR for the same reason); retry only that.
	for {
		_, err = unix.Kevent(handle.fd, nil, events, nil)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	must(t, err)
}
