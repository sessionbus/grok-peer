// SPDX-License-Identifier: MIT

//go:build linux

package grok

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

type processHandle struct{ fd int }

func pidfd(t *testing.T, pid int) processHandle {
	t.Helper()
	fd, err := unix.PidfdOpen(pid, 0)
	must(t, err)
	return processHandle{fd: fd}
}

func closeProcessHandle(handle processHandle) { _ = unix.Close(handle.fd) }

func processRunning(t *testing.T, handle processHandle) bool {
	t.Helper()
	poll := []unix.PollFd{{Fd: int32(handle.fd), Events: unix.POLLIN}}
	var count int
	var err error
	// poll(2) is not restarted after a signal, so a benign signal (Go
	// preemption, SIGCHLD from a test child) can interrupt it with EINTR; retry.
	for {
		count, err = unix.Poll(poll, 0)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	must(t, err)
	return count == 0
}

func waitProcessExit(t *testing.T, handle processHandle) {
	t.Helper()
	poll := []unix.PollFd{{Fd: int32(handle.fd), Events: unix.POLLIN}}
	var err error
	// As in processRunning: poll(2) is not restarted after a signal; retry on EINTR.
	for {
		_, err = unix.Poll(poll, -1)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	must(t, err)
}
