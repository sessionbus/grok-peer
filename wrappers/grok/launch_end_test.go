// SPDX-License-Identifier: MIT

package grok

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/testsocket"
)

func shortLaunchPoll(t *testing.T) {
	t.Helper()
	previous := launchPollInterval
	launchPollInterval = 50 * time.Millisecond
	t.Cleanup(func() { launchPollInterval = previous })
}

func TestLaunchEndedOnlyWhenLaunchDirectoryIsGone(t *testing.T) {
	root := t.TempDir()
	launch := filepath.Join(root, "grok-launch-1")
	must(t, os.Mkdir(launch, 0o700))
	socket := filepath.Join(launch, "grok-native.sock")
	check(t, !launchEnded(socket), "missing socket with its launch directory present ended the launch")
	must(t, os.WriteFile(socket, nil, 0o600))
	check(t, !launchEnded(socket), "present launch ended")
	// Any other stat error, here a directory that cannot be searched, is not the end.
	locked := filepath.Join(root, "locked")
	must(t, os.Mkdir(locked, 0o700))
	must(t, os.Mkdir(filepath.Join(locked, "grok-launch-2"), 0o700))
	must(t, os.Chmod(locked, 0))
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	if _, err := os.Stat(filepath.Join(locked, "grok-launch-2")); err != nil && !os.IsNotExist(err) {
		check(t, !launchEnded(filepath.Join(locked, "grok-launch-2", "grok-native.sock")), "a stat error other than absence ended the launch")
	}
	must(t, os.RemoveAll(launch))
	check(t, launchEnded(socket), "removed launch directory not seen as ended")
	check(t, !launchEnded("private"), "a relative marker's parent is the working directory")
}

func TestManagedLaunchContextFollowsTheDirectoryNotTheSocket(t *testing.T) {
	shortLaunchPoll(t)
	launch := filepath.Join(t.TempDir(), "grok-launch-1")
	must(t, os.Mkdir(launch, 0o700))
	socket := filepath.Join(launch, "grok-native.sock")
	must(t, os.WriteFile(socket, nil, 0o600))
	ctx, stop := WithManagedLaunch(context.Background(), managedPeerEnv(nil, testSessionID, socket))
	defer stop()
	// A native leader turnover can remove the socket while the launch continues.
	must(t, os.Remove(socket))
	select {
	case <-ctx.Done():
		t.Fatal("a missing socket ended a live launch")
	case <-time.After(20 * launchPollInterval):
	}
	must(t, os.RemoveAll(launch))
	awaitChannel(t, ctx.Done(), "launch end")
}

func TestManagedLaunchEndRetiresPeer(t *testing.T) {
	shortLaunchPoll(t)
	root := testsocket.Directory(t)
	bus := filepath.Join(root, "bus")
	server, hellos := fakeDaemon(t, bus)
	defer server.Close()
	t.Setenv(host.SocketEnv, bus)
	t.Setenv("GROK_TEST_RECORD", filepath.Join(root, "record"))
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	launch := filepath.Join(root, "grok-launch-1")
	must(t, os.Mkdir(launch, 0o700))
	env := managedPeerEnv(os.Environ(), testSessionID, filepath.Join(launch, "grok-native.sock"))
	ctx, stop := WithManagedLaunch(context.Background(), env)
	defer stop()
	backend, err := NewPeerBackend(ctx, env)
	must(t, err)
	defer backend.Shutdown()
	backend.Initialized()
	admitted := awaitHello(t, hellos)
	check(t, admitted.SessionID == testSessionID, "admitted identity=%+v", admitted)
	admitted.ack <- true
	awaitChannel(t, backend.ready, "Grok peer admission")
	select {
	case <-backend.done:
		t.Fatalf("Grok peer ended during its launch: %v", backend.err)
	case <-time.After(10 * launchPollInterval):
	}
	// The launcher quit: its private launch directory is gone. The peer ends and its
	// Sessionbus connection closes, so the daemon detaches the row.
	must(t, os.RemoveAll(launch))
	awaitChannel(t, ctx.Done(), "launch end")
	awaitChannel(t, backend.done, "Grok peer end")
	awaitChannel(t, admitted.connectionDone, "Sessionbus connection close")
}

func TestManagedHelperIsInactiveAfterItsLaunchEnded(t *testing.T) {
	launch := filepath.Join(t.TempDir(), "grok-launch-1")
	socket := filepath.Join(launch, "grok-native.sock")
	env := managedPeerEnv(nil, testSessionID, socket)
	check(t, !ManagedHelper(env), "a helper started after its launch ended is managed")
	_, err := NewPeerBackend(context.Background(), env)
	check(t, err != nil, "a backend was created for an ended launch")
	must(t, os.Mkdir(launch, 0o700))
	check(t, ManagedHelper(env), "a helper of a live launch is not managed")
}
