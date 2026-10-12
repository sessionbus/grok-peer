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
	check(t, !launchEnded(launch), "present launch ended")
	// Any other stat error, here a directory that cannot be searched, is not the end.
	locked := filepath.Join(root, "locked")
	must(t, os.Mkdir(locked, 0o700))
	must(t, os.Mkdir(filepath.Join(locked, "grok-launch-2"), 0o700))
	must(t, os.Chmod(locked, 0))
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	if _, err := os.Stat(filepath.Join(locked, "grok-launch-2")); err != nil && !os.IsNotExist(err) {
		check(t, !launchEnded(filepath.Join(locked, "grok-launch-2")), "a stat error other than absence ended the launch")
	}
	must(t, os.RemoveAll(launch))
	check(t, launchEnded(launch), "removed launch directory not seen as ended")
}

func TestManagedLaunchContextFollowsTheDirectoryNotItsContents(t *testing.T) {
	shortLaunchPoll(t)
	root := t.TempDir()
	launch := liveLaunch(t, launchFacts{Socket: filepath.Join(root, "bus")})
	ctx, stop := WithManagedLaunch(context.Background(), managedPeerEnv(nil, testSessionID, launch))
	defer stop()
	// Admission read the facts once; only the launcher's removal of the directory ends the launch.
	must(t, os.Remove(filepath.Join(launch, launchFactsFile)))
	select {
	case <-ctx.Done():
		t.Fatal("a missing facts file ended a live launch")
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
	launch := liveLaunch(t, launchFacts{Socket: bus})
	env := managedPeerEnv(os.Environ(), testSessionID, launch)
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
	root := t.TempDir()
	launch := liveLaunch(t, launchFacts{Socket: filepath.Join(root, "bus")})
	env := managedPeerEnv(nil, testSessionID, launch)
	check(t, ManagedHelper(env), "a helper of a live launch is not managed")
	must(t, os.RemoveAll(launch))
	check(t, !ManagedHelper(env), "a helper started after its launch ended is managed")
	_, err := NewPeerBackend(context.Background(), env)
	check(t, err != nil, "a backend was created for an ended launch")
}
