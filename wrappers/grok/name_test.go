// SPDX-License-Identifier: MIT
package grok

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/mcp"
	"github.com/sessionbus/peer-common/testsocket"
)

func TestInitialNameClaimIsExclusivePerLaunchAndRecordsItsSession(t *testing.T) {
	for range 2 {
		root := testsocket.Directory(t)
		socket := filepath.Join(root, "bus")
		t.Setenv(host.SocketEnv, socket)
		launch := liveLaunch(t, launchFacts{Socket: socket, Name: "initial"})
		claim := filepath.Join(launch, initialNameClaim)
		env := managedPeerEnv(os.Environ(), testSessionID, launch)
		// A helper without a native session is not of this launch.
		if b, err := NewPeerBackend(context.Background(), setEnvironment(env, grokSessionIDEnv, "")); err == nil {
			b.Shutdown()
			t.Fatal("helper without a native session consumed claim")
		}
		check(t, !exists(claim), "invalid helper consumed name")
		var wg sync.WaitGroup
		results := make(chan *PeerBackend, 2)
		for i := range 2 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				e := setEnvironment(env, grokSessionIDEnv, []string{testSessionID, "second-native-id"}[i])
				b, err := NewPeerBackend(context.Background(), e)
				if err != nil {
					t.Error(err)
					return
				}
				results <- b
			}(i)
		}
		wg.Wait()
		close(results)
		winners := []string{}
		for b := range results {
			if b.initialName != "" {
				winners = append(winners, b.identity.SessionID)
			}
			b.Shutdown()
		}
		check(t, len(winners) == 1, "name winners=%v", winners)
		info, err := os.Stat(claim)
		must(t, err)
		owner, err := os.ReadFile(claim)
		must(t, err)
		check(t, string(owner) == winners[0]+"\n" && info.Mode().Perm() == 0600, "claim %q does not record its one session %q, or has loose permissions %v", owner, winners[0], info.Mode())
	}
}

// The first helper of a session can be replaced before it has requested the name. Its
// successor of the same session applies it; no other session and, once the name is
// confirmed, no later helper requests it.
func TestSameSessionSuccessorAppliesUnfinishedInitialName(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	recordPath := filepath.Join(root, "record")
	server, hellos := fakeDaemon(t, socket)
	defer func() { _ = server.Close() }()
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	launch := liveLaunch(t, launchFacts{Socket: socket, Name: "requested"})
	claim := filepath.Join(launch, initialNameClaim)
	env := managedPeerEnv(os.Environ(), testSessionID, launch)
	replaced, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	check(t, replaced.initialName == "requested", "first helper did not claim the name")
	replaced.Shutdown()
	owner, err := os.ReadFile(claim)
	must(t, err)
	check(t, string(owner) == testSessionID+"\n" && countFrames(records(t, recordPath), "_x.ai/session/rename") == 0, "replaced helper left claim %q or requested the name", owner)
	other, err := NewPeerBackend(context.Background(), setEnvironment(env, grokSessionIDEnv, "second-native-id"))
	must(t, err)
	check(t, other.initialName == "", "unfinished claim transferred to another session")
	other.Shutdown()
	successor, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	check(t, successor.initialName == "requested", "same-session successor did not take over the unfinished claim")
	successor.Initialized()
	named := awaitHello(t, hellos)
	check(t, named.SessionID == testSessionID && named.Name == "requested", "successor published %+v", named)
	named.ack <- true
	awaitChannel(t, successor.ready, "successor admission")
	owner, err = os.ReadFile(claim)
	must(t, err)
	check(t, string(owner) == testSessionID+"\ncompleted\n" && countFrames(records(t, recordPath), "_x.ai/session/rename") == 1, "confirmed name left claim %q, renames=%d", owner, countFrames(records(t, recordPath), "_x.ai/session/rename"))
	successor.Shutdown()
	must(t, server.Close())

	// A helper restarted after completion finds the title changed and leaves it.
	server, hellos = fakeDaemon(t, socket)
	restarted, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	defer restarted.Shutdown()
	check(t, restarted.initialName == "", "completed claim was taken again")
	restarted.Initialized()
	later := awaitHello(t, hellos)
	check(t, later.SessionID == testSessionID && later.Name == "", "restarted helper published %+v", later)
	later.ack <- true
	awaitChannel(t, restarted.ready, "restarted admission")
	check(t, countFrames(records(t, recordPath), "_x.ai/session/rename") == 1, "restarted helper requested the name again")
}

func TestEqualNativeTitleCompletesInitialNameWithoutRename(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	recordPath := filepath.Join(root, "record")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	// The row already shows the name: an earlier helper's request was applied.
	t.Setenv("GROK_TEST_TITLES", "requested")
	launch := liveLaunch(t, launchFacts{Socket: socket, Name: "requested"})
	b, err := NewPeerBackend(context.Background(), managedPeerEnv(os.Environ(), testSessionID, launch))
	must(t, err)
	defer b.Shutdown()
	b.Initialized()
	named := awaitHello(t, hellos)
	check(t, named.Name == "requested", "published %+v", named)
	named.ack <- true
	awaitChannel(t, b.ready, "Grok peer admission")
	owner, err := os.ReadFile(filepath.Join(launch, initialNameClaim))
	must(t, err)
	check(t, string(owner) == testSessionID+"\ncompleted\n", "equal title left claim %q", owner)
	check(t, countFrames(records(t, recordPath), "_x.ai/session/rename") == 0, "equal title was requested again")
}

// A claim the helper cannot read, or cannot mark completed, is an error of the helper:
// it does not guess the name's state and it publishes nothing.
func TestUnreadableInitialNameClaimStopsTheHelper(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	recordPath := filepath.Join(root, "record")
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	launch := liveLaunch(t, launchFacts{Socket: socket, Name: "requested"})
	// The claim exists and cannot be read as a file.
	must(t, os.Mkdir(filepath.Join(launch, initialNameClaim), 0o700))
	b, err := NewPeerBackend(context.Background(), managedPeerEnv(os.Environ(), testSessionID, launch))
	if err == nil {
		b.Shutdown()
		t.Fatal("helper started over an unreadable name claim")
	}
	check(t, len(records(t, recordPath)) == 0, "helper started native over an unreadable name claim")
}

func TestInitialNameCompletionFailureStopsPublication(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	recordPath := filepath.Join(root, "record")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	launch := liveLaunch(t, launchFacts{Socket: socket, Name: "requested"})
	b, err := NewPeerBackend(context.Background(), managedPeerEnv(os.Environ(), testSessionID, launch))
	must(t, err)
	defer b.Shutdown()
	// The claim is gone when the confirmed name is to be recorded.
	must(t, os.Remove(filepath.Join(launch, initialNameClaim)))
	b.Initialized()
	awaitChannel(t, b.done, "Grok helper end")
	b.mu.Lock()
	terminal := b.err
	b.mu.Unlock()
	check(t, errors.Is(terminal, os.ErrNotExist), "helper result without a recordable claim = %v", terminal)
	check(t, countFrames(records(t, recordPath), "_x.ai/session/rename") == 1, "name was not requested before its completion failed")
	select {
	case published := <-hellos:
		t.Fatalf("helper published without recording its name claim: %+v", published)
	default:
	}
}

func TestUnconfirmedInitialTitleLeavesClaimUnfinished(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	recordPath := filepath.Join(root, "record")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	// Native accepts the request, and the row then shows another title.
	t.Setenv("GROK_TEST_TITLES", "before,other")
	launch := liveLaunch(t, launchFacts{Socket: socket, Name: "requested"})
	b, err := NewPeerBackend(context.Background(), managedPeerEnv(os.Environ(), testSessionID, launch))
	must(t, err)
	defer b.Shutdown()
	b.Initialized()
	awaitChannel(t, b.done, "Grok helper end")
	b.mu.Lock()
	terminal := b.err
	b.mu.Unlock()
	check(t, terminal != nil && strings.Contains(terminal.Error(), "native initial title does not match requested name"), "helper result with an unconfirmed title = %v", terminal)
	owner, err := os.ReadFile(filepath.Join(launch, initialNameClaim))
	must(t, err)
	check(t, string(owner) == testSessionID+"\n" && countFrames(records(t, recordPath), "_x.ai/session/rename") == 1, "unconfirmed title left claim %q", owner)
	select {
	case published := <-hellos:
		t.Fatalf("helper published an unconfirmed name: %+v", published)
	default:
	}
}

func TestMCPInitializationPrecedesHeldInitialNativeRename(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	recordPath := filepath.Join(root, "record")
	release := filepath.Join(root, "rename-release")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_RENAME_BLOCK", release)
	env := managedPeerEnv(os.Environ(), testSessionID, liveLaunch(t, launchFacts{Socket: socket, Name: "requested-initial"}))
	b, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	defer b.Shutdown()
	input, writer := io.Pipe()
	reader, output := io.Pipe()
	served := make(chan error, 1)
	go func() { served <- mcp.ServeSessionbus(b, input, output, mcp.ReportHandler{}) }()
	enc, scan := json.NewEncoder(writer), bufio.NewScanner(reader)
	response := mcpResponse(t, enc, scan, 1, "initialize", map[string]any{"protocolVersion": "2025-06-18"})
	check(t, response["result"] != nil, "native initialize withheld by rename")
	waitFrame(t, recordPath, "_x.ai/session/rename", 1)
	check(t, mcpResponse(t, enc, scan, 2, "tools/list", map[string]any{})["result"] != nil, "held native rename blocks MCP reader")
	select {
	case <-hellos:
		t.Fatal("invented title published before rename finished")
	default:
	}
	must(t, os.WriteFile(release, nil, 0600))
	hello := <-hellos
	check(t, hello.SessionID == testSessionID && hello.Name == "requested-initial", "name applied to wrong identity: %+v", hello)
	hello.ack <- true
	<-b.ready
	must(t, writer.Close())
	must(t, <-served)
}
func TestFailedInitialRenameNeverTransfersClaim(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_RENAME_ERROR", "1")
	launch := liveLaunch(t, launchFacts{Socket: socket, Name: "requested"})
	env := managedPeerEnv(os.Environ(), testSessionID, launch)
	b, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	b.Initialized()
	<-b.done
	check(t, b.err != nil, "failed native rename accepted")
	owner, err := os.ReadFile(filepath.Join(launch, initialNameClaim))
	must(t, err)
	check(t, string(owner) == testSessionID+"\n", "failed rename completed or released claim %q", owner)
	later, err := NewPeerBackend(context.Background(), setEnvironment(env, grokSessionIDEnv, "later-native-id"))
	must(t, err)
	defer later.Shutdown()
	check(t, later.initialName == "", "failed rename transferred to later helper")
}
