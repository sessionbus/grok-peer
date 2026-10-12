// SPDX-License-Identifier: MIT
package grok

import (
	"bufio"
	"context"
	"encoding/json"
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

// helperStderr sends os.Stderr, where the helper writes its notices, to a file until the
// test ends, and returns a reader of what has been written so far.
func helperStderr(t *testing.T) func() string {
	t.Helper()
	file, err := os.Create(filepath.Join(t.TempDir(), "stderr"))
	must(t, err)
	previous := os.Stderr
	os.Stderr = file
	t.Cleanup(func() {
		os.Stderr = previous
		_ = file.Close()
	})
	return func() string {
		body, err := os.ReadFile(file.Name())
		must(t, err)
		return string(body)
	}
}

// A claim that cannot be taken, read or interpreted costs this helper the name, never the
// session's presence: it requests nothing, says so once and publishes the native title.
func TestUnusableInitialNameClaimCostsTheNameNotThePresence(t *testing.T) {
	for name, spoil := range map[string]func(t *testing.T, launch string){
		"unreadable claim": func(t *testing.T, launch string) {
			must(t, os.Mkdir(filepath.Join(launch, initialNameClaim), 0o700))
		},
		"uninterpretable claim": func(t *testing.T, launch string) {
			must(t, os.WriteFile(filepath.Join(launch, initialNameClaim), []byte(testSessionID), 0o600))
		},
		"launch directory not private": func(t *testing.T, launch string) {
			must(t, os.Chmod(launch, 0o755))
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := testsocket.Directory(t)
			socket := filepath.Join(root, "bus")
			recordPath := filepath.Join(root, "record")
			server, hellos := fakeDaemon(t, socket)
			defer server.Close()
			t.Setenv(host.SocketEnv, socket)
			t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
			t.Setenv("GROK_TEST_RECORD", recordPath)
			t.Setenv("GROK_TEST_TITLES", "native-title")
			launch := liveLaunch(t, launchFacts{Socket: socket, Name: "requested"})
			spoil(t, launch)
			stderr := helperStderr(t)
			b, err := NewPeerBackend(context.Background(), managedPeerEnv(os.Environ(), testSessionID, launch))
			must(t, err)
			defer b.Shutdown()
			check(t, b.initialName == "", "helper took a name from an unusable claim")
			b.Initialized()
			published := awaitHello(t, hellos)
			check(t, published.SessionID == testSessionID && published.Name == "native-title", "published %+v", published)
			published.ack <- true
			awaitChannel(t, b.ready, "Grok peer admission")
			check(t, countFrames(records(t, recordPath), "_x.ai/session/rename") == 0, "name was requested over an unusable claim")
			notice := stderr()
			check(t, strings.Count(notice, "\n") == 1 && strings.HasPrefix(notice, "sessionbus: Grok helper: initial name not requested: "), "helper stderr = %q", notice)
		})
	}
}

// The name is applied and confirmed, and its completion cannot be recorded: the helper
// publishes, and the claim stays unfinished for a later helper of the same session.
func TestInitialNameCompletionFailureStillPublishes(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	recordPath := filepath.Join(root, "record")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	launch := liveLaunch(t, launchFacts{Socket: socket, Name: "requested"})
	claim := filepath.Join(launch, initialNameClaim)
	env := managedPeerEnv(os.Environ(), testSessionID, launch)
	stderr := helperStderr(t)
	b, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	defer b.Shutdown()
	must(t, os.Chmod(claim, 0o400))
	if probe, err := os.OpenFile(claim, os.O_APPEND|os.O_WRONLY, 0); err == nil {
		_ = probe.Close()
		t.Skip("file modes do not restrict this user")
	}
	b.Initialized()
	named := awaitHello(t, hellos)
	check(t, named.SessionID == testSessionID && named.Name == "requested", "published %+v", named)
	named.ack <- true
	awaitChannel(t, b.ready, "Grok peer admission")
	owner, err := os.ReadFile(claim)
	must(t, err)
	check(t, string(owner) == testSessionID+"\n" && countFrames(records(t, recordPath), "_x.ai/session/rename") == 1, "claim after a failed completion = %q", owner)
	notice := stderr()
	check(t, strings.Count(notice, "\n") == 1 && strings.HasPrefix(notice, "sessionbus: Grok helper: initial name claim not completed: "), "helper stderr = %q", notice)
	successor, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	defer successor.Shutdown()
	check(t, successor.initialName == "requested", "unfinished claim was not left to the same session")
}

// A rename which native answers without success is a failed rename: the helper ends
// as before and publishes nothing. Its claim stays unfinished, so the next helper of the
// same session requests the name again; no other session does, and nobody once it is
// confirmed.
func TestFailedInitialRenameLeavesClaimForSameSessionSuccessor(t *testing.T) {
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
	t.Setenv("GROK_TEST_RENAME_UNSUCCESSFUL", "1")
	failed, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	failed.Initialized()
	awaitChannel(t, failed.done, "failed helper end")
	failed.mu.Lock()
	terminal := failed.err
	failed.mu.Unlock()
	check(t, terminal != nil && strings.Contains(terminal.Error(), "native initial rename failed"), "helper result after an unsuccessful rename = %v", terminal)
	select {
	case published := <-hellos:
		t.Fatalf("helper published after an unsuccessful rename: %+v", published)
	default:
	}
	failed.Shutdown()
	owner, err := os.ReadFile(claim)
	must(t, err)
	check(t, string(owner) == testSessionID+"\n" && countFrames(records(t, recordPath), "_x.ai/session/rename") == 1, "unsuccessful rename left claim %q", owner)
	other, err := NewPeerBackend(context.Background(), setEnvironment(env, grokSessionIDEnv, "second-native-id"))
	must(t, err)
	check(t, other.initialName == "", "failed rename transferred to another session")
	other.Shutdown()

	// Native applies the same session's next request.
	t.Setenv("GROK_TEST_RENAME_UNSUCCESSFUL", "")
	successor, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	check(t, successor.initialName == "requested", "same-session successor did not take over the failed rename's claim")
	successor.Initialized()
	named := awaitHello(t, hellos)
	check(t, named.SessionID == testSessionID && named.Name == "requested", "successor published %+v", named)
	named.ack <- true
	awaitChannel(t, successor.ready, "successor admission")
	owner, err = os.ReadFile(claim)
	must(t, err)
	check(t, string(owner) == testSessionID+"\ncompleted\n" && countFrames(records(t, recordPath), "_x.ai/session/rename") == 2, "confirmed name left claim %q", owner)
	successor.Shutdown()
	must(t, server.Close())

	server, hellos = fakeDaemon(t, socket)
	restarted, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	defer restarted.Shutdown()
	check(t, restarted.initialName == "", "completed claim was taken again")
	restarted.Initialized()
	later := awaitHello(t, hellos)
	later.ack <- true
	awaitChannel(t, restarted.ready, "restarted admission")
	check(t, countFrames(records(t, recordPath), "_x.ai/session/rename") == 2, "restarted helper requested the name again")
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
