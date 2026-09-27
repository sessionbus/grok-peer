// SPDX-License-Identifier: MIT

package grok

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/mcp"
	"github.com/sessionbus/peer-common/testsocket"
)

type peerDeliveryResult struct {
	receipt sessionkit.DeliveryReceipt
	err     error
}

func TestInteractivePlan(t *testing.T) {
	for _, test := range []struct {
		args, native []string
		groups, name string
	}{
		{[]string{"--resume", "test1", "-g", "test", "--group=test2,test3", "--yolo"}, []string{"--resume", "test1", "--always-approve"}, `["test","test2","test3"]`, ""},
		{[]string{"--group", "a,b", "--future", "value with spaces", "-n", "first", "--group", "c", "--name=second", "--", "-g", "literal", "--yolo"}, []string{"--future", "value with spaces", "--", "-g", "literal", "--yolo"}, `["a","b","c"]`, "second"},
		{[]string{"--resume"}, []string{"--resume"}, `[]`, ""},
		{[]string{"--continue", "--fork-session"}, []string{"--continue", "--fork-session"}, `[]`, ""},
		{[]string{"--resume", testSessionID, "-r" + testSessionID}, []string{"--resume", testSessionID, "-r" + testSessionID}, `[]`, ""},
		{[]string{"--session-id", testSessionID, "--peer-name", "alias"}, []string{"--session-id", testSessionID}, `[]`, "alias"},
		{[]string{"--disallowed-tools", "Read,Write"}, []string{"--disallowed-tools", "Read,Write"}, `[]`, ""},
	} {
		plan, err := InteractivePlan(test.args, []string{"PATH=/bin", host.SessionIDEnv + "=inherited", host.NameEnv + "=inherited", host.GroupsEnv + `=["inherited"]`})
		must(t, err)
		check(t, slices.Equal(plan.Args, test.native), "argv changed: %#v", plan.Args)
		check(t, environment(plan.Env, host.SessionIDEnv) == "", "launcher invented/inherited session ID")
		check(t, environment(plan.Env, host.GroupsEnv) == test.groups && environment(plan.Env, host.NameEnv) == test.name, "owned options = %#v", plan.Env)
		check(t, environment(plan.Env, ManagedEnv) == "launch", "managed topology absent")
	}
	for _, args := range [][]string{{"--no-leader"}, {"--leader"}, {"--leader-socket=elsewhere"}, {"-g"}, {"--group", "--"}, {"-n"}} {
		_, err := InteractivePlan(args, nil)
		check(t, err != nil, "conflict/missing value accepted: %#v", args)
	}
	for _, args := range [][]string{{"--deny", "MCPTool(sessionbus__sessionbus)"}, {"--disallowedTools=mcp__sessionbus"}, {"--deny=unrelated,,other"}} {
		_, err := InteractivePlan(args, nil)
		check(t, err != nil && strings.Contains(err.Error(), "disables the managed Sessionbus tool"), "managed deny reached native plan: %#v: %v", args, err)
	}
	for _, args := range [][]string{{"--single", "prompt"}, {"-pprompt"}, {"--prompt-file", "prompt.txt"}, {"--prompt-json", `[]`}, {"--output-format", "json"}, {"--json-schema", `{}`}, {"--max-turns", "1"}, {"--include-partial-messages"}} {
		_, err := InteractivePlan(args, nil)
		check(t, err != nil && strings.Contains(err.Error(), "Sessionbus Grok lane"), "headless accepted: %#v", args)
	}
	for _, args := range [][]string{{"sessions", "list"}, {"--version"}, {"plugin", "list", "--json"}} {
		plan, err := InteractivePlan(args, []string{ManagedEnv + "=inherited"})
		must(t, err)
		check(t, slices.Equal(plan.Args, args) && environment(plan.Env, ManagedEnv) == "", "native command wrapped: %#v", plan)
	}
	for _, args := range [][]string{nil, {"--always-approve"}, {"--always-approve", "--permission-mode=default"}, {"--permission-mode", "default", "--always-approve"}, {"--permission-mode", "custom-native-value"}} {
		policy, err := interactivePolicy(args)
		must(t, err)
		check(t, slices.Equal(policy, appendGrant(args...)), "explicit native policy rewritten: %#v", policy)
	}
	policy, err := interactivePolicy([]string{"--", "--always-approve"})
	must(t, err)
	check(t, slices.Equal(policy, sessionbusLeaderPolicy()), "post-delimiter operand selected policy: %#v", policy)
}

func TestManagedInteractiveConfigFailurePrecedesNativeStart(t *testing.T) {
	root, home := testsocket.Directory(t), t.TempDir()
	recordPath := filepath.Join(t.TempDir(), "record")
	if err := os.WriteFile(filepath.Join(home, grokConfigFile), []byte("token = PRIVATE_VALUE @\n"), 0600); err != nil {
		t.Fatal(err)
	}
	plan := host.ExecPlan{
		Path: "grok",
		Env: []string{
			ManagedEnv + "=launch",
			host.SocketEnv + "=" + filepath.Join(root, "sessionbus.sock"),
			"GROK_HOME=" + home,
			"GROK_TEST_RECORD=" + recordPath,
		},
	}
	err := RunInteractive(context.Background(), plan)
	if err == nil || strings.Contains(err.Error(), "PRIVATE_VALUE") || !strings.Contains(err.Error(), "invalid TOML at line") {
		t.Fatalf("config failure = %v", err)
	}
	if _, err := os.Stat(recordPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native started before config validation: %v", err)
	}
}

func TestManagedInteractiveDenyValidationPrecedesConfigWrite(t *testing.T) {
	home := t.TempDir()
	plan := host.ExecPlan{
		Path: "grok",
		Args: []string{"--deny", sessionbusNativeRule},
		Env:  []string{ManagedEnv + "=launch", "GROK_HOME=" + home},
	}
	err := RunInteractive(context.Background(), plan)
	if err == nil || !strings.Contains(err.Error(), "disables the managed Sessionbus tool") {
		t.Fatalf("deny accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(home, grokConfigFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("config written before deny validation: %v", err)
	}
}

func TestManagedDenyRejectedBeforeNativeStart(t *testing.T) {
	root := testsocket.Directory(t)
	recordPath := filepath.Join(t.TempDir(), "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	plan := host.ExecPlan{
		Path: "grok",
		Args: []string{"--deny", "MCPTool(sessionbus__sessionbus)"},
		Env:  []string{ManagedEnv + "=launch", host.SocketEnv + "=" + filepath.Join(root, "sessionbus.sock")},
	}
	err := RunInteractive(context.Background(), plan)
	check(t, err != nil && strings.Contains(err.Error(), "disables the managed Sessionbus tool"), "managed deny = %v", err)
	check(t, len(records(t, recordPath)) == 0, "managed deny started native Grok: %s", records(t, recordPath))
}

func TestNativeTitleEventsRefreshPeerWithoutDelivery(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	recordPath := filepath.Join(root, "record")
	changed := filepath.Join(root, "changed")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_TITLES", "native-title,")
	t.Setenv("GROK_TEST_ROSTER_CHANGE", changed)
	env := managedPeerEnv(os.Environ(), testSessionID, filepath.Join(root, "leader.sock"))
	env = setEnvironment(env, host.GroupsEnv, `["peer-group"]`)
	backend, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	defer backend.Shutdown()
	check(t, len(records(t, recordPath)) == 0, "native observer started before MCP initialize")
	backend.Initialized()
	initial := <-hellos
	check(t, initial.SessionID == testSessionID && initial.Name == "native-title" && slices.Equal(initial.Groups, []string{"peer-group"}), "initial native identity=%+v", initial)
	initial.ack <- true
	<-backend.ready
	must(t, os.WriteFile(changed, nil, 0600))
	renamed := <-hellos
	check(t, renamed.SessionID == testSessionID && renamed.Name == "", "empty native title replaced with invented name: %+v", renamed)
	renamed.ack <- true
	<-renamed.done
	listed, err := backend.Caller().List(context.Background(), sessionkit.SessionListRequest{})
	must(t, err)
	check(t, len(listed.Sessions) == 1, "peer absent")
	check(t, countFrames(records(t, recordPath), "_x.ai/interject") == 0, "title update required delivery")
}

func TestPeerSurvivesAbsentDaemonAndReconnectsWithoutReplay(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_OBSERVER_PID", filepath.Join(root, "observer.pid"))
	t.Setenv("GROK_TEST_TITLES", "before-outage,after-outage")
	titleChange := filepath.Join(root, "title-change")
	t.Setenv("GROK_TEST_ROSTER_CHANGE", titleChange)
	env := managedPeerEnv(os.Environ(), testSessionID, filepath.Join(root, "leader.sock"))
	env = setEnvironment(env, host.GroupsEnv, `["peer-group"]`)
	backend, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	backend.Initialized()
	process := peerBackendProcess(t, backend)
	processFD := pidfd(t, process.cmd.Process.Pid)
	defer closeProcessHandle(processFD)
	select {
	case <-backend.ready:
		t.Fatal("Grok peer became ready while Sessionbus was absent")
	case <-backend.done:
		t.Fatalf("Grok peer ended while Sessionbus was absent: %v", backend.err)
	default:
	}
	check(t, processRunning(t, processFD), "Grok observer stopped while Sessionbus was absent")

	firstServer, firstHellos := fakeDaemon(t, socket)
	first := awaitHello(t, firstHellos)
	check(t, first.SessionID == testSessionID && first.Name == "before-outage" && slices.Equal(first.Groups, []string{"peer-group"}), "first identity=%+v", first)
	first.ack <- true
	awaitChannel(t, backend.ready, "Grok peer first admission")
	first.drop()
	awaitChannel(t, first.connectionDone, "first Sessionbus connection close")
	must(t, firstServer.Close())

	awaitNotConnected(t, backend)
	_, err = backend.Caller().List(context.Background(), sessionkit.SessionListRequest{})
	var unavailable *sessionkit.ProtocolError
	check(t, errors.As(err, &unavailable) && unavailable.Code == protocol.NotConnected, "disconnected call=%v", err)
	select {
	case <-backend.done:
		t.Fatalf("Grok peer treated idle Sessionbus loss as terminal: %v", backend.err)
	default:
	}
	check(t, peerBackendProcess(t, backend) == process && processRunning(t, processFD), "Grok observer generation changed during Sessionbus outage")
	must(t, os.WriteFile(titleChange, nil, 0o600))
	awaitPeerName(t, backend, "after-outage")

	secondServer, secondHellos := fakeDaemon(t, socket)
	defer secondServer.Close()
	second := awaitHello(t, secondHellos)
	check(t, second.SessionID == first.SessionID && second.Name == "after-outage" && slices.Equal(second.Groups, first.Groups) && reflect.DeepEqual(second.Info, first.Info), "reconnected identity mismatch: first=%+v second=%+v", first, second)
	second.ack <- true
	awaitChannel(t, second.done, "second hello response")
	select {
	case method := <-second.requests:
		t.Fatalf("disconnected call replayed after reconnect as %q", method)
	default:
	}
	listed := awaitPeerList(t, backend)
	check(t, len(listed.Sessions) == 1 && listed.Sessions[0].SessionID == testSessionID+"@local", "reconnected roster=%+v", listed)
	check(t, <-second.requests == "session.list", "post-reconnect method mismatch")
	check(t, peerBackendProcess(t, backend) == process && processRunning(t, processFD), "Grok observer generation changed after reconnect")

	backend.Shutdown()
	check(t, !processRunning(t, processFD), "Grok observer survived joined shutdown")
}

func TestPeerSupersededIsTerminalAndJoinsObserver(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_OBSERVER_PID", filepath.Join(root, "observer.pid"))
	backend, err := NewPeerBackend(context.Background(), managedPeerEnv(os.Environ(), testSessionID, filepath.Join(root, "leader.sock")))
	must(t, err)
	backend.Initialized()
	admitted := awaitHello(t, hellos)
	admitted.ack <- true
	awaitChannel(t, backend.ready, "Grok peer admission")
	process := peerBackendProcess(t, backend)
	processFD := pidfd(t, process.cmd.Process.Pid)
	defer closeProcessHandle(processFD)
	admitted.supersede()
	awaitChannel(t, backend.done, "superseded Grok peer shutdown")
	backend.mu.Lock()
	terminal := backend.err
	backend.mu.Unlock()
	var failure *sessionkit.ProtocolError
	check(t, errors.As(terminal, &failure) && failure.Code == protocol.Superseded, "superseded terminal error=%v", terminal)
	check(t, !processRunning(t, processFD), "superseded Grok observer survived shutdown")
	backend.Shutdown()
}

func managedPeerEnv(env []string, id, leader string) []string {
	env = setEnvironment(env, grokSessionIDEnv, id)
	env = setEnvironment(env, grokLeaderSocketEnv, leader)
	return setEnvironment(env, ManagedEnv, leader)
}

func TestProductSessionIDsCreateDistinctPeers(t *testing.T) {
	for index, id := range []string{testSessionID, "01a07800-94fb-7b12-b531-2f0509e033f1"} {
		root := testsocket.Directory(t)
		socket := filepath.Join(root, "sessionbus.sock")
		server, hellos := fakeDaemon(t, socket)
		t.Setenv(host.SocketEnv, socket)
		t.Setenv("GROK_TEST_SESSION_ID", id)
		environment := setEnvironment(setEnvironment(os.Environ(), grokSessionIDEnv, id), grokLeaderSocketEnv, filepath.Join(root, "leader.sock"))
		environment = setEnvironment(environment, ManagedEnv, environmentValue(environment, grokLeaderSocketEnv))
		opened := make(chan *PeerBackend, 1)
		go func() {
			backend, _ := NewPeerBackend(context.Background(), environment)
			backend.Initialized()
			opened <- backend
		}()
		hello := <-hellos
		check(t, hello.SessionID == id && hello.Name == "", "helper %d hello = %#v", index+1, hello)
		hello.ack <- true
		backend := <-opened
		backend.mu.Lock()
		actual := backend.identity.SessionID
		backend.mu.Unlock()
		check(t, actual == id, "helper %d identity changed", index+1)
		backend.Shutdown()
		server.Close()
	}
}

func TestPeerMCPServesWhileBusAdmissionIsHeld(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "sessionbus.sock")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	environment := setEnvironment(setEnvironment(os.Environ(), grokSessionIDEnv, testSessionID), grokLeaderSocketEnv, filepath.Join(root, "leader.sock"))
	environment = setEnvironment(environment, ManagedEnv, environmentValue(environment, grokLeaderSocketEnv))
	backend, err := NewPeerBackend(context.Background(), environment)
	must(t, err)
	input, writeInput := io.Pipe()
	readOutput, output := io.Pipe()
	served := make(chan error, 1)
	go func() {
		served <- mcp.ServeSessionbus(backend, input, output, mcp.ReportHandler{})
		_ = output.Close()
	}()
	encoder := json.NewEncoder(writeInput)
	scanner := bufio.NewScanner(readOutput)
	check(t, mcpResponse(t, encoder, scanner, 1, "initialize", map[string]any{"protocolVersion": "2025-06-18"})["result"] != nil, "MCP initialize failed")
	check(t, mcpResponse(t, encoder, scanner, 2, "tools/list", map[string]any{})["result"] != nil, "MCP tools/list failed")
	hello := <-hellos
	hello.ack <- false
	<-backend.done
	terminal := mcpResponse(t, encoder, scanner, 3, "tools/call", map[string]any{"name": "sessionbus", "arguments": map[string]any{"action": "list"}})
	failed, _ := terminal["result"].(map[string]any)
	check(t, failed["isError"] == true, "terminal tools/call = %#v", terminal)
	must(t, writeInput.Close())
	must(t, <-served)
	backend.Shutdown()
}

func TestPeerDeliveryOwnerShutdownCrossesBlockedInterject(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "sessionbus.sock")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	recordPath := filepath.Join(root, "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_INTERJECT_BLOCK", filepath.Join(root, "never"))
	observerPID := filepath.Join(root, "observer.pid")
	t.Setenv("GROK_TEST_OBSERVER_PID", observerPID)
	environment := setEnvironment(setEnvironment(os.Environ(), grokSessionIDEnv, testSessionID), grokLeaderSocketEnv, filepath.Join(root, "leader.sock"))
	environment = setEnvironment(environment, ManagedEnv, environmentValue(environment, grokLeaderSocketEnv))
	backend, err := NewPeerBackend(context.Background(), environment)
	must(t, err)
	backend.Initialized()
	hello := <-hellos
	hello.ack <- true
	<-backend.ready
	delivered := deliverPeer(backend, context.Background(), delivery("blocked"))
	waitFrame(t, recordPath, "_x.ai/interject", 1)
	pidfd := interactivePidfd(t, observerPID)
	defer closeProcessHandle(pidfd)
	closed := make(chan struct{})
	go func() { backend.Shutdown(); close(closed) }()
	<-closed
	result := <-delivered
	check(t, result.receipt.Disposition != "injected", "shutdown invented admission: %#v / %v", result.receipt, result.err)
	check(t, !processRunning(t, pidfd), "blocked observer survived helper shutdown")
}

func TestPeerDeliveryOwnerSerializesTwoReceipts(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "sessionbus.sock")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	recordPath := filepath.Join(root, "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	cwd, err := os.Getwd()
	must(t, err)
	t.Setenv("GROK_TEST_CWD", cwd)
	environment := setEnvironment(setEnvironment(os.Environ(), grokSessionIDEnv, testSessionID), grokLeaderSocketEnv, filepath.Join(root, "leader.sock"))
	environment = setEnvironment(environment, ManagedEnv, environmentValue(environment, grokLeaderSocketEnv))
	backend, err := NewPeerBackend(context.Background(), environment)
	must(t, err)
	backend.Initialized()
	hello := <-hellos
	hello.ack <- true
	<-backend.ready
	results := make(chan peerDeliveryResult, 2)
	for _, message := range []string{"first", "second"} {
		go func(message string) {
			request := delivery(message)
			request.MessageID = message
			receipt, err := backend.deliver(context.Background(), sessionkit.PeerIdentity{SessionID: testSessionID}, request)
			results <- peerDeliveryResult{receipt: receipt, err: err}
		}(message)
	}
	for range 2 {
		result := <-results
		must(t, result.err)
		check(t, result.receipt.Disposition == "injected", "delivery receipt = %#v", result.receipt)
	}
	check(t, len(peerClientPIDs(t, records(t, recordPath))) == 1, "serialized deliveries opened more than one observer")
	check(t, countFrames(records(t, recordPath), "_x.ai/interject") == 2, "serialized deliveries lost an interject")
	backend.Shutdown()
}

func TestPeerHelperRequiresProductIdentity(t *testing.T) {
	_, err := NewPeerBackend(context.Background(), nil)
	check(t, err != nil && strings.Contains(err.Error(), "start Grok with grok-peer"), "missing identity = %v", err)
}

func TestInteractiveLauncherOwnsLeaderHoldAndTUI(t *testing.T) {
	root := testsocket.Directory(t)
	recordPath := filepath.Join(root, "record")
	socket := filepath.Join(root, "sessionbus.sock")
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	started := filepath.Join(root, "interactive-started")
	leaderPID, holdPID, tuiPID := filepath.Join(root, "leader.pid"), filepath.Join(root, "hold.pid"), filepath.Join(root, "tui.pid")
	t.Setenv("GROK_TEST_INTERACTIVE_STARTED", started)
	t.Setenv("GROK_TEST_LEADER_PID", leaderPID)
	t.Setenv("GROK_TEST_OBSERVER_PID", holdPID)
	t.Setenv("GROK_TEST_INTERACTIVE_PID", tuiPID)
	plan, err := InteractivePlan([]string{"--session-id", testSessionID, "--group", "team", "--cwd", root, "--model", "--yolo"}, os.Environ())
	must(t, err)
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = RunInteractive(ctx, plan)
		close(done)
	}()
	t.Cleanup(func() {
		cancel(context.Canceled)
		<-done
	})
	<-fileReady(started)
	waitFrame(t, recordPath, "authenticate", 1)
	frames := records(t, recordPath)
	foundTUI, foundLeader := false, false
	for _, raw := range frames {
		var start struct {
			Kind  string `json:"kind"`
			Value struct {
				Arguments []string `json:"arguments"`
			} `json:"value"`
		}
		must(t, json.Unmarshal(raw, &start))
		if start.Kind == "START" && slices.Contains(start.Value.Arguments, "--leader") && !slices.Contains(start.Value.Arguments, "stdio") {
			index := slices.Index(start.Value.Arguments, "--model")
			check(t, index >= 0 && index+1 < len(start.Value.Arguments) && start.Value.Arguments[index+1] == "--yolo", "native model value rewritten: %q", start.Value.Arguments)
			check(t, !slices.Contains(start.Value.Arguments, "--always-approve"), "model value selected native bypass: %q", start.Value.Arguments)
			foundTUI = true
		}
		if start.Kind == "START" && slices.Contains(start.Value.Arguments, "agent") && slices.Contains(start.Value.Arguments, "leader") {
			check(t, slices.Contains(start.Value.Arguments, "--allow") && slices.Contains(start.Value.Arguments, "MCPTool(sessionbus__sessionbus)"), "private leader omitted exact Sessionbus grant: %q", start.Value.Arguments)
			check(t, !slices.Contains(start.Value.Arguments, "--always-approve"), "model value selected private-leader bypass: %q", start.Value.Arguments)
			foundLeader = true
		}
	}
	check(t, foundTUI && foundLeader, "native interactive topology missing: tui=%t leader=%t", foundTUI, foundLeader)
	check(t, countStartsContaining(frames, "--allow", "MCPTool(sessionbus__sessionbus)") == 1, "Sessionbus grant escaped the one private leader")
	clients := peerClientPIDs(t, frames)
	check(t, len(clients) == 1 && slices.Equal(peerClientMethods(frames, clients[0]), []string{"initialize", "authenticate"}), "startup hold was not the only quiet ACP client: %#v", frames)
	check(t, countFrames(frames, "_x.ai/sessions/list") == 0, "launcher queried the roster")
	check(t, containsStartEnv(frames, "leader", host.SocketEnv, socket) && containsStartEnv(frames, "leader", host.GroupsEnv, `["team"]`), "leader did not inherit helper bus identity")
	check(t, leaderManagedMarkerMatchesSocket(frames), "leader did not receive its computed socket as the managed marker")
	check(t, !containsStart(frames, "SESSIONBUS_LANE_SOCKET"), "interactive launcher published a private action endpoint")
	leaderPidfd, holdPidfd, tuiPidfd := interactivePidfd(t, leaderPID), interactivePidfd(t, holdPID), interactivePidfd(t, tuiPID)
	defer closeProcessHandle(leaderPidfd)
	defer closeProcessHandle(holdPidfd)
	defer closeProcessHandle(tuiPidfd)
	cancel(testSignal{syscall.SIGINT})
	var exited *exec.ExitError
	<-done
	err = runErr
	check(t, errors.As(err, &exited) && exited.ProcessState.Sys().(syscall.WaitStatus).Signal() == syscall.SIGINT, "signalled Grok child = %v", err)
	check(t, !processRunning(t, leaderPidfd) && !processRunning(t, holdPidfd) && !processRunning(t, tuiPidfd), "interactive dependency survived shutdown")
	check(t, !exists(filepath.Join(root, "lanes", host.LaunchTokenDigest(testSessionID)+".sock")), "peer endpoint remains")
}

func TestStartupHoldExitStopsInteractiveOwner(t *testing.T) {
	root := testsocket.Directory(t)
	recordPath := filepath.Join(root, "record")
	t.Setenv(host.SocketEnv, filepath.Join(root, "sessionbus.sock"))
	t.Setenv("GROK_TEST_RECORD", recordPath)
	tuiPID, leaderPID, holdPID := filepath.Join(root, "tui.pid"), filepath.Join(root, "leader.pid"), filepath.Join(root, "hold.pid")
	release := filepath.Join(root, "release-hold")
	t.Setenv("GROK_TEST_INTERACTIVE_PID", tuiPID)
	t.Setenv("GROK_TEST_LEADER_PID", leaderPID)
	t.Setenv("GROK_TEST_OBSERVER_PID", holdPID)
	t.Setenv("GROK_TEST_HOLD_EXIT", release)
	plan, err := InteractivePlan([]string{"--session-id", testSessionID, "--cwd", root}, os.Environ())
	must(t, err)
	done := make(chan error, 1)
	go func() { done <- RunInteractive(context.Background(), plan) }()
	tuiPidfd, leaderPidfd, holdPidfd := interactivePidfd(t, tuiPID), interactivePidfd(t, leaderPID), interactivePidfd(t, holdPID)
	defer closeProcessHandle(tuiPidfd)
	defer closeProcessHandle(leaderPidfd)
	defer closeProcessHandle(holdPidfd)
	must(t, os.WriteFile(release, nil, 0o600))
	err = <-done
	check(t, strings.Contains(err.Error(), "Grok startup hold closed"), "launcher error = %v", err)
	check(t, !processRunning(t, tuiPidfd) && !processRunning(t, holdPidfd) && !processRunning(t, leaderPidfd), "bootstrap process survived startup-hold failure")
	clients := peerClientPIDs(t, records(t, recordPath))
	check(t, len(clients) == 1 && slices.Equal(peerClientMethods(records(t, recordPath), clients[0]), []string{"initialize", "authenticate"}), "startup hold was not quiet")
}

func TestInteractiveLauncherReturnsProductExit(t *testing.T) {
	root := testsocket.Directory(t)
	t.Setenv(host.SocketEnv, filepath.Join(root, "sessionbus.sock"))
	leaderPID, holdPID := filepath.Join(root, "leader.pid"), filepath.Join(root, "hold.pid")
	interactivePID, release := filepath.Join(root, "interactive.pid"), filepath.Join(root, "release")
	t.Setenv("GROK_TEST_LEADER_PID", leaderPID)
	t.Setenv("GROK_TEST_OBSERVER_PID", holdPID)
	t.Setenv("GROK_TEST_INTERACTIVE_PID", interactivePID)
	t.Setenv("GROK_TEST_INTERACTIVE_EXIT", "7")
	t.Setenv("GROK_TEST_INTERACTIVE_EXIT_BARRIER", release)
	plan, err := InteractivePlan([]string{"--session-id", testSessionID, "--cwd", root}, os.Environ())
	must(t, err)
	done := make(chan error, 1)
	go func() { done <- RunInteractive(context.Background(), plan) }()
	leaderPidfd, holdPidfd := interactivePidfd(t, leaderPID), interactivePidfd(t, holdPID)
	must(t, os.WriteFile(release, nil, 0o600))
	err = <-done
	var exited *exec.ExitError
	check(t, errors.As(err, &exited) && exited.ExitCode() == 7, "exit = %v", err)
	check(t, !processRunning(t, leaderPidfd) && !processRunning(t, holdPidfd), "Grok dependencies survived the TUI")
	closeProcessHandle(leaderPidfd)
	closeProcessHandle(holdPidfd)
}

func TestLeaderCreatesDefaultStateRoot(t *testing.T) {
	root := filepath.Join(testsocket.Directory(t), "state")
	recordPath := filepath.Join(t.TempDir(), "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv(host.SocketEnv, "")
	t.Setenv("XDG_RUNTIME_DIR", root)
	socket := sessionkit.Socket()
	check(t, !exists(filepath.Dir(socket)), "default run directory already exists")
	leader, err := startLeader(context.Background(), socket, host.LaunchTokenDigest(testSessionID), t.TempDir(), "default", os.Environ())
	must(t, err)
	check(t, grokSocketReady(leaderSocket(socket, host.LaunchTokenDigest(testSessionID))), "leader socket was not created")
	check(t, containsStart(records(t, recordPath), "--allow", "MCPTool(sessionbus__sessionbus)", "--permission-mode", "default", "agent", "leader"), "lane leader policy missing: %s", records(t, recordPath))
	must(t, closeNative("leader", leader))
}

func TestExactRosterAuthority(t *testing.T) {
	yolo, resident := true, true
	live := peerSession{SessionID: testSessionID, Resident: &resident, Activity: "idle", Yolo: &yolo}
	for _, test := range []struct {
		name     string
		sessions []peerSession
		noLeader bool
	}{
		{"absent", nil, true},
		{"duplicate", []peerSession{live, live}, false},
		{"missing resident", []peerSession{{SessionID: testSessionID, Activity: "idle", Yolo: &yolo}}, false},
		{"not resident", []peerSession{{SessionID: testSessionID, Resident: new(bool), Activity: "idle", Yolo: &yolo}}, false},
		{"missing yolo", []peerSession{{SessionID: testSessionID, Resident: &resident, Activity: "idle"}}, false},
		{"unknown activity", []peerSession{{SessionID: testSessionID, Resident: &resident, Activity: "starting", Yolo: &yolo}}, false},
	} {
		_, err := exactRoster(test.sessions, testSessionID)
		check(t, err != nil && errors.Is(err, errNoLeader) == test.noLeader, "%s = %v", test.name, err)
	}
	_, err := exactRoster([]peerSession{live}, testSessionID)
	must(t, err)
}

func TestPeerShutdownKillsItsObserverProcessGroup(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "sessionbus.sock")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_DESCENDANT_PID", filepath.Join(root, "descendant.pid"))
	cwd, err := os.Getwd()
	must(t, err)
	t.Setenv("GROK_TEST_CWD", cwd)
	environment := setEnvironment(setEnvironment(os.Environ(), grokSessionIDEnv, testSessionID), grokLeaderSocketEnv, filepath.Join(root, "leader.sock"))
	environment = setEnvironment(environment, ManagedEnv, environmentValue(environment, grokLeaderSocketEnv))
	opened := make(chan *PeerBackend, 1)
	go func() {
		backend, _ := NewPeerBackend(context.Background(), environment)
		backend.Initialized()
		opened <- backend
	}()
	hello := <-hellos
	hello.ack <- true
	backend := <-opened
	receipt, err := backend.deliver(context.Background(), sessionkit.PeerIdentity{SessionID: testSessionID}, delivery("peer message"))
	must(t, err)
	check(t, receipt.Disposition == "injected", "delivery = %#v", receipt)
	body, err := os.ReadFile(filepath.Join(root, "descendant.pid"))
	must(t, err)
	var pid int
	_, err = fmt.Sscan(string(body), &pid)
	must(t, err)
	pidfd := pidfd(t, pid)
	defer closeProcessHandle(pidfd)
	backend.Shutdown()
	waitProcessExit(t, pidfd)
	check(t, !processRunning(t, pidfd), "observer descendant survived shutdown")
}

type hello struct {
	SessionID      string         `json:"session_id"`
	Name           string         `json:"name"`
	Product        string         `json:"product"`
	Groups         []string       `json:"groups"`
	Info           map[string]any `json:"info"`
	ack            chan bool
	done           chan struct{}
	drop           func()
	supersede      func()
	connectionDone <-chan struct{}
	requests       <-chan string
}

type testSignal struct{ os.Signal }

func (s testSignal) Error() string           { return s.String() }
func (s testSignal) CaughtSignal() os.Signal { return s.Signal }

func fakeDaemon(t *testing.T, path string) (net.Listener, <-chan hello) {
	t.Helper()
	listener, err := net.Listen("unix", path)
	must(t, err)
	hellos := make(chan hello, 4)
	requests := make(chan string, 64)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer connection.Close()
		connectionDone := make(chan struct{})
		defer close(connectionDone)
		scanner := bufio.NewScanner(connection)
		var write sync.Mutex
		var admitted hello
		for scanner.Scan() {
			var frame struct {
				ID     int64           `json:"id"`
				Method string          `json:"method"`
				Params json.RawMessage `json:"params"`
			}
			if json.Unmarshal(scanner.Bytes(), &frame) != nil {
				return
			}
			if frame.Method == "session.hello" {
				var value hello
				_ = json.Unmarshal(frame.Params, &value)
				value.ack = make(chan bool, 1)
				value.done = make(chan struct{})
				value.drop = func() { _ = connection.Close() }
				value.supersede = func() {
					write.Lock()
					_ = json.NewEncoder(connection).Encode(map[string]any{"jsonrpc": "2.0", "id": 9001, "method": "session.superseded", "params": map[string]any{}})
					write.Unlock()
				}
				value.connectionDone = connectionDone
				value.requests = requests
				hellos <- value
				go func(id int64, value hello) {
					defer close(value.done)
					accepted := <-value.ack
					write.Lock()
					if accepted {
						admitted = value
						_ = json.NewEncoder(connection).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{}})
					} else {
						_ = json.NewEncoder(connection).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32602, "message": "invalid_hello"}})
					}
					write.Unlock()
				}(frame.ID, value)
				continue
			}
			requests <- frame.Method
			if frame.Method == "session.list" {
				write.Lock()
				row := sessionkit.SessionSummary{SessionID: admitted.SessionID + "@local", Kind: "peer", Product: admitted.Product, Name: admitted.Name + "@local", Groups: admitted.Groups, Connected: true, Info: admitted.Info}
				_ = json.NewEncoder(connection).Encode(map[string]any{"jsonrpc": "2.0", "id": frame.ID, "result": sessionkit.SessionListResult{Sessions: []sessionkit.SessionSummary{row}}})
				write.Unlock()
				continue
			}
			if frame.Method != "" {
				write.Lock()
				_ = json.NewEncoder(connection).Encode(map[string]any{"jsonrpc": "2.0", "id": frame.ID, "result": map[string]any{}})
				write.Unlock()
			}
		}
	}()
	return listener, hellos
}

func awaitHello(t *testing.T, hellos <-chan hello) hello {
	t.Helper()
	select {
	case value := <-hellos:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Sessionbus hello")
		return hello{}
	}
}

func awaitChannel(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func peerBackendProcess(t *testing.T, backend *PeerBackend) *nativeProcess {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		backend.mu.Lock()
		process := backend.process
		backend.mu.Unlock()
		if process != nil && process.cmd != nil && process.cmd.Process != nil {
			return process
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for Grok observer process")
	return nil
}

func awaitPeerName(t *testing.T, backend *PeerBackend, expected string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		backend.mu.Lock()
		name := backend.identity.Name
		backend.mu.Unlock()
		if name == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Grok peer did not retain native title %q", expected)
}

func awaitNotConnected(t *testing.T, backend *PeerBackend) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := backend.Call(context.Background(), "session.list", sessionkit.SessionListRequest{})
		var failure *sessionkit.ProtocolError
		if errors.As(err, &failure) && failure.Code == protocol.NotConnected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Grok peer did not report disconnected transport")
}

func awaitPeerList(t *testing.T, backend *PeerBackend) sessionkit.SessionListResult {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		result, err := backend.Caller().List(context.Background(), sessionkit.SessionListRequest{})
		if err == nil {
			return result
		}
		var failure *sessionkit.ProtocolError
		if !errors.As(err, &failure) || failure.Code != protocol.NotConnected {
			t.Fatalf("post-reconnect list failed: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Grok peer did not reconnect")
	return sessionkit.SessionListResult{}
}

func deliverPeer(backend *PeerBackend, ctx context.Context, request sessionkit.DeliveryRequest) <-chan peerDeliveryResult {
	result := make(chan peerDeliveryResult, 1)
	go func() {
		receipt, err := backend.deliver(ctx, sessionkit.PeerIdentity{SessionID: testSessionID}, request)
		result <- peerDeliveryResult{receipt: receipt, err: err}
	}()
	return result
}

func mcpResponse(t *testing.T, encoder *json.Encoder, scanner *bufio.Scanner, id int, method string, params any) map[string]any {
	t.Helper()
	must(t, encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
	check(t, scanner.Scan(), "MCP response absent: %v", scanner.Err())
	var response map[string]any
	must(t, json.Unmarshal(scanner.Bytes(), &response))
	check(t, response["id"] == float64(id), "MCP response id = %#v", response)
	return response
}

func interactivePidfd(t *testing.T, path string) processHandle {
	t.Helper()
	<-fileReady(path)
	body, err := os.ReadFile(path)
	must(t, err)
	var pid int
	_, err = fmt.Sscan(string(body), &pid)
	must(t, err)
	return pidfd(t, pid)
}

func peerClientPIDs(t *testing.T, rows []json.RawMessage) []int {
	t.Helper()
	var result []int
	for _, raw := range rows {
		var record struct {
			Kind  string `json:"kind"`
			Value struct {
				PID       int      `json:"pid"`
				Arguments []string `json:"arguments"`
			} `json:"value"`
		}
		must(t, json.Unmarshal(raw, &record))
		if record.Kind == "START" && slices.Contains(record.Value.Arguments, "--leader") && slices.Contains(record.Value.Arguments, "stdio") {
			result = append(result, record.Value.PID)
		}
	}
	return result
}

func peerClientMethods(rows []json.RawMessage, pid int) []string {
	var result []string
	for _, raw := range rows {
		var record struct {
			Kind  string `json:"kind"`
			Value struct {
				PID    int    `json:"_testPID"`
				Method string `json:"method"`
			} `json:"value"`
		}
		if json.Unmarshal(raw, &record) == nil && record.Kind == "FRAME" && record.Value.PID == pid {
			result = append(result, record.Value.Method)
		}
	}
	return result
}

func containsStartEnv(rows []json.RawMessage, argument, name, value string) bool {
	for _, raw := range rows {
		var record struct {
			Kind  string `json:"kind"`
			Value struct {
				Arguments   []string          `json:"arguments"`
				Environment map[string]string `json:"environment"`
			} `json:"value"`
		}
		if json.Unmarshal(raw, &record) == nil && record.Kind == "START" && slices.Contains(record.Value.Arguments, argument) && record.Value.Environment[name] == value {
			return true
		}
	}
	return false
}

func leaderManagedMarkerMatchesSocket(rows []json.RawMessage) bool {
	for _, raw := range rows {
		var record struct {
			Kind  string `json:"kind"`
			Value struct {
				Arguments   []string          `json:"arguments"`
				Environment map[string]string `json:"environment"`
			} `json:"value"`
		}
		if json.Unmarshal(raw, &record) != nil || record.Kind != "START" || !slices.Contains(record.Value.Arguments, "leader") {
			continue
		}
		index := slices.Index(record.Value.Arguments, "--leader-socket")
		return index >= 0 && index+1 < len(record.Value.Arguments) && record.Value.Environment[ManagedEnv] == record.Value.Arguments[index+1]
	}
	return false
}

func environment(values []string, name string) string {
	for _, value := range values {
		if key, body, found := strings.Cut(value, "="); found && key == name {
			return body
		}
	}
	return ""
}

func TestResumingSameNativeIDUsesNewLaunchResources(t *testing.T) {
	root := testsocket.Directory(t)
	recordPath := filepath.Join(root, "record")
	t.Setenv(host.SocketEnv, filepath.Join(root, "bus"))
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_INTERACTIVE_EXIT", "7")
	for range 2 {
		plan, err := InteractivePlan([]string{"--resume", testSessionID, "-n", "initial"}, os.Environ())
		must(t, err)
		var exited *exec.ExitError
		check(t, errors.As(RunInteractive(context.Background(), plan), &exited) && exited.ExitCode() == 7, "native exit was not retained")
	}
	paths := []string{}
	for _, raw := range records(t, recordPath) {
		var record struct {
			Kind  string `json:"kind"`
			Value struct {
				Arguments []string `json:"arguments"`
			} `json:"value"`
		}
		must(t, json.Unmarshal(raw, &record))
		if record.Kind != "START" || !slices.Contains(record.Value.Arguments, "leader") {
			continue
		}
		paths = append(paths, nativeOption(record.Value.Arguments, "--leader-socket"))
	}
	check(t, len(paths) == 2 && paths[0] != paths[1], "same native ID reused launch identity: %#v", paths)
	for _, path := range paths {
		check(t, !exists(filepath.Dir(path)), "launch directory remains: %s", path)
	}
}
