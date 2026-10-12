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
		// Caller rules and modes reach the TUI as given, a rule naming the Sessionbus tool included.
		{[]string{"--allow", "Bash(pwd)", "--deny", "MCPTool(sessionbus__sessionbus)", "--allowedTools=Read(*)", "--disallowedTools", "mcp__sessionbus", "--deny=unrelated,,other"}, []string{"--allow", "Bash(pwd)", "--deny", "MCPTool(sessionbus__sessionbus)", "--allowedTools=Read(*)", "--disallowedTools", "mcp__sessionbus", "--deny=unrelated,,other"}, `[]`, ""},
		{[]string{"--permission-mode", "default", "--always-approve"}, []string{"--permission-mode", "default", "--always-approve"}, `[]`, ""},
		{[]string{"--sandbox", "off"}, []string{"--sandbox", "off"}, `[]`, ""},
		{[]string{"--sandbox=none", "--sandbox", "none", "--sandbox=off"}, []string{"--sandbox=none", "--sandbox", "none", "--sandbox=off"}, `[]`, ""},
		// An empty sandbox value is native's no override; a missing one is native's to judge.
		{[]string{"--sandbox="}, []string{"--sandbox="}, `[]`, ""},
		{[]string{"--sandbox", ""}, []string{"--sandbox", ""}, `[]`, ""},
		{[]string{"--sandbox"}, []string{"--sandbox"}, `[]`, ""},
		{[]string{"--sandbox", "--", "workspace"}, []string{"--sandbox", "--", "workspace"}, `[]`, ""},
		// The required value of a native option is its value, whatever it spells.
		{[]string{"--model", "--sandbox"}, []string{"--model", "--sandbox"}, `[]`, ""},
		{[]string{"--rules", "--print"}, []string{"--rules", "--print"}, `[]`, ""},
		{[]string{"--system-prompt", "--memory-flush"}, []string{"--system-prompt", "--memory-flush"}, `[]`, ""},
		{[]string{"--model=--single"}, []string{"--model=--single"}, `[]`, ""},
		{[]string{"--model", "--single", "--agent", "--leader", "--effort", "-pro"}, []string{"--model", "--single", "--agent", "--leader", "--effort", "-pro"}, `[]`, ""},
		// Nothing after the delimiter is a flag.
		{[]string{"--", "--sandbox", "workspace", "--print", "--memory-flush", "--leader", "--leader-socket", "elsewhere", "-p"}, []string{"--", "--sandbox", "workspace", "--print", "--memory-flush", "--leader", "--leader-socket", "elsewhere", "-p"}, `[]`, ""},
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
	// An option whose value is optional steps over nothing: the next token is a flag.
	for _, args := range [][]string{{"--no-leader"}, {"--leader"}, {"--leader-socket", "elsewhere"}, {"--worktree", "--leader"}} {
		_, err := InteractivePlan(args, nil)
		check(t, err != nil && strings.Contains(err.Error(), "grok-peer selects Grok's own leader"), "caller leader selection = %#v: %v", args, err)
	}
	for _, args := range [][]string{{"--single", "prompt"}, {"-pprompt"}, {"--prompt-file", "prompt.txt"}, {"--prompt-json", `[]`}, {"--output-format", "json"}, {"--json-schema", `{}`}, {"--max-turns", "1"}, {"--include-partial-messages"}, {"--print", "prompt"}, {"--print=prompt"}, {"--memory-flush"}, {"--leader", "--single", "x"}, {"--resume", "--single"}} {
		_, err := InteractivePlan(args, nil)
		check(t, err != nil && strings.Contains(err.Error(), "Sessionbus Grok lane"), "headless accepted: %#v", args)
	}
	// Native takes only the exact values off and none as no sandbox; nothing is trimmed or folded.
	for _, args := range [][]string{{"--sandbox", "workspace"}, {"--sandbox=workspace"}, {"--sandbox", "devbox"}, {"--sandbox", "read-only"}, {"--sandbox=readonly"}, {"--sandbox", "strict"}, {"--sandbox", "custom-profile"}, {"--sandbox", "Off"}, {"--sandbox=NONE"}, {"--sandbox", " off"}, {"--sandbox", "off", "--sandbox", "workspace"}, {"--model", "x", "--sandbox", "workspace"}} {
		_, err := InteractivePlan(args, nil)
		check(t, err != nil && strings.Contains(err.Error(), "without a leader"), "confining sandbox accepted: %#v: %v", args, err)
	}
	for _, args := range [][]string{{"sessions", "list"}, {"--version"}, {"plugin", "list", "--json"}} {
		plan, err := InteractivePlan(args, []string{ManagedEnv + "=inherited"})
		must(t, err)
		check(t, slices.Equal(plan.Args, args) && environment(plan.Env, ManagedEnv) == "", "native command wrapped: %#v", plan)
	}
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

// registeredGrokHome gives the test a Grok home whose config holds the helper's
// registration, as the installer leaves it.
func registeredGrokHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	must(t, os.WriteFile(filepath.Join(home, grokConfigFile), []byte("[mcp_servers.sessionbus]\ncommand = \"grok-peer-mcp\"\n"), 0o600))
	t.Setenv("GROK_HOME", home)
	return home
}

func TestManagedLaunchRequiresHelperRegistration(t *testing.T) {
	root := testsocket.Directory(t)
	recordPath := filepath.Join(root, "record")
	t.Setenv(host.SocketEnv, filepath.Join(root, "sessionbus.sock"))
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_INTERACTIVE_EXIT", "7")
	for name, config := range map[string]string{
		"no entry":       "model = \"grok\"\n",
		"another server": "[mcp_servers.other]\ncommand = \"other\"\n",
		"not a table":    "[mcp_servers]\nsessionbus = \"grok-peer-mcp\"\n",
		"no config file": "",
	} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			if config != "" {
				must(t, os.WriteFile(filepath.Join(home, grokConfigFile), []byte(config), 0o600))
			}
			t.Setenv("GROK_HOME", home)
			plan, err := InteractivePlan([]string{"--session-id", testSessionID, "--cwd", root}, os.Environ())
			must(t, err)
			err = RunInteractive(context.Background(), plan)
			check(t, err != nil && strings.Contains(err.Error(), "no mcp_servers.sessionbus entry") && strings.Contains(err.Error(), "grok-peer installer"), "unregistered launch = %v", err)
			check(t, len(records(t, recordPath)) == 0, "native started without the helper registration: %s", records(t, recordPath))
			launch, err := filepath.Glob(filepath.Join(root, "grok-launch-*"))
			must(t, err)
			check(t, len(launch) == 0, "refused launch left its directory: %v", launch)
		})
	}
	for name, config := range map[string]string{
		"table":        "[mcp_servers.sessionbus]\ncommand = \"grok-peer-mcp\"\n",
		"dotted keys":  "mcp_servers.sessionbus.command = \"grok-peer-mcp\"\n",
		"inline table": "mcp_servers = { sessionbus = { command = \"grok-peer-mcp\" } }\n",
	} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			must(t, os.WriteFile(filepath.Join(home, grokConfigFile), []byte(config), 0o600))
			t.Setenv("GROK_HOME", home)
			must(t, os.RemoveAll(recordPath))
			plan, err := InteractivePlan([]string{"--session-id", testSessionID, "--cwd", root}, os.Environ())
			must(t, err)
			var exited *exec.ExitError
			check(t, errors.As(RunInteractive(context.Background(), plan), &exited) && exited.ExitCode() == 7, "registered launch did not run the TUI")
			check(t, countStartsContaining(records(t, recordPath)) == 1, "registered launch starts = %s", records(t, recordPath))
		})
	}
}

func TestManagedLaunchRefusesExpandableLaunchPath(t *testing.T) {
	root := testsocket.Directory(t)
	recordPath := filepath.Join(root, "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_INTERACTIVE_EXIT", "7")
	registeredGrokHome(t)
	// The launch directory is created beside the bus socket; native would expand this name.
	beside := filepath.Join(root, "run$HOME")
	t.Setenv(host.SocketEnv, filepath.Join(beside, "sessionbus.sock"))
	plan, err := InteractivePlan([]string{"--session-id", testSessionID, "--cwd", root}, os.Environ())
	must(t, err)
	err = RunInteractive(context.Background(), plan)
	check(t, err != nil && strings.Contains(err.Error(), `contains "$"`), "expandable launch path = %v", err)
	check(t, len(records(t, recordPath)) == 0, "native started with an expandable launch path: %s", records(t, recordPath))
	entries, err := os.ReadDir(beside)
	must(t, err)
	check(t, len(entries) == 0, "refused launch left its directory: %v", entries)
}

func TestManagedLaunchRefusesNonUTF8LaunchPath(t *testing.T) {
	root := testsocket.Directory(t)
	recordPath := filepath.Join(root, "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_INTERACTIVE_EXIT", "7")
	registeredGrokHome(t)
	// The launch directory is created beside the bus socket; Grok cannot pass this name on.
	beside := filepath.Join(root, "run\xff")
	if err := os.Mkdir(beside, 0o700); err != nil {
		t.Skipf("filesystem rejects a name that is not UTF-8: %v", err)
	}
	t.Setenv(host.SocketEnv, filepath.Join(beside, "sessionbus.sock"))
	plan, err := InteractivePlan([]string{"--session-id", testSessionID, "--cwd", root}, os.Environ())
	must(t, err)
	err = RunInteractive(context.Background(), plan)
	check(t, err != nil && strings.Contains(err.Error(), "is not valid UTF-8"), "launch path that is not UTF-8 = %v", err)
	check(t, len(records(t, recordPath)) == 0, "native started with a launch path that is not UTF-8: %s", records(t, recordPath))
	entries, err := os.ReadDir(beside)
	must(t, err)
	check(t, len(entries) == 0, "refused launch left its directory: %v", entries)
}

// Only the launch directory travels through Grok. The facts file is read as bytes, so a
// working directory, name or socket that is not UTF-8 reaches the helper as written.
func TestLaunchFactsReachTheHelperByteForByte(t *testing.T) {
	root := testsocket.Directory(t)
	socket, work, name := filepath.Join(root, "bus\xfe"), filepath.Join(root, "work\xff"), "name\xfd $HOME"
	launch, err := os.MkdirTemp(root, "grok-launch-")
	must(t, err)
	must(t, writeLaunchFacts(launch, socket, `["team\u00e9"]`, name, work))
	_, facts, managed := managedLaunch(managedPeerEnv(nil, testSessionID, launch))
	check(t, managed && facts.Socket == socket && facts.Cwd == work && facts.Name == name && slices.Equal(facts.Groups, []string{"team\u00e9"}), "facts read by the helper = %q managed=%t", facts, managed)

	// The helper starts its observer in that directory and reaches the bus at that
	// socket. No name is requested here: native's JSON cannot return such a title.
	t.Run("observer in that directory", func(t *testing.T) {
		if err := os.Mkdir(work, 0o700); err != nil {
			t.Skipf("filesystem rejects a name that is not UTF-8: %v", err)
		}
		must(t, writeLaunchFacts(launch, socket, "[]", "", work))
		server, hellos := fakeDaemon(t, socket)
		defer server.Close()
		t.Setenv(host.SocketEnv, "")
		t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
		t.Setenv("GROK_TEST_CWD", root)
		backend, err := NewPeerBackend(context.Background(), managedPeerEnv(os.Environ(), testSessionID, launch))
		must(t, err)
		defer backend.Shutdown()
		backend.mu.Lock()
		observerDirectory := backend.cwd
		backend.mu.Unlock()
		check(t, observerDirectory == work, "observer directory = %q", observerDirectory)
		backend.Initialized()
		admitted := awaitHello(t, hellos)
		check(t, admitted.SessionID == testSessionID, "published identity = %+v", admitted)
		admitted.ack <- true
		awaitChannel(t, backend.ready, "Grok peer admission")
	})
}

func TestRelativeBusSocketIsAbsoluteInLaunchFacts(t *testing.T) {
	root := testsocket.Directory(t)
	recordPath := filepath.Join(root, "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	registeredGrokHome(t)
	tuiPID, release := filepath.Join(root, "tui.pid"), filepath.Join(root, "release")
	t.Setenv("GROK_TEST_INTERACTIVE_PID", tuiPID)
	t.Setenv("GROK_TEST_INTERACTIVE_EXIT", "7")
	t.Setenv("GROK_TEST_INTERACTIVE_EXIT_BARRIER", release)
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o600) })
	// The launcher resolves the relative socket where it runs.
	t.Chdir(root)
	t.Setenv(host.SocketEnv, filepath.Join("relative", "bus"))
	absolute := filepath.Join(root, "relative", "bus")
	plan, err := InteractivePlan([]string{"--session-id", testSessionID}, os.Environ())
	must(t, err)
	done := make(chan error, 1)
	go func() { done <- RunInteractive(context.Background(), plan) }()
	tuiPidfd := interactivePidfd(t, tuiPID)
	defer closeProcessHandle(tuiPidfd)
	var start struct {
		Kind  string `json:"kind"`
		Value struct {
			Environment map[string]string `json:"environment"`
		} `json:"value"`
	}
	must(t, json.Unmarshal(records(t, recordPath)[0], &start))
	marker := start.Value.Environment[ManagedEnv]
	info, err := os.Lstat(marker)
	check(t, start.Kind == "START" && filepath.IsAbs(marker) && err == nil && info.IsDir() && filepath.Dir(marker) == filepath.Dir(absolute), "TUI managed marker = %q (%v)", marker, err)
	// The helper runs under Grok's leader, in another directory and with the empty socket
	// its registration gives it. It reads the launch's absolute socket from the facts file
	// and applies it to its own environment, where the kit resolves it.
	t.Chdir(t.TempDir())
	t.Setenv(host.SocketEnv, "")
	env := managedPeerEnv(os.Environ(), testSessionID, marker)
	_, facts, managed := managedLaunch(env)
	check(t, managed && facts.Socket == absolute && facts.Cwd == root, "facts of a launch with a relative bus socket = %+v managed=%t", facts, managed)
	server, hellos := fakeDaemon(t, absolute)
	defer server.Close()
	backend, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	check(t, os.Getenv(host.SocketEnv) == absolute && sessionkit.Socket() == absolute, "helper bus socket = %q, kit resolves %q", os.Getenv(host.SocketEnv), sessionkit.Socket())
	backend.Initialized()
	admitted := awaitHello(t, hellos)
	check(t, admitted.SessionID == testSessionID, "published identity = %+v", admitted)
	admitted.ack <- true
	awaitChannel(t, backend.ready, "Grok peer admission")
	backend.Shutdown()
	must(t, os.WriteFile(release, nil, 0o600))
	var exited *exec.ExitError
	check(t, errors.As(<-done, &exited) && exited.ExitCode() == 7, "launcher result was not the TUI's")
	check(t, !exists(marker), "launch directory survived the launcher")
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
	env := managedPeerEnv(os.Environ(), testSessionID, liveLaunch(t, launchFacts{Socket: socket, Groups: []string{"peer-group"}}))
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
	env := managedPeerEnv(os.Environ(), testSessionID, liveLaunch(t, launchFacts{Socket: socket, Groups: []string{"peer-group"}}))
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
	backend, err := NewPeerBackend(context.Background(), managedPeerEnv(os.Environ(), testSessionID, liveLaunch(t, launchFacts{Socket: socket})))
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

// liveLaunch writes a launch directory as the launcher does: private, beside the bus
// socket, holding the facts file. The working directory defaults to the socket's.
func liveLaunch(t *testing.T, facts launchFacts) string {
	t.Helper()
	launch, err := os.MkdirTemp(filepath.Dir(facts.Socket), "grok-launch-")
	must(t, err)
	if facts.Groups == nil {
		facts.Groups = []string{}
	}
	if facts.Cwd == "" {
		facts.Cwd = filepath.Dir(facts.Socket)
	}
	groups, err := json.Marshal(facts.Groups)
	must(t, err)
	must(t, writeLaunchFacts(launch, facts.Socket, string(groups), facts.Name, facts.Cwd))
	return launch
}

// managedPeerEnv is a helper's environment as native starts it for a managed client:
// the registration's interpolated launch directory and the native session id.
func managedPeerEnv(env []string, id, launch string) []string {
	env = setEnvironment(env, grokSessionIDEnv, id)
	return setEnvironment(env, ManagedEnv, launch)
}

func TestProductSessionIDsCreateDistinctPeers(t *testing.T) {
	for index, id := range []string{testSessionID, "01a07800-94fb-7b12-b531-2f0509e033f1"} {
		root := testsocket.Directory(t)
		socket := filepath.Join(root, "sessionbus.sock")
		server, hellos := fakeDaemon(t, socket)
		t.Setenv(host.SocketEnv, socket)
		t.Setenv("GROK_TEST_SESSION_ID", id)
		environment := managedPeerEnv(os.Environ(), id, liveLaunch(t, launchFacts{Socket: socket}))
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
	environment := managedPeerEnv(os.Environ(), testSessionID, liveLaunch(t, launchFacts{Socket: socket}))
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
	environment := managedPeerEnv(os.Environ(), testSessionID, liveLaunch(t, launchFacts{Socket: socket}))
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
	environment := managedPeerEnv(os.Environ(), testSessionID, liveLaunch(t, launchFacts{Socket: socket}))
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
	check(t, countStartsContaining(records(t, recordPath), "--no-auto-update") == 0, "the peer observer suppressed native auto-update")
	check(t, countFrames(records(t, recordPath), "_x.ai/interject") == 2, "serialized deliveries lost an interject")
	backend.Shutdown()
}

func TestPeerHelperRequiresProductIdentity(t *testing.T) {
	_, err := NewPeerBackend(context.Background(), nil)
	check(t, err != nil && strings.Contains(err.Error(), "start Grok with grok-peer"), "missing identity = %v", err)
}

func TestHelperTakesLaunchFactsFromTheFileNotItsEnvironment(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	recordPath := filepath.Join(root, "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	// The registration hands the helper empty values. Whatever these names hold instead,
	// inherited from the leader or expanded by native, is not the launch's.
	t.Setenv(host.SocketEnv, filepath.Join(root, "another-bus"))
	t.Setenv(host.GroupsEnv, `["environment-group"]`)
	t.Setenv(host.NameEnv, "environment-name")
	// The published directory stays native's roster row, not the launch's.
	t.Setenv("GROK_TEST_CWD", root)
	work := filepath.Join(root, "work")
	must(t, os.Mkdir(work, 0o700))
	facts := launchFacts{Socket: socket, Groups: []string{"team$HOME", "${SESSIONBUS_GROUPS}"}, Name: "name$HOME ${SESSIONBUS_SESSION_NAME}", Cwd: work}
	env := managedPeerEnv(os.Environ(), testSessionID, liveLaunch(t, facts))
	// Grok's own leader names its socket as it likes; the helper no longer compares it.
	env = setEnvironment(env, grokLeaderSocketEnv, filepath.Join(root, "another-leader.sock"))
	backend, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	defer backend.Shutdown()
	backend.Initialized()
	admitted := awaitHello(t, hellos)
	check(t, admitted.SessionID == testSessionID && admitted.Name == facts.Name && slices.Equal(admitted.Groups, facts.Groups), "published identity = %+v", admitted)
	check(t, admitted.Info["cwd"] == root, "published directory = %v", admitted.Info)
	admitted.ack <- true
	awaitChannel(t, backend.ready, "Grok peer admission")
	frames := records(t, recordPath)
	var start struct {
		Kind  string `json:"kind"`
		Value struct {
			Arguments   []string          `json:"arguments"`
			Cwd         string            `json:"cwd"`
			Environment map[string]string `json:"environment"`
		} `json:"value"`
	}
	must(t, json.Unmarshal(frames[0], &start))
	check(t, start.Kind == "START" && slices.Equal(start.Value.Arguments, []string{"agent", "--leader", "stdio"}), "observer command = %s", frames[0])
	check(t, start.Value.Cwd == work, "observer directory = %q, want the launch's %q", start.Value.Cwd, work)
	check(t, start.Value.Environment[host.SocketEnv] == "" && start.Value.Environment[host.GroupsEnv] == "" && start.Value.Environment[ManagedEnv] == "", "observer inherited Sessionbus names: %v", start.Value.Environment)
	var rename struct {
		Value struct {
			Params struct {
				Title string `json:"title"`
			} `json:"params"`
		} `json:"value"`
	}
	must(t, json.Unmarshal(findFrame(frames, "_x.ai/session/rename"), &rename))
	check(t, rename.Value.Params.Title == facts.Name, "requested native title = %q", rename.Value.Params.Title)
}

func TestHelperDoesNotPublishWithoutItsLaunchWorkingDirectory(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	recordPath := filepath.Join(root, "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	gone := filepath.Join(root, "gone")
	env := managedPeerEnv(os.Environ(), testSessionID, liveLaunch(t, launchFacts{Socket: socket, Cwd: gone}))
	backend, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	defer backend.Shutdown()
	backend.Initialized()
	awaitChannel(t, backend.done, "Grok helper end")
	backend.mu.Lock()
	terminal := backend.err
	backend.mu.Unlock()
	check(t, errors.Is(terminal, os.ErrNotExist) && strings.Contains(terminal.Error(), "launch working directory "+gone), "helper result without its working directory = %v", terminal)
	// No fallback directory: the observer never ran, so nothing was published.
	check(t, len(records(t, recordPath)) == 0, "observer started elsewhere: %s", records(t, recordPath))
	select {
	case published := <-hellos:
		t.Fatalf("helper published without an observer: %+v", published)
	default:
	}
}

func TestInteractiveLauncherStartsOnlyTheTUIOnGroksLeader(t *testing.T) {
	root := testsocket.Directory(t)
	recordPath := filepath.Join(root, "record")
	socket := filepath.Join(root, "sessionbus.sock")
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	registeredGrokHome(t)
	started, tuiPID := filepath.Join(root, "interactive-started"), filepath.Join(root, "tui.pid")
	t.Setenv("GROK_TEST_INTERACTIVE_STARTED", started)
	t.Setenv("GROK_TEST_INTERACTIVE_PID", tuiPID)
	// What the launch directory holds when the launcher builds a native command,
	// which is before that command starts.
	type launchState struct {
		directory       string
		mode, factsMode os.FileMode
		facts           []byte
	}
	var mu sync.Mutex
	var built []launchState
	var native []*exec.Cmd
	previous := command
	command = func(name string, arguments ...string) *exec.Cmd {
		var state launchState
		if found, _ := filepath.Glob(filepath.Join(root, "grok-launch-*")); len(found) == 1 {
			state.directory = found[0]
			if info, err := os.Lstat(state.directory); err == nil {
				state.mode = info.Mode()
			}
			if info, err := os.Lstat(filepath.Join(state.directory, launchFactsFile)); err == nil {
				state.factsMode = info.Mode()
			}
			state.facts, _ = os.ReadFile(filepath.Join(state.directory, launchFactsFile))
		}
		cmd := previous(name, arguments...)
		mu.Lock()
		built = append(built, state)
		native = append(native, cmd)
		mu.Unlock()
		return cmd
	}
	t.Cleanup(func() { command = previous })
	forwarded := []string{"--session-id", testSessionID, "--cwd", root, "--model", "--yolo", "--allow", "Bash(pwd)", "--deny=MCPTool(sessionbus__sessionbus)", "--sandbox", "off"}
	plan, err := InteractivePlan(append([]string{"--group", "team,review$HOME", "-n", "initial ${SESSIONBUS_SESSION_NAME}"}, forwarded...), os.Environ())
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
	mu.Lock()
	states := slices.Clone(built)
	mu.Unlock()
	check(t, len(states) == 1 && states[0].directory != "", "launcher built %d native commands: %+v", len(states), states)
	state := states[0]
	check(t, state.mode.IsDir() && state.mode.Perm() == 0o700 && state.factsMode.IsRegular() && state.factsMode.Perm() == 0o600, "launch state is not private: directory %v, facts %v", state.mode, state.factsMode)
	// Four fields in a fixed order, one NUL between them, each as it is.
	check(t, string(state.facts) == socket+"\x00"+`["team","review$HOME"]`+"\x00"+"initial ${SESSIONBUS_SESSION_NAME}"+"\x00"+root, "facts written before native start = %q", state.facts)
	frames := records(t, recordPath)
	check(t, len(frames) == 1, "launcher started more than the TUI: %s", frames)
	var start struct {
		Kind  string `json:"kind"`
		Value struct {
			Arguments   []string          `json:"arguments"`
			Environment map[string]string `json:"environment"`
		} `json:"value"`
	}
	must(t, json.Unmarshal(frames[0], &start))
	// One process, grok --leader and the caller's arguments: a required value such as
	// the model's stays a value, and caller rules and a non-confining sandbox pass on.
	check(t, start.Kind == "START" && slices.Equal(start.Value.Arguments, append([]string{"--leader"}, forwarded...)), "TUI command = %q", start.Value.Arguments)
	check(t, start.Value.Environment[ManagedEnv] == state.directory && filepath.IsAbs(state.directory) && filepath.Dir(state.directory) == root, "TUI managed marker = %q, launch directory %q", start.Value.Environment[ManagedEnv], state.directory)
	check(t, start.Value.Environment[host.SocketEnv] == socket && start.Value.Environment[host.GroupsEnv] == "", "TUI Sessionbus environment = %v", start.Value.Environment)
	tuiPidfd := interactivePidfd(t, tuiPID)
	defer closeProcessHandle(tuiPidfd)
	cancel(testSignal{syscall.SIGINT})
	var exited *exec.ExitError
	<-done
	err = runErr
	check(t, errors.As(err, &exited) && exited.ProcessState.Sys().(syscall.WaitStatus).Signal() == syscall.SIGINT, "signalled Grok child = %v", err)
	check(t, !processRunning(t, tuiPidfd), "TUI survived the launcher")
	check(t, !exists(state.directory), "launch directory survived the launcher")
	check(t, len(records(t, recordPath)) == 1, "launcher teardown started native: %s", records(t, recordPath))
	// A leader this TUI starts inherits its environment: the launch's groups and name
	// travel in the facts file only, while the marker and the bus socket stay.
	mu.Lock()
	tuiEnv := native[0].Env
	mu.Unlock()
	check(t, !slices.ContainsFunc(tuiEnv, func(entry string) bool {
		return strings.HasPrefix(entry, host.GroupsEnv+"=") || strings.HasPrefix(entry, host.NameEnv+"=")
	}), "TUI environment holds the launch's groups or name")
	check(t, environment(tuiEnv, ManagedEnv) == state.directory && environment(tuiEnv, host.SocketEnv) == socket, "TUI environment lost its marker or bus socket")
}

func TestInteractiveLauncherRemovesItsDirectoryWhenTheTUIFailsToStart(t *testing.T) {
	root := testsocket.Directory(t)
	t.Setenv(host.SocketEnv, filepath.Join(root, "sessionbus.sock"))
	registeredGrokHome(t)
	// The launch directory holds its facts when the launcher builds native's command.
	var prepared []string
	previous := command
	command = func(string, ...string) *exec.Cmd {
		prepared, _ = filepath.Glob(filepath.Join(root, "grok-launch-*", launchFactsFile))
		return exec.Command(filepath.Join(root, "absent-grok"))
	}
	t.Cleanup(func() { command = previous })
	plan, err := InteractivePlan([]string{"--session-id", testSessionID, "--cwd", root}, os.Environ())
	must(t, err)
	err = RunInteractive(context.Background(), plan)
	check(t, errors.Is(err, os.ErrNotExist), "TUI start failure = %v", err)
	check(t, len(prepared) == 1, "launch state was not prepared before the TUI start: %v", prepared)
	left, err := filepath.Glob(filepath.Join(root, "grok-launch-*"))
	must(t, err)
	check(t, len(left) == 0, "failed TUI start left its launch directory: %v", left)
}

func TestInteractiveLauncherReturnsProductExit(t *testing.T) {
	root := testsocket.Directory(t)
	recordPath := filepath.Join(root, "record")
	t.Setenv(host.SocketEnv, filepath.Join(root, "sessionbus.sock"))
	t.Setenv("GROK_TEST_RECORD", recordPath)
	registeredGrokHome(t)
	interactivePID, release := filepath.Join(root, "interactive.pid"), filepath.Join(root, "release")
	t.Setenv("GROK_TEST_INTERACTIVE_PID", interactivePID)
	t.Setenv("GROK_TEST_INTERACTIVE_EXIT", "7")
	t.Setenv("GROK_TEST_INTERACTIVE_EXIT_BARRIER", release)
	plan, err := InteractivePlan([]string{"--session-id", testSessionID, "--cwd", root}, os.Environ())
	must(t, err)
	done := make(chan error, 1)
	go func() { done <- RunInteractive(context.Background(), plan) }()
	tuiPidfd := interactivePidfd(t, interactivePID)
	defer closeProcessHandle(tuiPidfd)
	launch, err := filepath.Glob(filepath.Join(root, "grok-launch-*"))
	must(t, err)
	check(t, len(launch) == 1 && exists(filepath.Join(launch[0], launchFactsFile)) && processRunning(t, tuiPidfd), "interactive launch was not live: %v", launch)
	must(t, os.WriteFile(release, nil, 0o600))
	err = <-done
	var exited *exec.ExitError
	check(t, errors.As(err, &exited) && exited.ExitCode() == 7, "exit = %v", err)
	check(t, !exists(launch[0]), "launch state survived the TUI")
	check(t, len(records(t, recordPath)) == 1, "launcher started more than the TUI: %s", records(t, recordPath))
}

func TestStopAuxSkipsReapedProcessGroup(t *testing.T) {
	var killed []int
	previous := killProcess
	killProcess = func(pid int, signal syscall.Signal) error { killed = append(killed, pid); return previous(pid, signal) }
	t.Cleanup(func() { killProcess = previous })
	gone := exec.Command("true")
	gone.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	exitedProcess, err := startNative(gone)
	must(t, err)
	<-exitedProcess.done
	// A waited process's group number may already belong to someone else.
	stopAux(exitedProcess)
	check(t, len(killed) == 0, "teardown signalled a reaped process group: %v", killed)
	live := exec.Command("sleep", "30")
	live.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	liveProcess, err := startNative(live)
	must(t, err)
	stopAux(liveProcess)
	check(t, slices.Equal(killed, []int{-liveProcess.cmd.Process.Pid}), "owned live group not killed: %v", killed)
	select {
	case <-liveProcess.done:
	case <-time.After(5 * time.Second):
		t.Fatal("owned live group survived teardown")
	}
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
	environment := managedPeerEnv(os.Environ(), testSessionID, liveLaunch(t, launchFacts{Socket: socket}))
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
	backend.mu.Lock()
	role := backend.observer.role
	backend.mu.Unlock()
	check(t, role == acpObserver, "interactive observer may answer shared native interactions")
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
	return pidfd(t, interactivePID(t, path))
}

func interactivePID(t *testing.T, path string) int {
	t.Helper()
	<-fileReady(path)
	body, err := os.ReadFile(path)
	must(t, err)
	var pid int
	_, err = fmt.Sscan(string(body), &pid)
	must(t, err)
	return pid
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
	registeredGrokHome(t)
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
				Environment map[string]string `json:"environment"`
			} `json:"value"`
		}
		must(t, json.Unmarshal(raw, &record))
		if record.Kind != "START" {
			continue
		}
		paths = append(paths, record.Value.Environment[ManagedEnv])
	}
	check(t, len(paths) == 2 && paths[0] != paths[1] && filepath.IsAbs(paths[0]) && filepath.IsAbs(paths[1]), "same native ID reused launch identity: %#v", paths)
	for _, path := range paths {
		check(t, !exists(path), "launch directory remains: %s", path)
	}
}
