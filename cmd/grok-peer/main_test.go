// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	kit "github.com/antst/sessionbus/bus/sdk/go"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sessionbus/grok-peer/wrappers/grok"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/mcp"
	"github.com/sessionbus/peer-common/testsocket"
)

func TestLaneModeRejectsArguments(t *testing.T) {
	t.Setenv(host.TokenEnv, "token")
	if err := run(context.Background(), []string{"mcp"}); err == nil || err.Error() != "lane mode accepts no arguments" {
		t.Fatalf("run = %v", err)
	}
}

// launchDirectory writes a managed launch's directory as the launcher does: private,
// holding the facts file with its four fields.
func launchDirectory(t *testing.T, path, bus string) {
	t.Helper()
	launchDirectoryWith(t, path, map[string]any{"socket": bus, "groups": []string{}, "name": "", "cwd": filepath.Dir(path)})
}

func launchDirectoryWith(t *testing.T, path string, fields map[string]any) {
	t.Helper()
	facts, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "launch.json"), facts, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestManagedHelperRequiresLiveLaunchDirectoryWithFacts(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Relative names resolve here, each to a live launch directory holding facts: only
	// the absolute-path rule keeps them inert, as it does the reference a plain Grok
	// client hands on unexpanded.
	t.Chdir(root)
	live := filepath.Join(root, "grok-launch-live")
	for _, name := range []string{"grok-launch-live", "${SESSIONBUS_GROK_MANAGED}"} {
		launchDirectory(t, filepath.Join(root, name), filepath.Join(root, "bus"))
	}
	regular, bare, undecoded := filepath.Join(root, "regular"), filepath.Join(root, "grok-launch-bare"), filepath.Join(root, "grok-launch-undecoded")
	if err := os.WriteFile(regular, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{bare, undecoded} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(undecoded, "launch.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	inert := map[string][]string{
		"no environment":          nil,
		"empty marker":            {"SESSIONBUS_GROK_MANAGED=", "GROK_SESSION_ID=native"},
		"unexpanded reference":    {"SESSIONBUS_GROK_MANAGED=${SESSIONBUS_GROK_MANAGED}", "GROK_SESSION_ID=native"},
		"relative path":           {"SESSIONBUS_GROK_MANAGED=grok-launch-live", "GROK_SESSION_ID=native"},
		"regular file":            {"SESSIONBUS_GROK_MANAGED=" + regular, "GROK_SESSION_ID=native"},
		"missing directory":       {"SESSIONBUS_GROK_MANAGED=" + filepath.Join(root, "grok-launch-gone"), "GROK_SESSION_ID=native"},
		"directory without facts": {"SESSIONBUS_GROK_MANAGED=" + bare, "GROK_SESSION_ID=native"},
		"undecodable facts":       {"SESSIONBUS_GROK_MANAGED=" + undecoded, "GROK_SESSION_ID=native"},
		"no native session":       {"SESSIONBUS_GROK_MANAGED=" + live},
		"empty native session":    {"SESSIONBUS_GROK_MANAGED=" + live, "GROK_SESSION_ID="},
	}
	// Facts count only whole: the group list, and an absolute bus socket and working directory.
	bus := filepath.Join(root, "bus")
	for name, fields := range map[string]map[string]any{
		"facts without groups":         {"socket": bus, "name": "", "cwd": root},
		"facts with a relative cwd":    {"socket": bus, "groups": []string{}, "name": "", "cwd": "work"},
		"facts with an empty cwd":      {"socket": bus, "groups": []string{}, "name": "", "cwd": ""},
		"facts without cwd":            {"socket": bus, "groups": []string{}, "name": ""},
		"facts with a relative socket": {"socket": "bus", "groups": []string{}, "name": "", "cwd": root},
		"facts with an empty socket":   {"socket": "", "groups": []string{}, "name": "", "cwd": root},
		"facts without socket":         {"groups": []string{}, "name": "", "cwd": root},
	} {
		directory := filepath.Join(root, "grok-launch-"+strings.ReplaceAll(name, " ", "-"))
		launchDirectoryWith(t, directory, fields)
		inert[name] = []string{"SESSIONBUS_GROK_MANAGED=" + directory, "GROK_SESSION_ID=native"}
	}
	for name, env := range inert {
		if grok.ManagedHelper(env) {
			t.Errorf("%s activated the helper: %v", name, env)
		}
	}
	// Grok's own leader is not this launch's: its socket no longer decides anything.
	for name, env := range map[string][]string{
		"live launch":                {"SESSIONBUS_GROK_MANAGED=" + live, "GROK_SESSION_ID=native"},
		"live launch, other leader":  {"SESSIONBUS_GROK_MANAGED=" + live, "GROK_LEADER_SOCKET=ordinary", "GROK_SESSION_ID=native"},
		"live launch, empty sockets": {"SESSIONBUS_GROK_MANAGED=" + live, "GROK_LEADER_SOCKET=", "SESSIONBUS_SOCKET=", "GROK_SESSION_ID=native"},
	} {
		if !grok.ManagedHelper(env) {
			t.Errorf("%s left the helper inactive: %v", name, env)
		}
	}
}

func TestSharedMCPEOFSettlesActualCaller(t *testing.T) {
	input, writer := io.Pipe()
	var output bytes.Buffer
	entered, settled, ended := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	caller := kit.NewCaller(func(ctx context.Context, method string, args any) (json.RawMessage, error) {
		if method != "session.list" {
			t.Errorf("method=%s", method)
		}
		close(entered)
		<-ctx.Done()
		close(settled)
		return nil, ctx.Err()
	})
	owner := grokMCPOwner{action: caller.Action, end: func() { once.Do(func() { close(ended) }) }}
	done := make(chan error, 1)
	go func() { done <- serveMCP(context.Background(), owner, input, &output) }()
	_, err := io.WriteString(writer, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"sessionbus","arguments":{"action":"list","arguments":{}}}}`+"\n")
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	_ = writer.Close()
	<-settled
	<-ended
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 {
		t.Fatalf("late reply after EOF: %s", output.String())
	}
}

// managedEntry runs the real runMCP as a helper whose launch marker is the directory
// launch, with stdin and stdout on pipes. No real Grok can start: PATH is empty, HOME
// and GROK_HOME are private, and the test stops unless grok is unresolvable before
// anything runs.
func managedEntry(t *testing.T, launch, bus, lane string) (stdin io.WriteCloser, stdout *bufio.Scanner, done <-chan error) {
	t.Helper()
	empty := t.TempDir()
	t.Setenv("PATH", empty)
	t.Setenv("HOME", empty)
	t.Setenv("GROK_HOME", empty)
	if path, err := exec.LookPath("grok"); err == nil {
		t.Fatalf("grok resolves to %s; refusing to run", path)
	}
	t.Setenv(mcp.LaneSocketEnv, lane)
	t.Setenv(grok.ManagedEnv, launch)
	t.Setenv("GROK_SESSION_ID", "native")
	t.Setenv(host.SocketEnv, bus)
	inRead, inWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outRead, outWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdinBefore, stdoutBefore := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = inRead, outWrite
	result, returned := make(chan error, 1), make(chan struct{})
	go func() { result <- runMCP(context.Background()); close(returned) }()
	t.Cleanup(func() {
		_ = inWrite.Close()
		select {
		case <-returned:
		case <-time.After(10 * time.Second):
			t.Error("runMCP did not return at cleanup")
		}
		os.Stdin, os.Stdout = stdinBefore, stdoutBefore
		_ = outRead.Close()
	})
	return inWrite, bufio.NewScanner(outRead), result
}

func TestManagedEntryEndsWithItsLaunchDirectory(t *testing.T) {
	launch := filepath.Join(t.TempDir(), "grok-launch-1")
	launchDirectory(t, launch, filepath.Join(t.TempDir(), "bus"))
	// The registration hands the helper an empty bus socket; the facts file names it.
	_, stdout, done := managedEntry(t, launch, "", "")
	go func() {
		for stdout.Scan() {
		}
	}()
	select {
	case err := <-done:
		t.Fatalf("helper ended while its launch was live: %v", err)
	case <-time.After(5 * time.Second):
	}
	// The launcher quit and removed its launch directory: the helper's MCP frontend ends.
	removed := time.Now()
	if err := os.RemoveAll(launch); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Logf("helper returned %v after %s", err, time.Since(removed).Round(time.Millisecond))
	case <-time.After(10 * time.Second):
		t.Fatal("helper kept running after its launch ended")
	}
}

// The lane decision comes first: a helper given a lane socket forwards to it, also when
// its environment holds the marker of a live managed launch.
func TestLaneSocketSelectsTheForwarderOverALiveManagedLaunch(t *testing.T) {
	root := testsocket.Directory(t)
	lane, leader := filepath.Join(root, "lane"), filepath.Join(root, "leader.sock")
	listener, err := net.Listen("unix", lane)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	identity := make(chan string, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		// Closing without an acknowledgement ends the forwarder.
		defer connection.Close()
		line, _ := bufio.NewReader(connection).ReadString('\n')
		identity <- line
	}()
	// A managed backend would claim this launch's requested name as soon as it is built.
	launch := filepath.Join(t.TempDir(), "grok-launch-1")
	launchDirectoryWith(t, launch, map[string]any{"socket": filepath.Join(root, "bus"), "groups": []string{}, "name": "requested", "cwd": root})
	t.Setenv("GROK_LEADER_SOCKET", leader)
	_, _, done := managedEntry(t, launch, "", lane)
	select {
	case line := <-identity:
		var native struct {
			SessionID string `json:"session_id"`
			Leader    string `json:"leader"`
		}
		if err := json.Unmarshal([]byte(line), &native); err != nil || native.SessionID != "native" || native.Leader != leader {
			t.Fatalf("lane endpoint received %q (%v)", line, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("helper did not reach its lane endpoint")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("forwarder ended cleanly without an endpoint acknowledgement")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("forwarder kept running after its endpoint closed")
	}
	if _, err := os.Lstat(filepath.Join(launch, "initial-name.claim")); !os.IsNotExist(err) {
		t.Fatalf("a managed backend ran beside the lane forwarder: %v", err)
	}
}

func TestManagedEntryStartedAfterItsLaunchIsInert(t *testing.T) {
	// A short socket directory: a t.TempDir path exceeds the Unix socket path limit on macOS.
	bus := filepath.Join(testsocket.Directory(t), "bus")
	if len(bus) >= 100 {
		t.Fatalf("bus socket path is %d bytes, too long for a Unix socket", len(bus))
	}
	t.Logf("bus socket path is %d bytes", len(bus))
	listener, err := net.Listen("unix", bus)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var mu sync.Mutex
	accepted := 0
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			accepted++
			mu.Unlock()
			_ = connection.Close()
		}
	}()
	stdin, stdout, done := managedEntry(t, filepath.Join(t.TempDir(), "grok-launch-gone"), bus, "")
	enc := json.NewEncoder(stdin)
	for id, method := range []string{"initialize", "tools/list"} {
		params := map[string]any{}
		if method == "initialize" {
			params["protocolVersion"] = "2025-06-18"
		}
		if err := enc.Encode(map[string]any{"jsonrpc": "2.0", "id": id + 1, "method": method, "params": params}); err != nil {
			t.Fatal(err)
		}
		if !stdout.Scan() {
			t.Fatalf("no %s response", method)
		}
		var response struct {
			Result map[string]json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		switch method {
		case "initialize":
			if _, tools := response.Result["capabilities"]; !tools || string(response.Result["capabilities"]) != "{}" {
				t.Fatalf("an ended launch's helper offers tools: %s", stdout.Text())
			}
		case "tools/list":
			if string(response.Result["tools"]) != "[]" {
				t.Fatalf("an ended launch's helper lists tools: %s", stdout.Text())
			}
		}
	}
	time.Sleep(time.Second)
	mu.Lock()
	count := accepted
	mu.Unlock()
	if count != 0 {
		t.Fatalf("an ended launch's helper connected to Sessionbus %d times", count)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("inactive helper result: %v", err)
	}
}
