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

func TestManagedHelperRequiresMatchingNativeLeader(t *testing.T) {
	for _, env := range [][]string{nil, {"SESSIONBUS_GROK_MANAGED=private", "GROK_SESSION_ID=native"}, {"SESSIONBUS_GROK_MANAGED=private", "GROK_LEADER_SOCKET=ordinary", "GROK_SESSION_ID=native"}} {
		if grok.ManagedHelper(env) {
			t.Fatalf("foreign/inherited activation: %v", env)
		}
	}
	if !grok.ManagedHelper([]string{"SESSIONBUS_GROK_MANAGED=private", "GROK_LEADER_SOCKET=private", "GROK_SESSION_ID=native"}) {
		t.Fatal("matching native helper inactive")
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

// managedEntry runs the real runMCP as a managed helper whose launch marker is
// launch/grok-native.sock, with stdin and stdout on pipes. No real Grok can start:
// PATH is empty, HOME and GROK_HOME are private, and the test stops unless grok is
// unresolvable before anything runs.
func managedEntry(t *testing.T, launch, bus string) (stdin io.WriteCloser, stdout *bufio.Scanner, done <-chan error) {
	t.Helper()
	empty := t.TempDir()
	t.Setenv("PATH", empty)
	t.Setenv("HOME", empty)
	t.Setenv("GROK_HOME", empty)
	if path, err := exec.LookPath("grok"); err == nil {
		t.Fatalf("grok resolves to %s; refusing to run", path)
	}
	marker := filepath.Join(launch, "grok-native.sock")
	t.Setenv(mcp.LaneSocketEnv, "")
	t.Setenv(grok.ManagedEnv, marker)
	t.Setenv("GROK_LEADER_SOCKET", marker)
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

func TestManagedEntryEndsWithItsLaunchDirectoryNotItsSocket(t *testing.T) {
	launch := filepath.Join(t.TempDir(), "grok-launch-1")
	if err := os.Mkdir(launch, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(launch, "grok-native.sock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, stdout, done := managedEntry(t, launch, filepath.Join(t.TempDir(), "bus"))
	go func() {
		for stdout.Scan() {
		}
	}()
	// A native leader turnover can remove the socket; the launch and the helper continue.
	if err := os.Remove(filepath.Join(launch, "grok-native.sock")); err != nil {
		t.Fatal(err)
	}
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
	stdin, stdout, done := managedEntry(t, filepath.Join(t.TempDir(), "grok-launch-gone"), bus)
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
