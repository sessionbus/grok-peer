// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"encoding/json"
	kit "github.com/antst/sessionbus/bus/sdk/go"
	"io"
	"sync"
	"testing"

	"github.com/sessionbus/grok-peer/wrappers/grok"
	"github.com/sessionbus/peer-common/host"
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
