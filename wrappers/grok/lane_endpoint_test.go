// SPDX-License-Identifier: MIT
package grok

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"sync"
	"testing"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/testsocket"
)

func TestResidentForwarderEOFCancelsCallerWaitWithoutAck(t *testing.T) {
	p := New(filepath.Join(testsocket.Directory(t), "bus.sock"), "forward")
	p.sessionID = "native"
	entered, settled, lost := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	p.SetShutdown(func() { once.Do(func() { close(lost) }) })
	p.SetCall(func(ctx context.Context, method string, params any) (json.RawMessage, error) {
		switch method {
		case "turn.wait":
			close(entered)
			<-ctx.Done()
			close(settled)
			return nil, ctx.Err()
		case "turn.status":
			return json.Marshal(kit.RunStatus{SessionID: "child@local", RunID: "g/1", State: "done", Result: &kit.TurnResult{Outcome: "completed", Result: "retained", NativeStopReason: "end_turn"}})
		default:
			t.Errorf("unexpected consuming or native call %s", method)
			return nil, io.ErrUnexpectedEOF
		}
	})
	endpoint, err := newGrokEndpoint(p)
	must(t, err)
	p.endpoint = endpoint
	defer endpoint.Close()
	input, writer := io.Pipe()
	var output bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- forwardLane(context.Background(), endpoint.Path, nativeHelperIdentity{"native", leaderSocket(p.socket, p.key)}, input, &output)
	}()
	_, err = io.WriteString(writer, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"sessionbus","arguments":{"action":"wait","arguments":{"session_id":"child@local","run_id":"g/1"}}}}`+"\n")
	must(t, err)
	<-entered
	_ = writer.Close()
	<-settled
	<-lost
	must(t, <-done)
	check(t, output.Len() == 0, "reply emitted after native EOF")
	// Actual Caller path verifies EOF did not issue ack or consume a cursor.
	for i := 0; i < 2; i++ {
		status, e := p.caller.Status(context.Background(), kit.ReadRequest{SessionID: "child@local", RunID: "g/1"})
		must(t, e)
		check(t, status.Result != nil && status.Result.Result == "retained", "retained result lost")
	}
}
