// SPDX-License-Identifier: MIT
package grok

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/testsocket"
)

type grokSeedProduct struct {
	*Wrapper
	entered, release chan struct{}
	returned         chan error
}

func (p *grokSeedProduct) Open(context.Context, kit.OpenRequest) (kit.OpenResult, error) {
	return kit.OpenResult{SessionID: p.sessionID}, nil
}
func (p *grokSeedProduct) Close(context.Context, kit.SessionCloseRequest) error {
	p.primary.close()
	return nil
}
func (p *grokSeedProduct) Run(ctx context.Context, run *kit.Run, seed kit.RunInput) (kit.TurnResult, error) {
	report := run.ReportDelivery
	if p.entered != nil {
		report = func(r kit.DeliveryReceipt, e error) error {
			close(p.entered)
			select {
			case <-p.release:
			case <-ctx.Done():
				return ctx.Err()
			}
			return run.ReportDelivery(r, e)
		}
	}
	result, err := p.executeRun(ctx, run, seed, report)
	p.returned <- err
	return result, err
}
func TestGrokWorkerSeedAdmissionReceiptAndCursor(t *testing.T) {
	for _, mode := range []string{"direct", "terminal-before-receipt", "bus-loss-at-receipt", "wrong-terminal", "native-error", "missing-admission", "replayed-admission"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("unix", filepath.Join(testsocket.Directory(t), "bus.sock"))
			must(t, err)
			defer listener.Close()
			t.Setenv("SESSIONBUS_SOCKET", listener.Addr().String())
			t.Setenv("SESSIONBUS_LAUNCH_TOKEN", "seed")
			t.Setenv("SESSIONBUS_LOCAL_KEY", "")
			requestRead, requestWrite := io.Pipe()
			responseRead, responseWrite := io.Pipe()
			defer requestRead.Close()
			defer responseWrite.Close()
			base := &Wrapper{sessionID: "native-session"}
			base.primary = newACPClient(requestWrite, responseRead, base.receive)
			defer base.primary.close()
			p := &grokSeedProduct{Wrapper: base, returned: make(chan error, 1)}
			if mode == "terminal-before-receipt" || mode == "bus-loss-at-receipt" {
				p.entered = make(chan struct{})
				p.release = make(chan struct{})
			}
			worker := kit.NewWorker(p)
			p.SetCaller(worker.Caller())
			served := make(chan error, 1)
			go func() { served <- worker.Serve(context.Background()) }()
			bus, err := listener.Accept()
			must(t, err)
			defer bus.Close()
			defer func() { _ = bus.Close(); <-served }()
			reader := bufio.NewReader(bus)
			next := func() protocol.Frame {
				t.Helper()
				line, e := reader.ReadBytes('\n')
				must(t, e)
				f, e := protocol.DecodeFrame(line[:len(line)-1])
				must(t, e)
				return f
			}
			send := func(body []byte, e error) { t.Helper(); must(t, e); _, e = bus.Write(body); must(t, e) }
			hello := next()
			var h kit.HelloDescription
			must(t, json.Unmarshal(hello.Params, &h))
			check(t, h.SupportsMessageRun, "missing wake capability")
			send(protocol.ResultBytes(hello.ID, "session.hello", struct{}{}))
			send(protocol.RequestBytes(1, "session.open", kit.OpenRequest{Name: "seed@local", Groups: []string{}, Policy: &kit.LanePolicy{IdleMessage: "run"}}))
			check(t, next().Error == nil, "open failed")
			d := delivery("owned-wake-marker")
			d.RunID = "g/1"
			send(protocol.RequestBytes(2, "message.deliver", d))
			nativeReader, nativeWriter := json.NewDecoder(requestRead), json.NewEncoder(responseWrite)
			prompt := readACP(t, nativeReader)
			check(t, prompt.Method == "session/prompt", "seed was not one owned prompt")
			var params struct {
				SessionID string
				Prompt    []struct{ Text string }
			}
			must(t, json.Unmarshal(prompt.Params, &params))
			check(t, len(params.Prompt) == 1 && strings.Contains(params.Prompt[0].Text, "owned-wake-marker"), "seed input missing")
			if mode == "native-error" || mode == "missing-admission" || mode == "replayed-admission" {
				if mode == "replayed-admission" {
					must(t, nativeWriter.Encode(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/queue/changed", "params": map[string]any{"sessionId": params.SessionID, "runningPromptId": "prompt-1", "runningText": params.Prompt[0].Text, "runningKind": "prompt", "_meta": map[string]bool{"isReplay": true}}}))
				}
				if mode == "native-error" {
					must(t, nativeWriter.Encode(map[string]any{"jsonrpc": "2.0", "id": prompt.ID, "error": map[string]any{"code": -32603, "message": "native refused"}}))
				} else {
					replyACP(t, nativeWriter, prompt, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "prompt-1"}})
				}
				refused := next()
				check(t, refused.ID == 2, "wrong refused receipt")
				ready := next()
				check(t, ready.Method == "turn.ready", "missing unavailable record")
				send(protocol.ResultBytes(ready.ID, "turn.ready", struct{}{}))
				// No EOF or manual release: its own RPC completion settled the seed.
				select {
				case <-base.primary.done:
					t.Fatal("healthy primary was closed")
				default:
				}
				return
			}
			// The live primary running event establishes admission, not the write return.
			must(t, nativeWriter.Encode(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/queue/changed", "params": map[string]any{"sessionId": params.SessionID, "runningPromptId": "prompt-1", "runningText": params.Prompt[0].Text, "runningKind": "prompt", "entries": []any{}}}))
			if p.entered != nil {
				<-p.entered
			}
			must(t, nativeWriter.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": params.SessionID, "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"text": "wake-answer"}}, "_meta": map[string]string{"promptId": "prompt-1"}}}))
			must(t, nativeWriter.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": params.SessionID, "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"text": "REPLAY"}}, "_meta": map[string]any{"promptId": "prompt-1", "isReplay": true}}}))
			terminalID := "prompt-1"
			if mode == "wrong-terminal" {
				terminalID = "foreign"
			}
			must(t, nativeWriter.Encode(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/session_notification", "params": map[string]any{"sessionId": params.SessionID, "update": map[string]any{"sessionUpdate": "turn_completed", "prompt_id": "prompt-1", "stop_reason": "end_turn"}}}))
			replyACP(t, nativeWriter, prompt, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": terminalID}})
			// A same-primary response barrier proves terminal parsing progresses while
			// ReportDelivery is held, independently of callback scheduling.
			barrier := make(chan error, 1)
			go func() { barrier <- base.primary.request(context.Background(), "barrier", nil, nil) }()
			replyACP(t, nativeWriter, readACP(t, nativeReader), map[string]any{})
			must(t, <-barrier)
			if mode == "bus-loss-at-receipt" {
				_ = bus.Close()
				if e := <-p.returned; e == nil {
					t.Fatal("lost receipt succeeded")
				}
				return
			}
			if p.release != nil {
				close(p.release)
			}
			receipt := next()
			var r kit.DeliveryReceipt
			must(t, protocol.UnmarshalResult("message.deliver", receipt.Result, &r))
			check(t, receipt.ID == 2 && r.Disposition == "injected", "missing native admitted receipt")
			ready := next()
			check(t, ready.Method == "turn.ready", "missing ready")
			send(protocol.ResultBytes(ready.ID, "turn.ready", struct{}{}))
			for _, id := range []int64{3, 4} {
				send(protocol.RequestBytes(id, "turn.status", kit.ReadRequest{SessionID: "native-session@local", RunID: "g/1"}))
				f := next()
				var status kit.RunStatus
				must(t, protocol.UnmarshalResult("turn.status", f.Result, &status))
				if mode == "wrong-terminal" {
					check(t, status.State == "unavailable" && status.Result == nil, "foreign terminal accepted")
				} else {
					check(t, status.State == "done" && status.Result != nil && status.Result.Result == "wake-answer", "nonconsuming result missing")
				}
			}
			send(protocol.RequestBytes(5, "turn.ack", kit.RunRef{SessionID: "native-session@local", RunID: "g/1"}))
			check(t, next().Error == nil, "ack failed")
		})
	}
}
