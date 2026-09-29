// SPDX-License-Identifier: MIT
package grok

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

func acpPipe(t *testing.T) (*acpClient, *json.Decoder, *json.Encoder, io.Closer) {
	t.Helper()
	requestRead, requestWrite := io.Pipe()
	responseRead, responseWrite := io.Pipe()
	c := newACPClient(acpPrimary, requestWrite, responseRead, nil)
	t.Cleanup(func() { c.close(); _ = requestRead.Close(); _ = responseWrite.Close() })
	return c, json.NewDecoder(requestRead), json.NewEncoder(responseWrite), responseWrite
}
func readACP(t *testing.T, d *json.Decoder) acpFrame {
	t.Helper()
	var f acpFrame
	if err := d.Decode(&f); err != nil {
		t.Fatal(err)
	}
	return f
}
func replyACP(t *testing.T, e *json.Encoder, f acpFrame, value any) {
	t.Helper()
	if err := e.Encode(map[string]any{"jsonrpc": "2.0", "id": f.ID, "result": value}); err != nil {
		t.Fatal(err)
	}
}
func TestResponseBeforeEOFWins(t *testing.T) {
	c, d, e, end := acpPipe(t)
	done := make(chan error, 1)
	var result map[string]string
	go func() { done <- c.request(context.Background(), "example", nil, &result) }()
	f := readACP(t, d)
	replyACP(t, e, f, map[string]string{"value": "read"})
	_ = end.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if result["value"] != "read" {
		t.Fatal(result)
	}
}
func TestACPConcurrentCallsAndCancelledDrain(t *testing.T) {
	c, d, e, _ := acpPipe(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	started := make(chan error, 1)
	go func() { done <- c.requestStarted(ctx, "first", nil, nil, started) }()
	first := readACP(t, d)
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	cancel()
	if !errors.Is(<-done, context.Canceled) {
		t.Fatal("cancellation lost")
	}
	go func() { done <- c.request(context.Background(), "second", nil, nil) }()
	second := readACP(t, d)
	replyACP(t, e, second, map[string]bool{"ok": true})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	replyACP(t, e, first, map[string]bool{"late": true})
	go func() { done <- c.request(context.Background(), "barrier", nil, nil) }()
	barrier := readACP(t, d)
	replyACP(t, e, barrier, map[string]bool{})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) != 0 {
		t.Fatal(c.pending)
	}
}
func TestACPActorAcknowledgementOrdering(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(map[bool]string{false: "reply-first", true: "echo-first"}[early], func(t *testing.T) {
			c, d, e, _ := acpPipe(t)
			done := make(chan error, 1)
			go func() { done <- c.interject(context.Background(), "session", "message", "text") }()
			f := readACP(t, d)
			notice := func() {
				if err := e.Encode(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/session/interjection", "params": map[string]string{"sessionId": "session", "interjectionId": "message"}}); err != nil {
					t.Fatal(err)
				}
			}
			if early {
				notice()
			}
			replyACP(t, e, f, map[string]any{"result": map[string]string{"status": "queued"}})
			if !early {
				notice()
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestACPQueuedWithoutActorAckFailsOnEOF(t *testing.T) {
	c, d, e, end := acpPipe(t)
	done := make(chan error, 1)
	go func() { done <- c.interject(context.Background(), "session", "message", "text") }()
	replyACP(t, e, readACP(t, d), map[string]any{"result": map[string]string{"status": "queued"}})
	_ = end.Close()
	if err := <-done; err == nil {
		t.Fatal("queued reply was counted as admission")
	}
}
func TestACPBlockedWriteCancellationClosesTransport(t *testing.T) {
	requestRead, requestWrite := io.Pipe()
	responseRead, responseWrite := io.Pipe()
	c := newACPClient(acpPrimary, requestWrite, responseRead, nil)
	defer c.close()
	defer requestRead.Close()
	defer responseWrite.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.request(ctx, "blocked", nil, nil) }()
	if _, err := io.ReadFull(requestRead, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-c.done
}

func TestACPCancelledRequestsRemainBounded(t *testing.T) {
	c, d, e, _ := acpPipe(t)
	var first acpFrame
	for i := 0; i < maxACPPending; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan error, 1)
		done := make(chan error, 1)
		go func() { done <- c.requestStarted(ctx, "held", nil, nil, started) }()
		f := readACP(t, d)
		if i == 0 {
			first = f
		}
		if err := <-started; err != nil {
			t.Fatal(err)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	if err := c.request(context.Background(), "overflow", nil, nil); !errors.Is(err, errACPCapacity) {
		t.Fatal(err)
	}
	// Reader-ordered notification is a barrier after the late reply releases capacity.
	barrier := make(chan struct{})
	c.notify = func(acpFrame) { close(barrier) }
	replyACP(t, e, first, map[string]bool{})
	if err := e.Encode(map[string]any{"jsonrpc": "2.0", "method": "barrier"}); err != nil {
		t.Fatal(err)
	}
	<-barrier
	done := make(chan error, 1)
	go func() { done <- c.request(context.Background(), "after-drain", nil, nil) }()
	replyACP(t, e, readACP(t, d), map[string]bool{})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestACPMalformedCancelledResponseRetiresConnection(t *testing.T) {
	c, d, e, _ := acpPipe(t)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	done := make(chan error, 1)
	go func() { done <- c.requestStarted(ctx, "held", nil, nil, started) }()
	f := readACP(t, d)
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	cancel()
	<-done
	if err := e.Encode(map[string]any{"jsonrpc": "2.0", "id": f.ID}); err != nil {
		t.Fatal(err)
	}
	<-c.done
}

// Native client requests are answered on the one ACP writer without approval,
// and neither the running turn nor the lane loses its connection.
func TestNativeClientRequestsKeepLaneWithoutApproval(t *testing.T) {
	h := newContinuationHarness(t)
	reverse := func(id json.RawMessage, method string, params map[string]any, expected string) {
		t.Helper()
		params["sessionId"] = testSessionID
		h.primaryWrite.SetEscapeHTML(false)
		must(t, h.primaryWrite.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
		var reply json.RawMessage
		must(t, h.primaryRead.Decode(&reply))
		check(t, string(reply) == expected, "reply to %s: %s", method, reply)
		h.p.primary.mu.Lock()
		err := h.p.primary.err
		h.p.primary.mu.Unlock()
		check(t, err == nil, "%s retired the connection: %v", method, err)
	}
	original := h.start(t, 2, "g/1", "owned-first")
	reverse(json.RawMessage(`"unknown-\u0031"`), "fs/read_text_file", map[string]any{"path": "/etc/hostname"}, `{"jsonrpc":"2.0","id":"unknown-\u0031","error":{"code":-32601,"message":"Method not found"}}`)
	h.answer(t, "p-g/1", "first-answer")
	options := []map[string]string{{"optionId": "allow", "name": "Allow", "kind": "allow_once"}, {"optionId": "reject", "name": "Reject", "kind": "reject_once"}}
	reverse(json.RawMessage(`"<permission&2>"`), "session/request_permission", map[string]any{"toolCall": map[string]string{"toolCallId": "call-1", "title": "shell", "kind": "execute"}, "options": options}, `{"jsonrpc":"2.0","id":"<permission&2>","result":{"outcome":{"outcome":"cancelled"}}}`)
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, original, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	readWorkerReadyID(t, h.bus, "g/1")
	s := h.status(t, 3, "g/1")
	check(t, s.State == "done" && s.Result != nil && s.Result.Result == "first-answer" && s.Result.Outcome == "completed", "turn with native requests: %+v", s)
	writeWorkerRequest(t, h.bus, 4, "turn.ack", map[string]string{"session_id": testSessionID + "@local", "run_id": "g/1"})
	check(t, readWorkerResponse(t, h.bus, 4).Error == nil, "ack failed")
	next := h.start(t, 5, "g/2", "following-explicit")
	h.answer(t, "p-g/2", "next-answer")
	h.terminal(t, "p-g/2", "end_turn")
	replyACP(t, h.primaryWrite, next, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/2"}})
	readWorkerReadyID(t, h.bus, "g/2")
	s = h.status(t, 6, "g/2")
	check(t, s.State == "done" && s.Result != nil && s.Result.Result == "next-answer" && s.Result.Outcome == "completed", "subsequent turn: %+v", s)
}

func TestACPIDAbsentAndNullStayDistinct(t *testing.T) {
	requestRead, requestWrite := io.Pipe()
	responseRead, responseWrite := io.Pipe()
	notified := make(chan string, 1)
	c := newACPClient(acpPrimary, requestWrite, responseRead, func(f acpFrame) { notified <- f.Method })
	t.Cleanup(func() { c.close(); _ = requestRead.Close(); _ = responseWrite.Close() })
	e := json.NewEncoder(responseWrite)
	must(t, e.Encode(map[string]any{"jsonrpc": "2.0", "method": "absent"}))
	check(t, <-notified == "absent", "notification with absent ID was not delivered")
	_, err := io.WriteString(responseWrite, `{"jsonrpc":"2.0","id":null,"method":"unknown"}`+"\n")
	must(t, err)
	var reply json.RawMessage
	must(t, json.NewDecoder(requestRead).Decode(&reply))
	check(t, string(reply) == `{"jsonrpc":"2.0","id":null,"error":{"code":-32601,"message":"Method not found"}}`, "null ID reply: %s", reply)
}

func TestACPInvalidIDShapesUseMalformedFramePath(t *testing.T) {
	for name, id := range map[string]string{
		"object": `{}`, "array": `[]`, "boolean": `true`, "fraction": `1.5`, "out-of-range": `9223372036854775808`,
	} {
		t.Run(name, func(t *testing.T) {
			requestRead, requestWrite := io.Pipe()
			responseRead, responseWrite := io.Pipe()
			c := newACPClient(acpPrimary, requestWrite, responseRead, nil)
			t.Cleanup(func() { c.close(); _ = requestRead.Close(); _ = responseWrite.Close() })
			_, err := io.WriteString(responseWrite, `{"jsonrpc":"2.0","id":`+id+`,"method":"unknown"}`+"\n")
			must(t, err)
			<-c.done
			c.mu.Lock()
			err = c.err
			c.mu.Unlock()
			check(t, err != nil && err.Error() == "malformed Grok ACP frame", "%s ID error: %v", name, err)
		})
	}
}

// Invalid framing and a failed reverse-request reply still retire the connection.
func TestACPMalformedFramesAndReplyWriteFailureRemainFatal(t *testing.T) {
	for name, line := range map[string]string{
		"request-with-result": `{"jsonrpc":"2.0","id":7,"method":"session/request_permission","result":{}}`,
		"request-with-error":  `{"jsonrpc":"2.0","id":7,"method":"example","error":{"code":1,"message":"refused"}}`,
		"version":             `{"jsonrpc":"1.0","id":7,"method":"session/request_permission"}`,
		"framing":             `{"jsonrpc":"2.0","id":7,"method":`,
		"reply-write":         `{"jsonrpc":"2.0","id":7,"method":"session/request_permission"}`,
	} {
		t.Run(name, func(t *testing.T) {
			requestRead, requestWrite := io.Pipe()
			responseRead, responseWrite := io.Pipe()
			c := newACPClient(acpPrimary, requestWrite, responseRead, nil)
			t.Cleanup(func() { c.close(); _ = requestRead.Close(); _ = responseWrite.Close() })
			if name == "reply-write" {
				_ = requestRead.Close()
			}
			if _, err := io.WriteString(responseWrite, line+"\n"); err != nil {
				t.Fatal(err)
			}
			<-c.done
			c.mu.Lock()
			err := c.err
			c.mu.Unlock()
			if name == "reply-write" {
				if !errors.Is(err, io.ErrClosedPipe) {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("malformed frame kept the connection")
			}
			if _, err = requestRead.Read(make([]byte, 1)); err != io.EOF {
				t.Fatalf("malformed frame was answered: %v", err)
			}
		})
	}
}

// Grok broadcasts shared interactions such as permission to every subscriber
// and takes the first answer, so an observer consumes native client requests
// unanswered and keeps serving the frames that follow.
func TestACPObserverLeavesNativeClientRequestsUnanswered(t *testing.T) {
	requestRead, requestWrite := io.Pipe()
	responseRead, responseWrite := io.Pipe()
	notified := make(chan string, 4)
	c := newACPClient(acpObserver, requestWrite, responseRead, func(f acpFrame) { notified <- f.Method })
	t.Cleanup(func() { c.close(); _ = requestRead.Close(); _ = responseWrite.Close() })
	written := make(chan acpFrame, 8)
	go func() {
		defer close(written)
		d := json.NewDecoder(requestRead)
		for {
			var f acpFrame
			if d.Decode(&f) != nil {
				return
			}
			written <- f
		}
	}()
	e := json.NewEncoder(responseWrite)
	done := make(chan error, 1)
	var renamed map[string]bool
	go func() { done <- c.request(context.Background(), "_x.ai/session/rename", nil, &renamed) }()
	rename := <-written
	options := []map[string]string{{"optionId": "allow", "name": "Allow", "kind": "allow_once"}}
	must(t, e.Encode(map[string]any{"jsonrpc": "2.0", "id": 9, "method": "session/request_permission", "params": map[string]any{"sessionId": testSessionID, "toolCall": map[string]string{"toolCallId": "call-1"}, "options": options}}))
	must(t, e.Encode(map[string]any{"jsonrpc": "2.0", "id": 10, "method": "_x.ai/ask_user_question", "params": map[string]any{"sessionId": testSessionID}}))
	must(t, e.Encode(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/sessions/changed"}))
	replyACP(t, e, rename, map[string]bool{"success": true})
	must(t, <-done)
	check(t, renamed["success"], "response after native requests was not delivered: %v", renamed)
	check(t, len(notified) == 1 && <-notified == "_x.ai/sessions/changed", "notification after native requests was not delivered")
	// Any answer would precede this request on the one ordered writer.
	go func() { done <- c.request(context.Background(), "barrier", nil, nil) }()
	barrier := <-written
	check(t, barrier.Method == "barrier", "observer answered a native request: %+v", barrier)
	replyACP(t, e, barrier, map[string]any{})
	must(t, <-done)
	// Malformed input still retires an observer, and nothing else was written.
	_, err := io.WriteString(responseWrite, `{"jsonrpc":"2.0","id":11,"method":"session/request_permission","result":{}}`+"\n")
	must(t, err)
	<-c.done
	for f := range written {
		t.Fatalf("observer wrote %+v", f)
	}
}
