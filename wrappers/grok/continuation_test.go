// SPDX-License-Identifier: MIT
package grok

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/testsocket"
)

type continuationHarness struct {
	p                           *Wrapper
	bus                         *workerReader
	primaryRead                 *json.Decoder
	observerRead                <-chan continuationACPRead
	primaryWrite, observerWrite *json.Encoder
}

func newContinuationHarness(t *testing.T, configure ...func(*grokSeedProduct)) *continuationHarness {
	t.Helper()
	path := filepath.Join(testsocket.Directory(t), "bus")
	l, e := net.Listen("unix", path)
	must(t, e)
	t.Setenv(host.SocketEnv, path)
	t.Setenv(host.TokenEnv, "continuation-test")
	p := &Wrapper{sessionID: testSessionID}
	rr, rw := io.Pipe()
	sr, sw := io.Pipe()
	or, ow := io.Pipe()
	osr, osw := io.Pipe()
	p.primary = newACPClient(rw, sr, p.receive)
	p.observer = newACPClient(ow, osr, nil)
	product := &grokSeedProduct{Wrapper: p, returned: make(chan error, 8)}
	for _, apply := range configure {
		apply(product)
	}
	worker := kit.NewWorker(product)
	p.SetCaller(worker.Caller())
	served := make(chan error, 1)
	go func() { served <- worker.Serve(context.Background()) }()
	c, e := l.Accept()
	must(t, e)
	reader := &workerReader{connection: c, reader: bufio.NewReader(c), requestIDs: map[int]int{}, pending: map[int]workerResponse{}, ready: map[string]map[string]any{}}
	readLine(t, reader.reader)
	_, e = c.Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"))
	must(t, e)
	writeWorkerRequest(t, reader, 1, "session.open", map[string]any{"name": "continuation@local", "groups": []string{}, "open": map[string]any{}})
	opened := readWorkerResponse(t, reader, 1)
	check(t, opened.Error == nil, "Open failed: %s", opened.Error)
	observerRead, stopReaders, joinReaders := continuationReaders(reader, json.NewDecoder(or))
	t.Cleanup(func() {
		stopReaders()
		_ = c.Close()
		p.primary.close()
		p.observer.close()
		rr.Close()
		sw.Close()
		or.Close()
		osw.Close()
		joinReaders()
		<-served
		l.Close()
	})
	return &continuationHarness{p, reader, json.NewDecoder(rr), observerRead, json.NewEncoder(sw), json.NewEncoder(osw)}
}
func (h *continuationHarness) notify(t *testing.T, method string, params map[string]any) {
	t.Helper()
	params["sessionId"] = testSessionID
	must(t, h.primaryWrite.Encode(map[string]any{"jsonrpc": "2.0", "method": method, "params": params}))
}
func (h *continuationHarness) running(t *testing.T, id, text string) {
	h.notify(t, "_x.ai/queue/changed", map[string]any{"runningKind": "prompt", "runningPromptId": id, "runningText": text})
}
func (h *continuationHarness) answer(t *testing.T, id, text string) {
	h.notify(t, "session/update", map[string]any{"update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"text": text}}, "_meta": map[string]string{"promptId": id}})
}
func (h *continuationHarness) terminal(t *testing.T, id, reason string) {
	h.notify(t, "_x.ai/session_notification", map[string]any{"update": map[string]string{"sessionUpdate": "turn_completed", "prompt_id": id, "stop_reason": reason}})
}
func (h *continuationHarness) barrier(t *testing.T) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- h.p.primary.request(context.Background(), "barrier", nil, nil) }()
	replyACP(t, h.primaryWrite, readACP(t, h.primaryRead), map[string]any{})
	must(t, <-done)
}
func (h *continuationHarness) start(t *testing.T, id int, key, text string) acpFrame {
	t.Helper()
	writeWorkerRequest(t, h.bus, id, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": key, "input": text})
	check(t, readWorkerResponse(t, h.bus, id).Error == nil, "execute refused")
	f := readACP(t, h.primaryRead)
	check(t, f.Method == "session/prompt", "unexpected additional native submission: %s", f.Method)
	h.running(t, "p-"+key, text)
	h.barrier(t)
	return f
}
func (h *continuationHarness) status(t *testing.T, id int, key string) kit.RunStatus {
	writeWorkerRequest(t, h.bus, id, "turn.status", map[string]any{"session_id": testSessionID + "@local", "run_id": key})
	f := readWorkerResponse(t, h.bus, id)
	var s kit.RunStatus
	must(t, json.Unmarshal(f.Result, &s))
	return s
}
func TestActorAcknowledgedDeliveryNeverRequeuesAfterTerminal(t *testing.T) {
	testContinuation(t, true, false)
}
func TestActiveDeliveryUsesInterject(t *testing.T)        { testContinuation(t, false, false) }
func TestNativeContinuationEOFIsUnavailable(t *testing.T) { testContinuation(t, true, true) }
func testContinuation(t *testing.T, fallback, lost bool) {
	h := newContinuationHarness(t)
	original := h.start(t, 2, "g/1", "owned-first")
	writeWorkerRequest(t, h.bus, 3, "message.deliver", delivery("once-marker"))
	interject := h.expectInterject(t, 3, nil)
	check(t, interject.Method == "_x.ai/interject", "not native interject")
	var params struct{ Text string }
	must(t, json.Unmarshal(interject.Params, &params))
	h.answer(t, "p-g/1", "first-answer")
	if fallback {
		h.terminal(t, "p-g/1", "cancelled")
		replyACP(t, h.primaryWrite, original, map[string]any{"stopReason": "cancelled", "_meta": map[string]string{"promptId": "p-g/1"}})
		h.barrier(t)
		check(t, h.status(t, 8, "g/1").State == "running", "unclassified delivery released original slot")
	}
	h.notify(t, "_x.ai/session/interjection", map[string]any{"interjectionId": "message-1"})
	receipt := readWorkerResponse(t, h.bus, 3)
	var admitted kit.DeliveryReceipt
	must(t, json.Unmarshal(receipt.Result, &admitted))
	check(t, admitted.Disposition == "injected", "primary actor acknowledgement not retained: %s", receipt.Result)
	// The observer's RPC response is deliberately still withheld here.
	if fallback {
		h.running(t, "native-continuation", params.Text)
		h.answer(t, "foreign", "foreign-poison")
		h.notify(t, "session/update", map[string]any{"update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"text": "replay-poison"}}, "_meta": map[string]any{"promptId": "native-continuation", "isReplay": true}})
		h.answer(t, "native-continuation", "second-answer")
		h.barrier(t)
		check(t, h.status(t, 9, "g/1").State == "running", "held continuation published terminal")
		writeWorkerRequest(t, h.bus, 10, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/2", "input": "must-not-submit"})
		check(t, readWorkerResponse(t, h.bus, 10).Error != nil, "continuation did not reserve shared Run")
		if lost {
			h.p.primary.close()
		} else {
			writeWorkerRequest(t, h.bus, 11, "turn.interrupt", map[string]string{"session_id": testSessionID + "@local"})
			check(t, readACP(t, h.primaryRead).Method == "session/cancel", "interrupt did not reach current native work")
			check(t, readWorkerResponse(t, h.bus, 11).Error == nil, "interrupt failed")
			h.terminal(t, "native-continuation", "end_turn")
		}
	} else {
		h.terminal(t, "p-g/1", "end_turn")
		replyACP(t, h.primaryWrite, original, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	}
	replyACP(t, h.observerWrite, interject, map[string]any{"result": map[string]string{"status": "queued"}})
	readWorkerReadyID(t, h.bus, "g/1")
	for _, id := range []int{12, 13} {
		s := h.status(t, id, "g/1")
		if lost {
			check(t, s.State == "unavailable" && s.Result == nil, "missing continuation terminal fabricated success")
			continue
		}
		check(t, s.State == "done" && s.Result != nil, "no retained terminal: %+v", s)
		expected := "first-answer"
		outcome := "completed"
		if fallback {
			expected += "\nsecond-answer"
			outcome = "interrupted"
		}
		check(t, s.Result.Result == expected && s.Result.Outcome == outcome, "aggregate=%+v", s.Result)
	}
	if lost {
		return
	}
	writeWorkerRequest(t, h.bus, 14, "turn.ack", map[string]string{"session_id": testSessionID + "@local", "run_id": "g/1"})
	check(t, readWorkerResponse(t, h.bus, 14).Error == nil, "ack failed")
	next := h.start(t, 15, "g/2", "following-explicit")
	check(t, !strings.Contains(string(next.Params), "once-marker"), "submitted message replayed")
	h.answer(t, "p-g/2", "next-answer")
	h.terminal(t, "p-g/2", "end_turn")
	replyACP(t, h.primaryWrite, next, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/2"}})
	check(t, readWorkerReadyID(t, h.bus, "g/2")["state"] == "done", "next run missing")
}

func TestInterjectCancellationPreservesAttemptedNativeAccounting(t *testing.T) {
	for _, submitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-write", true: "after-write"}[submitted], func(t *testing.T) {
			h := newContinuationHarness(t)
			original := h.start(t, 2, "g/1", "owned-first")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			returned := make(chan continuationDeliveryResult, 1)
			if !submitted {
				cancel()
			}
			go func() {
				r, e := h.p.Deliver(ctx, delivery("cancel-marker"), nil)
				returned <- continuationDeliveryResult{r, e}
			}()
			var interject acpFrame
			if submitted {
				interject = h.expectInterject(t, 0, returned)
				cancel()
			}
			result := <-returned
			// Before submission, a still-unavailable local native owner can reject
			// explicitly; a nil Go error alone is not an admission receipt.
			if result.err != nil && !errors.Is(result.err, context.Canceled) {
				t.Fatalf("unexpected cancellation error: %v", result.err)
			}
			if result.err == nil && (submitted || result.receipt.Disposition != "rejected" || result.receipt.Reason != "native_turn_unavailable") {
				t.Fatalf("cancelled caller admitted before native acknowledgement: %+v", result)
			}
			h.answer(t, "p-g/1", "first")
			h.terminal(t, "p-g/1", "end_turn")
			replyACP(t, h.primaryWrite, original, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
			h.barrier(t)
			if submitted {
				check(t, h.status(t, 8, "g/1").State == "running", "caller cancellation discarded native accounting")
				var params struct{ Text string }
				must(t, json.Unmarshal(interject.Params, &params))
				h.notify(t, "_x.ai/session/interjection", map[string]any{"interjectionId": "message-1"})
				h.running(t, "continued", params.Text)
				h.answer(t, "continued", "after-cancel")
				h.terminal(t, "continued", "end_turn")
				replyACP(t, h.observerWrite, interject, map[string]any{"result": map[string]string{"status": "queued"}})
			} else {
				barrier := make(chan error, 1)
				go func() { barrier <- h.p.observer.request(context.Background(), "barrier", nil, nil) }()
				f := h.readObserver(t)
				check(t, f.Method == "barrier", "cancelled unsent message reached native")
				replyACP(t, h.observerWrite, f, map[string]any{})
				must(t, <-barrier)
			}
			readWorkerReadyID(t, h.bus, "g/1")
			s := h.status(t, 9, "g/1")
			expected := "first"
			if submitted {
				expected += "\nafter-cancel"
			}
			check(t, s.State == "done" && s.Result != nil && s.Result.Result == expected, "retained result=%+v", s)
		})
	}
}

// Done is first consulted after Deliver selects the active path, at the
// deliveryGate select. Observe that boundary before ending the native turn.
type deliveryGateObservedContext struct {
	context.Context
	entered chan struct{}
}

func (c deliveryGateObservedContext) Done() <-chan struct{} {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	return c.Context.Done()
}

func TestWaitingInterjectCannotWakeRetiredRun(t *testing.T) {
	h := newContinuationHarness(t)
	original := h.start(t, 2, "g/1", "owned-first")
	h.p.mu.Lock()
	h.p.deliveryGate = make(chan struct{}, 1)
	gate := h.p.deliveryGate
	h.p.mu.Unlock()
	type deliveryResult struct {
		receipt kit.DeliveryReceipt
		err     error
	}
	returned := make(chan deliveryResult, 1)
	ctx := deliveryGateObservedContext{Context: context.Background(), entered: make(chan struct{}, 1)}
	go func() { r, err := h.p.Deliver(ctx, delivery("not-submitted"), nil); returned <- deliveryResult{r, err} }()
	select {
	case <-ctx.entered:
	case result := <-returned:
		t.Fatalf("delivery did not enter the active gate: %+v", result)
	}
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, original, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	readWorkerReadyID(t, h.bus, "g/1")
	gate <- struct{}{}
	result := <-returned
	var notRunning *kit.ProtocolError
	check(t, errors.As(result.err, &notRunning) && notRunning.Code == -32004 && result.receipt.Disposition == "", "retired run was woken: %+v", result)
	barrier := make(chan error, 1)
	go func() { barrier <- h.p.observer.request(context.Background(), "barrier", nil, nil) }()
	f := h.readObserver(t)
	check(t, f.Method == "barrier", "retired run got native interject")
	replyACP(t, h.observerWrite, f, map[string]any{})
	must(t, <-barrier)
}

func TestOwnedNativeOutputOverflowIsUnavailable(t *testing.T) {
	h := newContinuationHarness(t)
	original := h.start(t, 2, "g/1", "owned-first")
	writeWorkerRequest(t, h.bus, 3, "message.deliver", delivery("overflow-seed"))
	interject := h.expectInterject(t, 3, nil)
	var params struct{ Text string }
	must(t, json.Unmarshal(interject.Params, &params))
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, original, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	h.notify(t, "_x.ai/session/interjection", map[string]any{"interjectionId": "message-1"})
	readWorkerResponse(t, h.bus, 3)
	h.running(t, "held-fallback", params.Text)
	replyACP(t, h.observerWrite, interject, map[string]any{"result": map[string]string{"status": "queued"}})
	for i := 0; i < 33; i++ {
		h.answer(t, "held-fallback", strings.Repeat("x", 32768))
	}
	// No fallback terminal is supplied. Overflow must retire native transport.
	<-h.p.primary.done
	readWorkerReadyID(t, h.bus, "g/1")
	s := h.status(t, 4, "g/1")
	check(t, s.State == "unavailable" && s.Result == nil, "overflow returned truncated success: %+v", s)
	h.p.mu.Lock()
	failure := h.p.nativeFailure
	h.p.mu.Unlock()
	check(t, failure != nil, "overflow left a live idle integration")
}

func TestObserverLossSettlesUnclassifiedRunWithoutNativeTerminal(t *testing.T) {
	h := newContinuationHarness(t)
	h.start(t, 2, "g/1", "owned-first")
	writeWorkerRequest(t, h.bus, 3, "message.deliver", delivery("unclassified"))
	h.expectInterject(t, 3, nil)
	h.p.observer.close()
	response := readWorkerResponse(t, h.bus, 3)
	var receipt kit.DeliveryReceipt
	_ = json.Unmarshal(response.Result, &receipt)
	check(t, receipt.Disposition != "injected" && receipt.Disposition != "queued_for_next_turn", "lost observer invented admission: %s", response.Result)
	check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "unavailable", "unclassified native work became a normal terminal")
	<-h.p.primary.done
}

func TestObserverWriteGateCannotSubmitAfterNativeTerminal(t *testing.T) {
	h := newContinuationHarness(t)
	original := h.start(t, 2, "g/1", "owned-first")
	<-h.p.observer.writeGate
	returned := make(chan error, 1)
	go func() { _, e := h.p.Deliver(context.Background(), delivery("never-attempted"), nil); returned <- e }()
	h.p.mu.Lock()
	for h.p.pendingPrompt.delivery == nil {
		changed := h.p.pendingPrompt.changed
		h.p.mu.Unlock()
		<-changed
		h.p.mu.Lock()
	}
	h.p.mu.Unlock()
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, original, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	h.barrier(t)
	h.p.observer.writeGate <- struct{}{}
	err := <-returned
	var notRunning *kit.ProtocolError
	check(t, errors.As(err, &notRunning) && notRunning.Code == -32004, "terminal crossing = %v", err)
	barrier := make(chan error, 1)
	go func() { barrier <- h.p.observer.request(context.Background(), "barrier", nil, nil) }()
	f := h.readObserver(t)
	check(t, f.Method == "barrier", "terminal-crossed interject reached native")
	replyACP(t, h.observerWrite, f, map[string]any{})
	must(t, <-barrier)
	check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "done", "unsent refusal stranded original run")
}
