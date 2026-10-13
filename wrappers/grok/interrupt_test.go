// SPDX-License-Identifier: MIT
package grok

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"
)

// Hold reporting a completed inner pipe write, separately from blocking the
// actual pipe write. The native reader remains free to produce its terminal.
type interruptWriteGate struct {
	io.WriteCloser
	entered, completed, released, closed chan struct{}
	releaseOnce, closeOnce               sync.Once
}

func (w *interruptWriteGate) Write(b []byte) (int, error) {
	cancel := bytes.Contains(b, []byte(`"method":"session/cancel"`))
	if cancel {
		close(w.entered)
	}
	n, err := w.WriteCloser.Write(b)
	if cancel && n == len(b) && err == nil {
		close(w.completed)
		<-w.released
	}
	return n, err
}
func (w *interruptWriteGate) release() { w.releaseOnce.Do(func() { close(w.released) }) }
func (w *interruptWriteGate) Close() error {
	w.closeOnce.Do(func() { close(w.closed) })
	w.release()
	return w.WriteCloser.Close()
}
func awaitInterrupt(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("interrupt boundary did not settle:", label)
	}
}
func gateInterruptWrite(t *testing.T, h *continuationHarness) *interruptWriteGate {
	t.Helper()
	<-h.p.primary.writeGate
	w := &interruptWriteGate{WriteCloser: h.p.primary.input, entered: make(chan struct{}), completed: make(chan struct{}), released: make(chan struct{}), closed: make(chan struct{})}
	h.p.primary.input = w
	h.p.primary.writeGate <- struct{}{}
	t.Cleanup(w.release)
	return w
}
func TestWorkerInterruptJoinsCompletedWriteBeforeRunReturn(t *testing.T) {
	h := newContinuationHarness(t)
	prompt := h.start(t, 2, "g/1", "owned")
	w := gateInterruptWrite(t, h)
	h.p.mu.Lock()
	native := h.p.pendingPrompt
	h.p.mu.Unlock()
	writeWorkerRequest(t, h.bus, 3, "turn.interrupt", map[string]string{"session_id": testSessionID + "@local"})
	check(t, h.readPrimary(t).Method == "session/cancel", "missing native cancel")
	awaitInterrupt(t, w.completed, "inner cancel Write accepted")
	h.answer(t, "p-g/1", "interrupted-answer")
	h.terminal(t, "p-g/1", "cancelled")
	replyACP(t, h.primaryWrite, prompt, map[string]any{"stopReason": "cancelled", "_meta": map[string]string{"promptId": "p-g/1"}})
	awaitInterrupt(t, native.done, "native RPC terminal")
	check(t, h.status(t, 4, "g/1").State == "running", "native terminal released Run before interrupt write settled")
	select {
	case <-w.closed:
		t.Fatal("completed write was retired by Run completion")
	default:
	}
	w.release()
	check(t, readWorkerResponse(t, h.bus, 3).Error == nil, "interrupt reply failed")
	ready := readWorkerReadyID(t, h.bus, "g/1")
	check(t, ready["state"] == "done", "run failed: %v", ready)
	check(t, h.status(t, 5, "g/1").Result.Outcome == "interrupted", "native outcome changed")
	// A stale callback on the completed prompt must not send another cancel.
	must(t, native.Interrupt(context.Background()))
	h.barrier(t)
	next := h.start(t, 6, "g/2", "following")
	h.answer(t, "p-g/2", "next-answer")
	h.terminal(t, "p-g/2", "end_turn")
	replyACP(t, h.primaryWrite, next, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/2"}})
	readWorkerReadyID(t, h.bus, "g/2")
	check(t, h.status(t, 7, "g/2").Result.Result == "next-answer", "healthy next run lost")
}
func TestSeedInterruptCallbackAndFallbackShareOneCancel(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	h := newContinuationHarness(t, func(p *grokSeedProduct) { p.entered = entered; p.release = release })
	var once sync.Once
	defer once.Do(func() { close(release) })
	d := delivery("owned-seed")
	d.RunID = "g/1"
	writeWorkerRequest(t, h.bus, 2, "message.deliver", d)
	prompt := h.readPrimary(t)
	var params struct{ Prompt []struct{ Text string } }
	must(t, json.Unmarshal(prompt.Params, &params))
	h.running(t, "seed-native", params.Prompt[0].Text)
	awaitInterrupt(t, entered, "seed ReportDelivery")
	writeWorkerRequest(t, h.bus, 3, "turn.interrupt", map[string]string{"session_id": testSessionID + "@local"})
	check(t, h.readPrimary(t).Method == "session/cancel", "missing first cancel")
	check(t, readWorkerResponse(t, h.bus, 3).Error == nil, "interrupt callback incomplete")
	once.Do(func() { close(release) })
	check(t, readWorkerResponse(t, h.bus, 2).Error == nil, "seed receipt failed")
	h.barrier(t) // The next native frame must be this barrier, not a duplicate cancel.
	h.answer(t, "seed-native", "answer")
	h.terminal(t, "seed-native", "cancelled")
	replyACP(t, h.primaryWrite, prompt, map[string]any{"stopReason": "cancelled", "_meta": map[string]string{"promptId": "seed-native"}})
	check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "done", "seed failed")
}
func TestWorkerBusLossSettlesBlockedInterruptWrite(t *testing.T) {
	h := newContinuationHarness(t)
	h.start(t, 2, "g/1", "owned")
	w := gateInterruptWrite(t, h)
	h.p.mu.Lock()
	run := h.p.run
	h.p.mu.Unlock()
	writeWorkerRequest(t, h.bus, 3, "turn.interrupt", map[string]string{"session_id": testSessionID + "@local"})
	awaitInterrupt(t, w.entered, "cancel entered unread native pipe")
	select {
	case <-w.completed:
		t.Fatal("unread cancel unexpectedly completed")
	default:
	}
	must(t, h.bus.connection.Close())
	awaitInterrupt(t, w.closed, "bus loss closes blocked native write")
	awaitInterrupt(t, run.Done(), "Worker settles run on bus loss")
	select {
	case <-w.completed:
		t.Fatal("failed attempted write reported complete")
	default:
	}
}
