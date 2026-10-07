// SPDX-License-Identifier: MIT
package grok

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
)

// Native can read and admit a prompt before its caller observes Write returning.
// This holds that actual pipe boundary, without a production scheduling hook.
type promptOwnershipWriter struct {
	io.WriteCloser
	entered, completed, released chan struct{}
	once, holdOnce               sync.Once
	result                       error
}

func (w *promptOwnershipWriter) Write(b []byte) (int, error) {
	prompt := false
	if bytes.Contains(b, []byte(`"method":"session/prompt"`)) {
		w.holdOnce.Do(func() { prompt = true })
	}
	if prompt {
		close(w.entered)
	}
	n, err := w.WriteCloser.Write(b)
	if prompt && err == nil && n == len(b) {
		close(w.completed)
		<-w.released
		if w.result != nil {
			return n, w.result
		}
	}
	return n, err
}
func (w *promptOwnershipWriter) release()     { w.once.Do(func() { close(w.released) }) }
func (w *promptOwnershipWriter) Close() error { w.release(); return w.WriteCloser.Close() }
func holdPromptOwnershipWrite(t *testing.T, h *continuationHarness) *promptOwnershipWriter {
	t.Helper()
	<-h.p.primary.writeGate
	w := &promptOwnershipWriter{WriteCloser: h.p.primary.input, entered: make(chan struct{}), completed: make(chan struct{}), released: make(chan struct{})}
	h.p.primary.input = w
	h.p.primary.writeGate <- struct{}{}
	t.Cleanup(w.release)
	return w
}
func beginHeldPrompt(t *testing.T, h *continuationHarness, w *promptOwnershipWriter) (acpFrame, *nativePrompt) {
	t.Helper()
	writeWorkerRequest(t, h.bus, 2, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/1", "input": "owned-first"})
	check(t, readWorkerResponse(t, h.bus, 2).Error == nil, "execute refused")
	f := readACP(t, h.primaryRead)
	check(t, f.Method == "session/prompt", "not prompt: %s", f.Method)
	awaitInterrupt(t, w.completed, "inner prompt write accepted")
	h.p.mu.Lock()
	native := h.p.pendingPrompt
	h.p.mu.Unlock()
	if native == nil {
		t.Fatal("native request owner not registered before write")
	}
	return f, native
}

func TestWorkerDeliveryUsesAdmittedOwnerBeforePromptWriteReturns(t *testing.T) {
	h := newContinuationHarness(t)
	w := holdPromptOwnershipWrite(t, h)
	prompt, native := beginHeldPrompt(t, h, w)
	h.running(t, "p-g/1", "owned-first")
	awaitInterrupt(t, native.admitted, "primary native admission")
	writeWorkerRequest(t, h.bus, 3, "message.deliver", delivery("once-marker"))
	interject := h.expectInterject(t, 3, nil)
	h.notify(t, "_x.ai/session/interjection", map[string]any{"interjectionId": "message-1"})
	result := readWorkerResponse(t, h.bus, 3)
	var receipt kit.DeliveryReceipt
	must(t, json.Unmarshal(result.Result, &receipt))
	check(t, receipt.Disposition == "injected", "wrong receipt: %+v", result)
	replyACP(t, h.observerWrite, interject, map[string]any{"result": map[string]string{"status": "queued"}})
	w.release()
	h.answer(t, "p-g/1", "answer")
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, prompt, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "done", "completed prompt failed")
	check(t, h.status(t, 4, "g/1").Result.Result == "answer", "native output changed")
}

func TestPendingOwnerDeliveryCancellationWaitsForNativeAdmission(t *testing.T) {
	h := newContinuationHarness(t)
	w := holdPromptOwnershipWrite(t, h)
	prompt, _ := beginHeldPrompt(t, h, w)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := deliveryGateObservedContext{Context: ctx, entered: make(chan struct{}, 1)}
	returned := make(chan error, 1)
	run := h.currentRun()
	go func() { _, err := h.p.Deliver(observed, delivery("unsent"), run); returned <- err }()
	awaitInterrupt(t, observed.entered, "delivery gate entered before native admission")
	cancel()
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-admission cancellation=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pre-admission delivery did not cancel")
	}
	// Cancellation before admission must not submit anything to the observer.
	done := make(chan error, 1)
	go func() { done <- h.p.observer.request(context.Background(), "barrier", nil, nil) }()
	f := h.readObserver(t)
	check(t, f.Method == "barrier", "pre-admission delivery submitted %s", f.Method)
	replyACP(t, h.observerWrite, f, map[string]any{})
	must(t, <-done)
	h.running(t, "p-g/1", "owned-first")
	w.release()
	h.answer(t, "p-g/1", "answer")
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, prompt, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "done", "cancelled delivery changed original prompt")
}

// The first Done observation is the delivery gate; the second is the existing
// native-admission select. This binds the pre-admission schedule without a sleep.
type ownershipAdmissionContext struct {
	context.Context
	mu      sync.Mutex
	calls   int
	waiting chan struct{}
}

func (c *ownershipAdmissionContext) Done() <-chan struct{} {
	c.mu.Lock()
	c.calls++
	if c.calls == 2 {
		close(c.waiting)
	}
	c.mu.Unlock()
	return c.Context.Done()
}
func TestPendingOwnerWaitsForPrimaryAdmissionOrLoss(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "admitted", true: "primary-loss"}[lost], func(t *testing.T) {
			h := newContinuationHarness(t)
			w := holdPromptOwnershipWrite(t, h)
			prompt, native := beginHeldPrompt(t, h, w)
			ctx := &ownershipAdmissionContext{Context: context.Background(), waiting: make(chan struct{})}
			returned := make(chan continuationDeliveryResult, 1)
			run := h.currentRun()
			go func() {
				r, e := h.p.Deliver(ctx, delivery("after-admission"), run)
				returned <- continuationDeliveryResult{r, e}
			}()
			select {
			case <-ctx.waiting:
			case result := <-returned:
				t.Fatalf("delivery did not await admission: %+v", result)
			case <-time.After(3 * time.Second):
				t.Fatal("delivery did not reach admission wait")
			}
			// Both streams are live; before the primary admission event the next
			// observer frame must be our barrier, never the pending interject.
			done := make(chan error, 1)
			go func() { done <- h.p.observer.request(context.Background(), "barrier", nil, nil) }()
			f := h.readObserver(t)
			check(t, f.Method == "barrier", "interject preceded primary admission")
			replyACP(t, h.observerWrite, f, map[string]any{})
			must(t, <-done)
			if lost {
				h.p.primary.close()
				select {
				case result := <-returned:
					check(t, result.err != nil, "loss accepted delivery: %+v", result)
				case <-time.After(3 * time.Second):
					t.Fatal("primary loss stranded admission wait")
				}
				awaitInterrupt(t, native.done, "failed prompt sender joined")
				check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "unavailable", "lost prompt survived")
				return
			}
			h.running(t, "p-g/1", "owned-first")
			f = h.expectInterject(t, 0, returned)
			h.notify(t, "_x.ai/session/interjection", map[string]any{"interjectionId": "message-1"})
			r := <-returned
			must(t, r.err)
			check(t, r.receipt.Disposition == "injected", "receipt=%+v", r)
			replyACP(t, h.observerWrite, f, map[string]any{"result": map[string]string{"status": "queued"}})
			w.release()
			h.answer(t, "p-g/1", "answer")
			h.terminal(t, "p-g/1", "end_turn")
			replyACP(t, h.primaryWrite, prompt, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
			check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "done", "admitted prompt failed")
		})
	}
}

func TestPendingOwnerInterruptJoinsHeldPromptWrite(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "write-failure"}[failed], func(t *testing.T) {
			h := newContinuationHarness(t)
			w := holdPromptOwnershipWrite(t, h)
			if failed {
				w.result = errors.New("fixture prompt write failure")
			}
			prompt, native := beginHeldPrompt(t, h, w)
			h.running(t, "p-g/1", "owned-first")
			awaitInterrupt(t, native.admitted, "primary admission before interrupt")
			ctx := deliveryGateObservedContext{Context: context.Background(), entered: make(chan struct{}, 1)}
			returned := make(chan error, 1)
			h.p.mu.Lock()
			run := h.p.run
			h.p.mu.Unlock()
			go func() { returned <- h.p.Interrupt(ctx, run) }()
			select {
			case <-ctx.entered:
			case err := <-returned:
				t.Fatalf("interrupt ignored attempted pending owner: %v", err)
			case <-time.After(3 * time.Second):
				t.Fatal("interrupt never reached original write gate")
			}
			h.p.mu.Lock()
			operation := native.interrupt
			h.p.mu.Unlock()
			check(t, operation != nil, "interrupt has no prompt owner")
			w.release()
			if failed {
				select {
				case err := <-returned:
					check(t, err != nil, "failed transport accepted interrupt")
				case <-time.After(3 * time.Second):
					t.Fatal("failed prompt stranded owned interrupt")
				}
				awaitInterrupt(t, native.done, "failed original request joined")
				awaitInterrupt(t, operation.done, "failed interrupt joined")
				check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "unavailable", "write failure not unavailable")
				h.p.mu.Lock()
				owner := h.p.pendingPrompt
				retiring := native.retiring
				h.p.mu.Unlock()
				check(t, owner == nil && retiring, "failed owner bypassed retirement")
				return
			}
			check(t, readACP(t, h.primaryRead).Method == "session/cancel", "missing owned cancel after original write")
			must(t, <-returned)
			must(t, native.Interrupt(context.Background()))
			h.barrier(t) // No duplicate callback/fallback cancel.
			h.terminal(t, "p-g/1", "cancelled")
			replyACP(t, h.primaryWrite, prompt, map[string]any{"stopReason": "cancelled", "_meta": map[string]string{"promptId": "p-g/1"}})
			check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "done", "interrupt lost original Run")
			check(t, h.status(t, 4, "g/1").Result.Outcome == "interrupted", "wrong interrupt outcome")
			next := h.start(t, 5, "g/2", "following")
			h.answer(t, "p-g/2", "next-answer")
			h.terminal(t, "p-g/2", "end_turn")
			replyACP(t, h.primaryWrite, next, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/2"}})
			readWorkerReadyID(t, h.bus, "g/2")
			check(t, h.status(t, 6, "g/2").Result.Result == "next-answer", "healthy next Run lost")
		})
	}
}
