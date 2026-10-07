// SPDX-License-Identifier: MIT
package grok

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
)

// A delivery callback the SDK captured for an ended Run must not reach the
// lane's next Run: it is refused before any native write, the next Run is left
// untouched, and a delivery captured for the next Run still reaches native.
func TestStaleRunDeliveryCannotSteerNextRun(t *testing.T) {
	h := newContinuationHarness(t)
	first := h.start(t, 2, "g/1", "first")
	h.p.mu.Lock()
	runA := h.p.run
	h.p.mu.Unlock()
	h.answer(t, "p-g/1", "first-answer")
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, first, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	readWorkerReadyID(t, h.bus, "g/1")
	second := h.start(t, 3, "g/2", "second")
	h.p.mu.Lock()
	runB, current := h.p.run, h.p.pendingPrompt
	h.p.mu.Unlock()
	check(t, runA != nil && runB != nil && runA != runB && current != nil, "distinct Runs A and B are required")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stale := make(chan error, 1)
	go func() {
		_, err := h.p.Deliver(ctx, delivery("stale-for-A"), runA)
		stale <- err
	}()
	select {
	case err := <-stale:
		var notRunning *kit.ProtocolError
		check(t, errors.As(err, &notRunning) && notRunning.Code == -32004, "stale Run A delivery = %v", err)
	case next := <-h.observerRead:
		t.Fatalf("stale Run A delivery reached native: %s %s", next.frame.Method, next.frame.Params)
	}
	h.p.mu.Lock()
	untouched := h.p.run == runB && h.p.pendingPrompt == current && current.delivery == nil
	h.p.mu.Unlock()
	check(t, untouched, "stale Run A delivery changed Run B")

	writeWorkerRequest(t, h.bus, 4, "message.deliver", delivery("valid-for-B"))
	interject := h.expectInterject(t, 4, nil)
	var params struct{ Text string }
	must(t, json.Unmarshal(interject.Params, &params))
	check(t, strings.Contains(params.Text, "valid-for-B") && !strings.Contains(params.Text, "stale-for-A"), "Run B interject = %q", params.Text)
	h.notify(t, "_x.ai/session/interjection", map[string]any{"interjectionId": "message-1"})
	var receipt kit.DeliveryReceipt
	must(t, json.Unmarshal(readWorkerResponse(t, h.bus, 4).Result, &receipt))
	check(t, receipt.Disposition == "injected", "Run B delivery = %+v", receipt)
	replyACP(t, h.observerWrite, interject, map[string]any{"result": map[string]string{"status": "queued"}})
	h.answer(t, "p-g/2", "second-answer")
	h.terminal(t, "p-g/2", "end_turn")
	replyACP(t, h.primaryWrite, second, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/2"}})
	check(t, readWorkerReadyID(t, h.bus, "g/2")["state"] == "done", "Run B did not finish")
}
