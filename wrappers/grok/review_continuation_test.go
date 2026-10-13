// SPDX-License-Identifier: MIT
package grok

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReviewOverflowCannotReleaseLiveContinuation(t *testing.T) {
	h := newContinuationHarness(t)
	original := h.start(t, 2, "g/1", "owned-first")
	writeWorkerRequest(t, h.bus, 3, "message.deliver", delivery("overflow-continuation"))
	interject := h.expectInterject(t, 3, nil)
	var params struct{ Text string }
	must(t, json.Unmarshal(interject.Params, &params))
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, original, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	h.notify(t, "_x.ai/session/interjection", map[string]any{"interjectionId": "message-1"})
	readWorkerResponse(t, h.bus, 3)
	h.running(t, "held-fallback", params.Text)
	replyACP(t, h.primaryWrite, interject, map[string]any{"result": map[string]string{"status": "queued"}})
	// Native fallback stays live: deliberately send no terminal for it.
	for range 33 {
		h.answer(t, "held-fallback", strings.Repeat("x", 32768))
	}
	readWorkerReadyID(t, h.bus, "g/1")
	select {
	case <-h.p.primary.done:
	default:
		t.Fatal("Worker released a run while its overflowed native continuation was still live")
	}
}
