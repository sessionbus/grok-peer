// SPDX-License-Identifier: MIT
package grok

import (
	"encoding/json"
	"testing"
)

func TestReviewTerminalMismatchCannotReleaseLiveContinuation(t *testing.T) {
	h := newContinuationHarness(t)
	original := h.start(t, 2, "g/1", "owned-first")
	writeWorkerRequest(t, h.bus, 3, "message.deliver", delivery("held-continuation"))
	interject := h.expectInterject(t, 3, nil)
	var params struct{ Text string }
	must(t, json.Unmarshal(interject.Params, &params))
	h.terminal(t, "p-g/1", "end_turn")
	h.notify(t, "_x.ai/session/interjection", map[string]any{"interjectionId": "message-1"})
	readWorkerResponse(t, h.bus, 3)
	h.running(t, "held-fallback", params.Text)
	replyACP(t, h.primaryWrite, interject, map[string]any{"result": map[string]string{"status": "queued"}})
	h.barrier(t)
	// Original RPC arrives after actual fallback admission, contradicting its
	// native prompt identity. Fallback deliberately has no terminal.
	replyACP(t, h.primaryWrite, original, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "wrong-original"}})
	readWorkerReadyID(t, h.bus, "g/1")
	select {
	case <-h.p.primary.done:
	default:
		t.Fatal("terminal mismatch released Worker while native continuation remained live")
	}
}
