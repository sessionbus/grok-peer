// SPDX-License-Identifier: MIT
package grok

import (
	"encoding/json"
	"testing"

	kit "github.com/antst/sessionbus/bus/sdk/go"
)

// A Run seeded by a peer delivery is sent as a parent message, and Grok's
// queue notice for it, which carries its own kind, admits it only by the exact
// prompt id the lane sent.
func TestPeerDeliveryRunIsSentAndAdmittedAsParentMessage(t *testing.T) {
	h := newContinuationHarness(t)
	d := delivery("peer-body")
	d.MessageID = "message-7"
	d.RunID = "g/1"
	writeWorkerRequest(t, h.bus, 2, "message.deliver", d)
	prompt := h.readPrimary(t)
	check(t, prompt.Method == "session/prompt", "seed was not one owned prompt: %s", prompt.Method)
	var params struct {
		Prompt []struct{ Text string }
		Meta   map[string]string `json:"_meta"`
	}
	must(t, json.Unmarshal(prompt.Params, &params))
	check(t, len(params.Prompt) == 1 && params.Meta["promptId"] == "parent-message-message-7" && len(params.Meta) == 1, "delivery prompt not marked: %s", prompt.Params)

	admittedAs := func() string {
		h.p.mu.Lock()
		defer h.p.mu.Unlock()
		return h.p.pendingPrompt.nativeID
	}
	parent := func(id string) {
		h.notify(t, "_x.ai/queue/changed", map[string]any{"runningKind": "parent_agent_message", "runningPromptId": id, "runningText": params.Prompt[0].Text})
		h.barrier(t)
	}
	parent("parent-message-message-8")
	check(t, admittedAs() == "", "a parent notice for another id admitted the delivery")
	parent("parent-message-message-7")
	check(t, admittedAs() == "parent-message-message-7", "the parent notice with the sent id did not admit the delivery")

	h.answer(t, "parent-message-message-7", "peer-answer")
	h.terminal(t, "parent-message-message-7", "end_turn")
	replyACP(t, h.primaryWrite, prompt, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "parent-message-message-7"}})
	var receipt kit.DeliveryReceipt
	must(t, json.Unmarshal(readWorkerResponse(t, h.bus, 2).Result, &receipt))
	check(t, receipt.Disposition == "injected", "wrong receipt: %+v", receipt)
	check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "done", "marked Run did not complete")
	check(t, h.status(t, 3, "g/1").Result.Result == "peer-answer", "marked Run output changed")
}

// The owner's Run input is an ordinary prompt: it carries no _meta, a parent
// notice never admits it, and the ordinary notice admits it as before.
func TestOwnerRunStaysAnOrdinaryPrompt(t *testing.T) {
	h := newContinuationHarness(t)
	writeWorkerRequest(t, h.bus, 2, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/1", "input": "owner-task"})
	check(t, readWorkerResponse(t, h.bus, 2).Error == nil, "execute refused")
	prompt := h.readPrimary(t)
	check(t, prompt.Method == "session/prompt", "unexpected native submission: %s", prompt.Method)
	var params map[string]json.RawMessage
	must(t, json.Unmarshal(prompt.Params, &params))
	_, marked := params["_meta"]
	check(t, !marked, "owner prompt carries _meta: %s", prompt.Params)

	admittedAs := func() string {
		h.p.mu.Lock()
		defer h.p.mu.Unlock()
		return h.p.pendingPrompt.nativeID
	}
	h.notify(t, "_x.ai/queue/changed", map[string]any{"runningKind": "parent_agent_message", "runningPromptId": "p-g/1", "runningText": "owner-task"})
	h.barrier(t)
	check(t, admittedAs() == "", "a parent notice admitted the owner's Run")
	h.running(t, "p-g/1", "owner-task")
	h.barrier(t)
	check(t, admittedAs() == "p-g/1", "the ordinary notice did not admit the owner's Run")

	h.answer(t, "p-g/1", "owner-answer")
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, prompt, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "done", "owner Run did not complete")
	check(t, h.status(t, 3, "g/1").Result.Result == "owner-answer", "owner Run output changed")
}

// A delivery acknowledged after its prompt ended waits for Grok's continuation
// prompt. A parent notice with the same text is not that continuation; the
// ordinary notice still is.
func TestParentNoticeNeverClassifiesALateDelivery(t *testing.T) {
	h := newContinuationHarness(t)
	original := h.start(t, 2, "g/1", "owned-first")
	writeWorkerRequest(t, h.bus, 3, "message.deliver", delivery("late-marker"))
	interject := h.expectInterject(t, 3, nil)
	var params struct{ Text string }
	must(t, json.Unmarshal(interject.Params, &params))
	h.answer(t, "p-g/1", "first-answer")
	h.terminal(t, "p-g/1", "cancelled")
	replyACP(t, h.primaryWrite, original, map[string]any{"stopReason": "cancelled", "_meta": map[string]string{"promptId": "p-g/1"}})
	h.barrier(t)
	h.notify(t, "_x.ai/session/interjection", map[string]any{"interjectionId": "message-1"})
	var receipt kit.DeliveryReceipt
	must(t, json.Unmarshal(readWorkerResponse(t, h.bus, 3).Result, &receipt))
	check(t, receipt.Disposition == "injected", "wrong receipt: %+v", receipt)

	h.p.mu.Lock()
	native := h.p.pendingPrompt
	h.p.mu.Unlock()
	check(t, native != nil, "late delivery released its Run")
	waiting := func(id string) bool {
		h.p.mu.Lock()
		defer h.p.mu.Unlock()
		return native.delivery != nil && !native.delivery.classified && !native.hasSegment(id)
	}
	h.notify(t, "_x.ai/queue/changed", map[string]any{"runningKind": "parent_agent_message", "runningPromptId": "parent-message-other", "runningText": params.Text})
	h.barrier(t)
	check(t, waiting("parent-message-other"), "a parent notice classified the late delivery")
	h.running(t, "native-continuation", params.Text)
	h.barrier(t)
	h.p.mu.Lock()
	classified := native.delivery == nil && native.hasSegment("native-continuation")
	h.p.mu.Unlock()
	check(t, classified, "the ordinary notice did not classify the late delivery")

	writeWorkerRequest(t, h.bus, 4, "turn.interrupt", map[string]string{"session_id": testSessionID + "@local"})
	check(t, h.readPrimary(t).Method == "session/cancel", "interrupt did not reach current native work")
	check(t, readWorkerResponse(t, h.bus, 4).Error == nil, "interrupt failed")
	h.terminal(t, "native-continuation", "end_turn")
	replyACP(t, h.primaryWrite, interject, map[string]any{"result": map[string]string{"status": "queued"}})
	check(t, readWorkerReadyID(t, h.bus, "g/1")["state"] == "done", "late delivery Run did not settle")
}
