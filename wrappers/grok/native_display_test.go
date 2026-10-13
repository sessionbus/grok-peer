// SPDX-License-Identifier: MIT
package grok

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeRunningTextMatchesSingleTextBlockProjection(t *testing.T) {
	for _, test := range []struct {
		name, submitted, running string
		want                     bool
	}{
		{"exact", "text", "text", true},
		{"ascii-edges", "\t text \r\n", "text", true},
		{"unicode-edges", "\u00a0\u2003text\u2003\u00a0", "text", true},
		{"internal-whitespace", " text  body ", "text body", false},
		{"wrong-text", " text ", "other", false},
		{"zero-width-space-is-content", "\u200btext\u200b", "text", false},
		{"bom-is-content", "\ufefftext\ufeff", "text", false},
		{"native-display-must-be-projected", " text ", " text ", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := nativeRunningTextMatches(test.submitted, test.running); got != test.want {
				t.Fatalf("nativeRunningTextMatches(%q, %q) = %t, want %t", test.submitted, test.running, got, test.want)
			}
		})
	}
}

func TestInitialPromptAdmissionUsesNativeTrimmedDisplay(t *testing.T) {
	h := newContinuationHarness(t)
	raw := "\u00a0\u2003Reply  with internal spacing\nthen finish\u2003\u00a0"
	writeWorkerRequest(t, h.bus, 2, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/1", "input": raw})
	check(t, readWorkerResponse(t, h.bus, 2).Error == nil, "execute refused")
	prompt := h.readPrimary(t)
	var params struct {
		Prompt []struct{ Text string }
	}
	must(t, json.Unmarshal(prompt.Params, &params))
	check(t, len(params.Prompt) == 1 && params.Prompt[0].Text == raw, "wire prompt changed: %#v", params.Prompt)

	h.running(t, "p-g/1", strings.TrimSpace(raw))
	h.answer(t, "p-g/1", "trimmed-display-answer")
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, prompt, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	ready := readWorkerReadyID(t, h.bus, "g/1")
	check(t, ready["state"] == "done", "trimmed native display did not admit prompt: %#v", ready)
	status := h.status(t, 3, "g/1")
	check(t, status.Result != nil && status.Result.Result == "trimmed-display-answer", "wrong retained result: %+v", status)
}

func TestInitialPromptAdmissionKeepsInternalWhitespaceStrict(t *testing.T) {
	h := newContinuationHarness(t)
	raw := " edge  spacing\n"
	writeWorkerRequest(t, h.bus, 2, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/1", "input": raw})
	check(t, readWorkerResponse(t, h.bus, 2).Error == nil, "execute refused")
	prompt := h.readPrimary(t)

	// Native trims only the edges; collapsing the two internal spaces must not
	// bind an otherwise valid prompt ID or terminal to this run.
	h.running(t, "p-g/1", "edge spacing")
	h.terminal(t, "p-g/1", "end_turn")
	replyACP(t, h.primaryWrite, prompt, map[string]any{"stopReason": "end_turn", "_meta": map[string]string{"promptId": "p-g/1"}})
	ready := readWorkerReadyID(t, h.bus, "g/1")
	check(t, ready["state"] == "unavailable", "internal whitespace mismatch was admitted: %#v", ready)
	reason, _ := ready["reason"].(string)
	check(t, strings.Contains(reason, "matching native admission"), "wrong unavailable reason: %#v", ready)
}

func TestContinuationAdmissionUsesNativeTrimmedDisplay(t *testing.T) {
	for _, test := range []struct {
		name, running string
		want          bool
	}{
		{"trimmed-edges", "continuation  body", true},
		{"internal-mismatch", "continuation body", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			gate := make(chan struct{}, 1)
			p := &Wrapper{sessionID: "native", deliveryGate: gate}
			d := &nativeDelivery{
				id: "message-1", text: "\u00a0continuation  body\u2003\n",
				attempted: true, acked: true, admitted: make(chan struct{}), settled: make(chan struct{}),
			}
			prompt := &nativePrompt{
				owner: p, sessionID: "native", promptText: "original", nativeID: "original", attempted: true,
				changed: make(chan struct{}), admitted: make(chan struct{}), done: make(chan struct{}), delivery: d,
				segments: []*nativeSegment{{id: "original", terminal: true}},
			}
			p.pendingPrompt = prompt
			params, err := json.Marshal(map[string]any{
				"sessionId": "native", "runningPromptId": "continued", "runningText": test.running, "runningKind": "prompt",
			})
			must(t, err)
			p.receiveQueue(acpFrame{JSONRPC: "2.0", Method: "_x.ai/queue/changed", Params: params})
			if test.want {
				check(t, prompt.delivery == nil && len(prompt.segments) == 2 && prompt.segments[1].id == "continued", "trimmed continuation was not classified")
				select {
				case <-d.settled:
				default:
					t.Fatal("trimmed continuation did not settle delivery ownership")
				}
				return
			}
			check(t, prompt.delivery == d && len(prompt.segments) == 1, "internal whitespace mismatch classified continuation")
			select {
			case <-d.settled:
				t.Fatal("internal whitespace mismatch settled delivery ownership")
			default:
			}
		})
	}
}
