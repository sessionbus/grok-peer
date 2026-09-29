// SPDX-License-Identifier: MIT
package grok

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"
)

func reviewPrompt(t *testing.T) (*Wrapper, *nativePrompt, *json.Decoder, *json.Encoder, acpFrame) {
	t.Helper()
	rr, rw := io.Pipe()
	sr, sw := io.Pipe()
	p := &Wrapper{sessionID: "native"}
	p.primary = newACPClient(acpPrimary, rw, sr, p.receive)
	t.Cleanup(func() { p.primary.close(); rr.Close(); sw.Close() })
	decoder, encoder := json.NewDecoder(rr), json.NewEncoder(sw)
	started := make(chan *nativePrompt, 1)
	go func() {
		prompt, e := p.startPrompt(context.Background(), p.primary, "native", "owned text")
		if e != nil {
			started <- nil
			return
		}
		started <- prompt
	}()
	request := readACP(t, decoder)
	prompt := <-started
	if prompt == nil {
		t.Fatal("start failed")
	}
	return p, prompt, decoder, encoder, request
}

func TestReviewGrokSeedRefusalWithoutAdmissionSettles(t *testing.T) {
	_, prompt, _, encoder, request := reviewPrompt(t)
	if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": -32602, "message": "native refuses prompt"}}); err != nil {
		t.Fatal(err)
	}
	// Only the test has a deadline: native refuses the prompt but keeps its ACP connection open.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := prompt.admission(ctx)
	var native *acpError
	if !errors.As(err, &native) {
		t.Fatalf("completed native refusal did not settle admission: %v", err)
	}
}

func TestReviewGrokReplayCannotBindAdmission(t *testing.T) {
	p, prompt, decoder, encoder, _ := reviewPrompt(t)
	if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/queue/changed", "params": map[string]any{"sessionId": "native", "runningPromptId": "historic", "runningText": "owned text", "runningKind": "prompt", "_meta": map[string]any{"isReplay": true}}}); err != nil {
		t.Fatal(err)
	}
	barrier := make(chan error, 1)
	go func() { barrier <- p.primary.request(context.Background(), "barrier", nil, nil) }()
	replyACP(t, encoder, readACP(t, decoder), map[string]any{})
	if err := <-barrier; err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	id := prompt.nativeID
	p.mu.Unlock()
	if id != "" {
		t.Fatalf("replayed running frame was accepted as live admission: %s", id)
	}
}
