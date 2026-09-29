// SPDX-License-Identifier: MIT
package grok

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestReviewGrokReadinessSettlesOnPrimaryEOF(t *testing.T) {
	requestRead, requestWrite := io.Pipe()
	responseRead, responseWrite := io.Pipe()
	defer requestRead.Close()
	p := &Wrapper{}
	p.primary = newACPClient(acpPrimary, requestWrite, responseRead, nil)
	defer p.primary.close()
	endpoint := &grokEndpoint{owner: p, ready: make(chan struct{})}
	responseWrite.Close()
	<-p.primary.done // Actual ACP reader observed native EOF; connection stays unready.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := endpoint.waitReady(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("native death did not settle Open readiness: %v", err)
	}
}

func TestReadyHelperLossPreventsOpenCommit(t *testing.T) {
	requestRead, requestWrite := io.Pipe()
	responseRead, responseWrite := io.Pipe()
	defer requestRead.Close()
	defer responseWrite.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &Wrapper{ctx: ctx, cancel: cancel, nativeFailed: make(chan struct{})}
	p.primary = newACPClient(acpPrimary, requestWrite, responseRead, nil)
	defer p.primary.close()
	endpoint := &grokEndpoint{owner: p, ready: make(chan struct{})}
	helper := &laneToolOwner{endpoint: endpoint}
	helper.Initialized()
	if err := endpoint.waitReady(ctx); err != nil {
		t.Fatal(err)
	}
	helper.End() // Synchronously observed native helper loss, before Open commits.
	if err := p.commitOpen(ctx, func() bool { return true }); err == nil {
		t.Fatal("Open adopted a dead helper")
	}
	if p.opened {
		t.Fatal("failed adoption published Open")
	}
	if err := endpoint.waitReady(ctx); err == nil {
		t.Fatal("stale readiness accepted")
	}
}
