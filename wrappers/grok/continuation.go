// SPDX-License-Identifier: MIT
package grok

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

type nativeSegment struct {
	id, reason string
	terminal   bool
}
type nativeDelivery struct {
	id, text                     string
	attempted, acked, classified bool
	admitted, settled            chan struct{}
	err                          error
}

// All state below is protected by owner.mu. Signals carry no result bodies.
func (t *nativePrompt) signal() { close(t.changed); t.changed = make(chan struct{}) }
func (t *nativePrompt) hasSegment(id string) bool {
	for _, s := range t.segments {
		if s.id == id {
			return true
		}
	}
	return false
}
func (t *nativePrompt) releaseDelivery(d *nativeDelivery) {
	if t.delivery != d {
		return
	}
	t.delivery = nil
	close(d.settled)
	t.owner.deliveryGate <- struct{}{}
	t.signal()
}
func (p *Wrapper) receiveLifecycle(f acpFrame) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t := p.pendingPrompt
	if t == nil {
		return
	}
	switch f.Method {
	case "_x.ai/session/interjection":
		var n interjectionNotice
		if json.Unmarshal(f.Params, &n) != nil || n.SessionID != t.sessionID {
			return
		}
		d := t.delivery
		if d == nil || !d.attempted || d.id != n.InterjectionID || d.acked {
			return
		}
		d.acked = true
		close(d.admitted)
		if len(t.segments) > 0 && !t.segments[len(t.segments)-1].terminal {
			d.classified = true
			t.releaseDelivery(d)
		}
	case "_x.ai/session_notification":
		var n struct {
			SessionID string `json:"sessionId"`
			Update    struct {
				Kind   string `json:"sessionUpdate"`
				ID     string `json:"prompt_id"`
				Reason string `json:"stop_reason"`
			} `json:"update"`
		}
		if json.Unmarshal(f.Params, &n) != nil || n.SessionID != t.sessionID || n.Update.Kind != "turn_completed" {
			return
		}
		for _, s := range t.segments {
			if s.id == n.Update.ID && !s.terminal {
				s.terminal = true
				s.reason = n.Update.Reason
				t.signal()
				return
			}
		}
	}
}
func (t *nativePrompt) waitOwned(ctx context.Context) (kit.TurnResult, error) {
	select {
	case <-t.done:
	case <-ctx.Done():
		return kit.TurnResult{}, ctx.Err()
	}
	if t.err != nil {
		return kit.TurnResult{}, t.err
	}
	p := t.owner
	p.mu.Lock()
	if t.nativeID == "" || t.nativeID != t.result.Meta.PromptID {
		p.mu.Unlock()
		return kit.TurnResult{}, errors.New("Grok terminal lacks matching native admission")
	}
	if len(t.segments) == 0 || !t.segments[0].terminal {
		p.mu.Unlock()
		return kit.TurnResult{}, errors.New("Grok prompt response lacks ordered native terminal")
	}
	for {
		if t.failure != nil {
			e := t.failure
			p.mu.Unlock()
			t.abortAccounting(e)
			return kit.TurnResult{}, e
		}
		complete := t.delivery == nil
		for _, s := range t.segments {
			complete = complete && s.terminal
		}
		if complete && t.interrupt != nil {
			select {
			case <-t.interrupt.done:
			default:
				complete = false
			}
		}
		if complete {
			t.retiring = true
			result := kit.TurnResult{Outcome: "completed", NativeStopReason: t.result.StopReason}
			parts := make([]string, 0, len(t.segments))
			for _, s := range t.segments {
				if s.id == t.nativeID && s.reason != t.result.StopReason {
					p.mu.Unlock()
					return kit.TurnResult{}, errors.New("Grok terminal reasons disagree")
				}
				if result.Outcome == "completed" {
					result.NativeStopReason = s.reason
					if s.reason == "cancelled" {
						result.Outcome = "interrupted"
					} else if s.reason != "end_turn" {
						result.Outcome = "failed"
					}
				}
				if b := p.answers[s.id]; b != nil {
					parts = append(parts, b.String())
				} else {
					parts = append(parts, "")
				}
			}
			result.Result = strings.Join(parts, "\n")
			if len(result.Result) > maxACPFrame {
				p.mu.Unlock()
				return kit.TurnResult{}, errors.New("Grok output exceeds retention limit")
			}
			p.mu.Unlock()
			return result, nil
		}
		changed := t.changed
		p.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return kit.TurnResult{}, ctx.Err()
		case <-t.client.done:
			// A final primary notification wins transport EOF only if all native work
			// was already accounted for; inspect once more before returning unavailable.
			p.mu.Lock()
			complete = t.delivery == nil
			for _, s := range t.segments {
				complete = complete && s.terminal
			}
			p.mu.Unlock()
			if !complete {
				return kit.TurnResult{}, errors.New("Grok native continuation ended without terminal")
			}
		}
		p.mu.Lock()
	}
}
func (p *Wrapper) deliverActive(ctx context.Context, r kit.DeliveryRequest, text string) (kit.DeliveryReceipt, error) {
	p.mu.Lock()
	if p.deliveryGate == nil {
		p.deliveryGate = make(chan struct{}, 1)
		p.deliveryGate <- struct{}{}
	}
	gate := p.deliveryGate
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return kit.DeliveryReceipt{}, ctx.Err()
	case <-gate:
	}
	p.mu.Lock()
	t, observer := p.pendingPrompt, p.observer
	if p.closing || p.nativeFailure != nil || t == nil || observer == nil || t.retiring {
		p.mu.Unlock()
		gate <- struct{}{}
		return kit.DeliveryReceipt{}, host.NotRunning()
	}
	p.mu.Unlock()
	// The primary stream is the admission authority. The observer must never
	// submit an interject into native idle while this prompt is still starting.
	if err := t.admission(ctx); err != nil {
		gate <- struct{}{}
		if ctx.Err() != nil {
			return kit.DeliveryReceipt{}, ctx.Err()
		}
		return kit.DeliveryReceipt{}, host.NotRunning()
	}
	p.mu.Lock()
	if p.pendingPrompt != t || p.closing || p.nativeFailure != nil || t.retiring || len(t.segments) == 0 || t.segments[len(t.segments)-1].terminal {
		p.mu.Unlock()
		gate <- struct{}{}
		return kit.DeliveryReceipt{}, host.NotRunning()
	}
	d := &nativeDelivery{id: r.MessageID, text: text, admitted: make(chan struct{}), settled: make(chan struct{})}
	t.delivery = d
	t.signal()
	p.mu.Unlock()
	// Before submission the caller may cancel this RPC. After submission only
	// the owned Run lifetime cancels it; caller departure never erases accounting.
	rpcCtx, cancel := context.WithCancel(t.ctx)
	stop := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		if !d.attempted {
			cancel()
		}
		p.mu.Unlock()
	})
	go func() {
		defer cancel()
		defer stop()
		var result struct {
			Result struct {
				Status string `json:"status"`
			} `json:"result"`
		}
		err := observer.requestSubmitting(rpcCtx, "_x.ai/interject", map[string]string{"sessionId": t.sessionID, "interjectionId": d.id, "text": d.text}, &result, nil, func() error {
			p.mu.Lock()
			defer p.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return err
			}
			if p.pendingPrompt != t || t.delivery != d || p.closing || p.nativeFailure != nil || t.retiring || len(t.segments) == 0 || t.segments[len(t.segments)-1].terminal {
				return host.NotRunning()
			}
			d.attempted = true
			stop()
			return nil
		})
		p.mu.Lock()
		abort := false
		var refused *acpError
		definite := !d.attempted || errors.As(err, &refused) || err == nil && result.Result.Status != "queued"
		if definite && !d.acked {
			d.err = err
			if d.err == nil {
				d.err = errors.New("Grok interjection refused")
			}
			t.releaseDelivery(d)
		} else if err != nil && !d.acked {
			d.err = fmt.Errorf("uncertain_native_admission: %w", err)
			t.failure = d.err
			abort = true
			t.releaseDelivery(d)
		}
		p.mu.Unlock()
		if abort {
			t.abortAccounting(d.err)
		}
		// A queued RPC is not scheduling admission. Keep the sole gate until the
		// PRIMARY actor/running event classifies it or the owned connection ends.
		select {
		case <-d.settled:
			return
		case <-t.client.done:
		case <-rpcCtx.Done():
		case <-observer.done:
		}
		p.mu.Lock()
		abort = t.delivery == d
		if t.delivery == d {
			d.err = errors.New("Grok interjection scheduling unavailable")
			t.failure = d.err
			t.releaseDelivery(d)
		}
		p.mu.Unlock()
		if abort {
			t.abortAccounting(d.err)
		}
	}()
	select {
	case <-d.admitted:
		return kit.DeliveryReceipt{Disposition: "injected"}, nil
	case <-d.settled:
	case <-ctx.Done():
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if d.acked {
		return kit.DeliveryReceipt{Disposition: "injected"}, nil
	}
	if err := ctx.Err(); err != nil {
		return kit.DeliveryReceipt{}, err
	}
	return kit.DeliveryReceipt{}, d.err
}

// Accounting loss retires the integration; it must not expose an idle Worker
// while native work whose completion is unknown continues behind it.
func (t *nativePrompt) abortAccounting(err error) { t.owner.pendingFailure(err) }
func (p *Wrapper) pendingFailure(err error) {
	p.mu.Lock()
	if p.nativeFailure == nil {
		p.nativeFailure = err
		if p.nativeFailed != nil {
			close(p.nativeFailed)
		}
	}
	primary, child, run, shutdown := p.primary, p.child, p.run, p.shutdown
	p.mu.Unlock()
	p.lossOnce.Do(func() {
		if primary != nil {
			primary.finish(err)
		}
		if child != nil {
			_ = child.cmd.Process.Kill()
		}
		if shutdown != nil {
			go func() {
				if run != nil {
					<-run.Done()
				}
				shutdown()
			}()
		}
	})
}

// Errors may end the shared callback only after outstanding native ownership
// has ended or been aborted. A definite refused prompt needs no forced abort.
func (t *nativePrompt) failOwned(err error) {
	p := t.owner
	p.mu.Lock()
	t.retiring = true
	operation := t.interrupt
	unsettled := t.delivery != nil || t.failure != nil
	if operation != nil {
		select {
		case <-operation.done:
		default:
			unsettled = true
		}
	}
	for _, s := range t.segments {
		unsettled = unsettled || !s.terminal
	}
	if len(t.segments) == 0 && t.attempted {
		known := false
		select {
		case <-t.done:
			var refused *acpError
			known = errors.As(t.err, &refused) || t.err == nil && t.result.Meta.PromptID != ""
		default:
		}
		unsettled = unsettled || !known
	}
	p.mu.Unlock()
	if unsettled {
		t.abortAccounting(err)
	}
	if operation != nil {
		<-operation.done
	}
}
