// SPDX-License-Identifier: MIT
package grok

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	kit "github.com/antst/sessionbus/bus/sdk/go"
)

type continuationACPRead struct {
	frame acpFrame
	raw   json.RawMessage
	err   error
}

type continuationDeliveryResult struct {
	receipt kit.DeliveryReceipt
	err     error
}

// Each stream has exactly one decoder owner. The one-element channels allow
// the test to select native submission against an early delivery result without
// abandoning a read or racing a later status/receipt reader. Cleanup must close
// the actual streams after stop and before join. Native is decoded only when
// the test asks for a frame: until then an unread frame keeps the wrapper's
// write blocked, as the unread pipe itself would.
func continuationReaders(bus *workerReader, native *json.Decoder) (<-chan continuationACPRead, chan<- struct{}, func(), func()) {
	stop := make(chan struct{})
	busDone, nativeDone := make(chan struct{}), make(chan struct{})
	busFrames := make(chan workerRead, 1)
	nativeFrames, nativeWant := make(chan continuationACPRead, 1), make(chan struct{}, 1)
	bus.frames = busFrames
	go func() {
		defer close(busDone)
		defer close(busFrames)
		for {
			var next workerRead
			line, err := bus.reader.ReadBytes('\n')
			next.err = err
			if err == nil {
				next.err = json.Unmarshal(line, &next.response)
			}
			select {
			case busFrames <- next:
			case <-stop:
				return
			}
			if next.err != nil {
				return
			}
		}
	}()
	go func() {
		defer close(nativeDone)
		defer close(nativeFrames)
		for {
			select {
			case <-nativeWant:
			case <-stop:
				return
			}
			var next continuationACPRead
			if next.err = native.Decode(&next.raw); next.err == nil {
				next.err = json.Unmarshal(next.raw, &next.frame)
			}
			select {
			case nativeFrames <- next:
			case <-stop:
				return
			}
			if next.err != nil {
				return
			}
		}
	}()
	return nativeFrames, nativeWant, func() { close(stop) }, func() { <-busDone; <-nativeDone }
}

// primaryFrame asks the stream's reader for the next native frame, unless an
// earlier ask is still open, and returns where that frame arrives.
func (h *continuationHarness) primaryFrame() <-chan continuationACPRead {
	if !h.primaryAsked {
		h.primaryAsked = true
		h.primaryWant <- struct{}{}
	}
	return h.primaryRead
}

func (h *continuationHarness) nextPrimary(t *testing.T) continuationACPRead {
	t.Helper()
	next, ok := <-h.primaryFrame()
	h.primaryAsked = false
	if !ok {
		t.Fatal("native frame reader closed")
	}
	must(t, next.err)
	return next
}

func (h *continuationHarness) readPrimary(t *testing.T) acpFrame {
	t.Helper()
	return h.nextPrimary(t).frame
}

func (h *continuationHarness) expectInterject(t *testing.T, deliveryID int, direct <-chan continuationDeliveryResult) acpFrame {
	t.Helper()
	f, err := h.interjectOrDelivery(t, deliveryID, direct)
	must(t, err)
	check(t, f.Method == "_x.ai/interject", "not native interject: %+v", f)
	return f
}

// All bus maps remain owned by the calling test goroutine. Responses for other
// requests and turn.ready notifications keep the existing correlation/ack path.
func (h *continuationHarness) interjectOrDelivery(t *testing.T, deliveryID int, direct <-chan continuationDeliveryResult) (acpFrame, error) {
	t.Helper()
	if response, ok := h.bus.pending[deliveryID]; deliveryID != 0 && ok {
		return acpFrame{}, earlyWorkerDelivery(response)
	}
	for {
		select {
		case next, ok := <-h.primaryFrame():
			h.primaryAsked = false
			if !ok {
				return acpFrame{}, fmt.Errorf("native closed before interject: %w", io.EOF)
			}
			return next.frame, next.err
		case next, ok := <-h.bus.frames:
			if !ok {
				return acpFrame{}, fmt.Errorf("worker closed before interject: %w", io.EOF)
			}
			if next.err != nil {
				return acpFrame{}, fmt.Errorf("worker read before interject: %w", next.err)
			}
			response := next.response
			if workerReady(t, h.bus, response) {
				continue
			}
			response.ID = h.bus.requestIDs[response.ID]
			h.bus.pending[response.ID] = response
			if deliveryID != 0 && response.ID == deliveryID {
				return acpFrame{}, earlyWorkerDelivery(response)
			}
		case result := <-direct:
			return acpFrame{}, fmt.Errorf("Deliver returned before native interject: receipt=%+v error=%v", result.receipt, result.err)
		}
	}
}

func earlyWorkerDelivery(response workerResponse) error {
	return fmt.Errorf("Worker delivery %d returned before native interject: result=%s error=%s", response.ID, response.Result, response.Error)
}

func TestContinuationDiagnosticsSurfaceEarlyWorkerDelivery(t *testing.T) {
	h := newContinuationHarness(t)
	// The actual Worker has no Run. Its NotRunning response must diagnose
	// a test expecting interject instead of stranding the native read.
	writeWorkerRequest(t, h.bus, 2, "turn.status", map[string]string{"session_id": testSessionID + "@local"})
	writeWorkerRequest(t, h.bus, 3, "message.deliver", delivery("idle"))
	_, err := h.interjectOrDelivery(t, 3, nil)
	check(t, err != nil && strings.Contains(err.Error(), `"code":-32004`), "missing actual Worker result: %v", err)
	status := readWorkerResponse(t, h.bus, 2)
	check(t, status.ID == 2 && status.Error != nil, "missing-Run status error lost: %+v", status)
	response := readWorkerResponse(t, h.bus, 3)
	check(t, response.Error != nil && strings.Contains(string(response.Error), `"code":-32004`), "diagnostic consumed original error: %+v", response)
}

func TestContinuationDiagnosticsSurfaceEarlyDirectDelivery(t *testing.T) {
	h := newContinuationHarness(t)
	returned := make(chan continuationDeliveryResult, 1)
	go func() {
		r, err := h.p.Deliver(context.Background(), delivery("idle"), nil)
		returned <- continuationDeliveryResult{r, err}
	}()
	_, err := h.interjectOrDelivery(t, 0, returned)
	check(t, err != nil && strings.Contains(err.Error(), "not_running"), "missing actual direct result: %v", err)
}

func TestContinuationDiagnosticsObserveNativeFrame(t *testing.T) {
	h := newContinuationHarness(t)
	done := make(chan error, 1)
	go func() { done <- h.p.primary.request(context.Background(), "_x.ai/interject", nil, nil) }()
	f := h.expectInterject(t, 0, nil)
	replyACP(t, h.primaryWrite, f, map[string]any{})
	must(t, <-done)
}

func TestContinuationDiagnosticsObserveNativeEOF(t *testing.T) {
	h := newContinuationHarness(t)
	h.p.primary.close()
	_, err := h.interjectOrDelivery(t, 0, nil)
	check(t, err != nil, "native EOF accepted as interject")
}
