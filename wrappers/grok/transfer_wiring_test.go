// SPDX-License-Identifier: MIT
package grok

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/testsocket"
)

// This is the production-codec fixture used by the Claude wiring controls,
// adapted to Grok's standard Peer. Only the daemon and native delivery callback
// are simulated. The SDK owns the connection, transfer manager and real files.
type transferWire struct {
	fd     net.Conn
	frames chan protocol.Frame
	writes sync.Mutex
	nextID int64
}

func newTransferWire(t *testing.T, fd net.Conn) *transferWire {
	t.Helper()
	w := &transferWire{fd: fd, frames: make(chan protocol.Frame, 32)}
	t.Cleanup(func() { _ = fd.Close() })
	go func() {
		defer close(w.frames)
		r := bufio.NewReader(fd)
		for {
			b, err := r.ReadBytes('\n')
			if err != nil {
				return
			}
			f, err := protocol.DecodeFrame(b[:len(b)-1])
			if err != nil {
				t.Error(err)
				return
			}
			if f.Request {
				if _, err := protocol.DecodeParams(f.Method, f.Params); err != nil {
					t.Error(err)
					return
				}
			}
			w.frames <- f
		}
	}()
	return w
}
func (w *transferWire) next(t *testing.T) protocol.Frame {
	t.Helper()
	select {
	case f, ok := <-w.frames:
		if !ok {
			t.Fatal("transfer wire closed")
		}
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("transfer wire did not progress")
		return protocol.Frame{}
	}
}
func (w *transferWire) write(t *testing.T, body []byte) {
	t.Helper()
	w.writes.Lock()
	defer w.writes.Unlock()
	if _, err := w.fd.Write(body); err != nil {
		t.Fatal(err)
	}
}
func (w *transferWire) reply(t *testing.T, f protocol.Frame, value any) {
	t.Helper()
	body, err := protocol.ResultBytes(f.ID, f.Method, value)
	if err != nil {
		t.Fatal(err)
	}
	w.write(t, body)
}
func (w *transferWire) request(t *testing.T, method string, params any) int64 {
	t.Helper()
	w.nextID++
	body, err := protocol.RequestBytes(w.nextID, method, params)
	if err != nil {
		t.Fatal(err)
	}
	w.write(t, body)
	return w.nextID
}
func (w *transferWire) machine(t *testing.T, method string, params any) protocol.Frame {
	t.Helper()
	id := w.request(t, method, params)
	f := w.next(t)
	if f.ID != id || f.Error != nil {
		t.Fatalf("%s reply: %+v", method, f)
	}
	if _, err := protocol.DecodeResult(method, f.Result); err != nil {
		t.Fatalf("invalid %s result: %s %v", method, f.Result, err)
	}
	return f
}
func transferBackend(t *testing.T, cwd string) (*PeerBackend, *transferWire, <-chan kit.DeliveryRequest) {
	t.Helper()
	socket := filepath.Join(testsocket.Directory(t), "bus.sock")
	listener, err := net.Listen("unix", socket)
	must(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	t.Setenv(host.SocketEnv, socket)
	accepted := make(chan net.Conn, 1)
	go func() {
		fd, err := listener.Accept()
		if err == nil {
			accepted <- fd
		}
	}()
	deliveries := make(chan kit.DeliveryRequest, 4)
	peer, err := kit.ConnectPeer(kit.PeerIdentity{Product: Product, SessionID: testSessionID, Groups: []string{"test"}, Info: map[string]any{"cwd": cwd}}, func(_ context.Context, identity kit.PeerIdentity, d kit.DeliveryRequest) (kit.DeliveryReceipt, error) {
		if identity.SessionID != testSessionID {
			t.Errorf("foreign owner: %+v", identity)
		}
		deliveries <- d
		return kit.DeliveryReceipt{Disposition: "injected"}, nil
	})
	must(t, err)
	t.Cleanup(func() { peer.Shutdown(); <-peer.Closed() })
	var fd net.Conn
	select {
	case fd = <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("peer did not connect")
	}
	w := newTransferWire(t, fd)
	hello := w.next(t)
	var identity kit.PeerIdentity
	must(t, json.Unmarshal(hello.Params, &identity))
	check(t, hello.Method == "session.hello" && identity.SessionID == testSessionID && identity.Info["sessionbus_transfer"] == float64(1), "transfer hello = %s %s", hello.Method, hello.Params)
	w.reply(t, hello, map[string]any{"capabilities": []string{"transfer/1"}})
	awaitChannel(t, peer.Ready(), "transfer peer admission")
	// Isolate the public Action wiring after admission. The existing native ACP
	// owner, permission and final-disposal suites remain separate and unchanged.
	b := &PeerBackend{peer: peer, cwd: cwd, admitted: true, ready: make(chan struct{}), done: make(chan struct{})}
	close(b.ready)
	b.caller = kit.NewCaller(b.Call)
	return b, w, deliveries
}
func grokTransferDescriptor(id, state string) map[string]any {
	return map[string]any{"id": id, "sender": testSessionID, "receiver": "peer@local", "name": "notes.txt", "created_by": testSessionID, "state": state, "bytes": 0}
}
func grokOffer(t *testing.T, b *PeerBackend, w *transferWire, source, id string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]string{"target": "peer@local", "source": source})
	done := make(chan error, 1)
	go func() { _, err := b.Action(context.Background(), "transfer.offer", raw); done <- err }()
	var f protocol.Frame
	select {
	case f = <-w.frames:
	case err := <-done:
		t.Fatalf("public Action did not use its bound peer caller: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("public transfer Action did not progress")
	}
	var offer map[string]any
	check(t, f.Method == "transfer.offer" && json.Unmarshal(f.Params, &offer) == nil && len(offer) == 2 && offer["target"] == "peer@local" && offer["name"] == filepath.Base(source), "offer leaked/changed local binding: %s %s", f.Method, f.Params)
	w.reply(t, f, grokTransferDescriptor(id, "offered"))
	must(t, <-done)
}
func TestPeerTransferUsesItsBoundCallerAndNativeCwd(t *testing.T) {
	cwd, outside := t.TempDir(), t.TempDir()
	must(t, os.Mkdir(filepath.Join(outside, "nested"), 0700))
	must(t, os.Symlink(filepath.Join(outside, "nested"), filepath.Join(cwd, "hop")))
	must(t, os.WriteFile(filepath.Join(outside, "notes.txt"), []byte("native symlink parent"), 0600))
	must(t, os.WriteFile(filepath.Join(cwd, "notes.txt"), []byte("wrong"), 0600))
	b, w, _ := transferBackend(t, cwd)
	grokOffer(t, b, w, "hop/../notes.txt", "offered-one")
	f := w.machine(t, "transfer.prepare", map[string]any{"id": "offered-one", "role": "source"})
	var prepared struct {
		Size *int64 `json:"size"`
	}
	must(t, json.Unmarshal(f.Result, &prepared))
	check(t, prepared.Size != nil && *prepared.Size == int64(len("native symlink parent")), "native path was cleaned/rebased: %s", f.Result)
	// Ordinary actions still use that same admitted connection.
	done := make(chan error, 1)
	go func() { _, err := b.Action(context.Background(), "list", json.RawMessage(`{}`)); done <- err }()
	f = w.next(t)
	check(t, f.Method == "session.list", "ordinary method = %s", f.Method)
	w.reply(t, f, kit.SessionListResult{Sessions: []kit.SessionSummary{}})
	must(t, <-done)
}
func TestPeerTransferUnknownBaseAndCaseAliasesAreRefused(t *testing.T) {
	b, w, _ := transferBackend(t, "")
	if _, err := b.Action(context.Background(), "transfer.offer", json.RawMessage(`{"target":"peer@local","source":"notes.txt"}`)); err == nil || !strings.Contains(err.Error(), "absolute path") {
		t.Fatalf("unknown native cwd: %v", err)
	}
	if _, err := b.Action(context.Background(), "transfer.offer", json.RawMessage(`{"target":"peer@local","Source":"notes.txt"}`)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("path alias: %v", err)
	}
	absolute := filepath.Join(t.TempDir(), "notes.txt")
	must(t, os.WriteFile(absolute, []byte("absolute"), 0600))
	grokOffer(t, b, w, absolute, "absolute-one")
	b.mu.Lock()
	b.ending = true
	b.mu.Unlock()
	if _, err := b.Action(context.Background(), "transfer.offer", json.RawMessage(`{"target":"peer@local","source":"notes.txt"}`)); err == nil || err.Error() != "Grok peer is unavailable" {
		t.Fatalf("ended peer: %v", err)
	}
}
func TestPeerTransferReceivesAndProjectsThroughItsExistingDelivery(t *testing.T) {
	cwd := t.TempDir()
	b, w, deliveries := transferBackend(t, cwd)
	body := []byte("received native bytes")
	digest := sha256.Sum256(body)
	sum := hex.EncodeToString(digest[:])
	from := kit.DeliverySource{SessionID: "peer@local", Product: "fixture", Groups: []string{}}
	w.machine(t, "transfer.notice", map[string]any{"kind": "offer", "id": "received", "name": "out.bin", "from": from, "size": len(body)})
	done := make(chan error, 1)
	go func() {
		_, err := b.Action(context.Background(), "transfer.accept", json.RawMessage(`{"id":"received","destination":"out.bin"}`))
		done <- err
	}()
	accepted := w.next(t)
	check(t, accepted.Method == "transfer.accept" && string(accepted.Params) == `{"id":"received"}`, "accept leaked path: %s %s", accepted.Method, accepted.Params)
	descriptor := grokTransferDescriptor("received", "preparing")
	descriptor["sender"], descriptor["receiver"], descriptor["created_by"] = "peer@local", testSessionID, "peer@local"
	w.reply(t, accepted, descriptor)
	must(t, <-done)
	for _, step := range []struct {
		method string
		params map[string]any
	}{
		{"transfer.prepare", map[string]any{"id": "received", "role": "destination", "size": len(body)}},
		{"transfer.start", map[string]any{"id": "received", "role": "destination", "size": len(body)}},
		{"transfer.chunk", map[string]any{"id": "received", "offset": 0, "data": base64.StdEncoding.EncodeToString(body)}},
		{"transfer.finish", map[string]any{"id": "received", "bytes": len(body), "sha256": sum}},
	} {
		w.machine(t, step.method, step.params)
	}
	path := filepath.Join(cwd, "out.bin")
	got, err := os.ReadFile(path)
	check(t, err == nil && string(got) == string(body), "native destination = %q %v", got, err)
	terminal := map[string]any{"kind": "terminal", "id": "received", "outcome": "completed", "bytes": len(body), "sha256": sum}
	raw, _ := json.Marshal(terminal)
	terminalID := w.request(t, "message.deliver", kit.DeliveryRequest{MessageID: "transfer-notice-received", From: from, Body: string(raw)})
	laterID := w.request(t, "message.deliver", kit.DeliveryRequest{MessageID: "later", From: from, Body: "later"})
	f := w.next(t)
	check(t, f.ID == laterID && f.Error == nil, "reader blocked behind local projection: %+v", f)
	machineID := w.request(t, "transfer.notice", terminal)
	replies := map[int64]protocol.Frame{}
	for len(replies) < 2 {
		f := w.next(t)
		replies[f.ID] = f
	}
	for _, id := range []int64{terminalID, machineID} {
		f, ok := replies[id]
		check(t, ok && f.Error == nil, "terminal reply %d: %+v", id, f)
	}
	first, last := <-deliveries, <-deliveries
	check(t, first.MessageID == "later" && last.MessageID == "transfer-notice-received" && strings.Contains(last.Body, `"destination":"`+path+`"`), "projected delivery = %+v %+v", first, last)
}
func TestLaneTransferRequiresItsOpenedNativeBase(t *testing.T) {
	p := New(filepath.Join(testsocket.Directory(t), "bus.sock"), "transfer")
	p.sessionID = "native"
	p.caller = kit.NewCaller(func(context.Context, string, any) (json.RawMessage, error) {
		t.Fatal("unexpected generic transfer call")
		return nil, nil
	})
	o := &laneToolOwner{endpoint: &grokEndpoint{owner: p}, native: nativeHelperIdentity{"native"}}
	raw := json.RawMessage(`{"target":"peer@local","source":"notes.txt"}`)
	if _, err := o.Action(context.Background(), "transfer.offer", raw); err == nil || !strings.Contains(err.Error(), "absolute path") {
		t.Fatalf("unknown lane base: %v", err)
	}
	p.cwd = "/native/lane"
	if _, err := o.Action(context.Background(), "transfer.offer", raw); !errors.Is(err, kit.ErrTransferUnsupported) {
		t.Fatalf("bound lane did not reach SDK: %v", err)
	}
	p.closing = true
	if _, err := o.Action(context.Background(), "transfer.offer", raw); err == nil {
		t.Fatal("closed lane accepted transfer")
	}
}
