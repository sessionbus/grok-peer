// SPDX-License-Identifier: MIT
package grok

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/sessionbus/peer-common/host"
)

type PeerBackend struct {
	mu                               sync.Mutex
	identity                         kit.PeerIdentity
	leader, cwd, socket, initialName string
	ctx                              context.Context
	cancel                           context.CancelFunc
	peer                             *kit.Peer
	caller                           *kit.Caller
	observer                         *acpClient
	process                          *nativeProcess
	ready, done                      chan struct{}
	changed                          chan struct{}
	deliveryGate                     chan struct{}
	initialized                      sync.Once
	work                             sync.WaitGroup
	admitted, ending                 bool
	err                              error
	slots                            chan struct{}
}

func NewPeerBackend(ctx context.Context, env []string) (*PeerBackend, error) {
	if !ManagedHelper(env) {
		return nil, errors.New("Grok peer identity is unavailable; start Grok with grok-peer")
	}
	groups := []string{}
	if raw := environmentValue(env, host.GroupsEnv); raw != "" {
		if json.Unmarshal([]byte(raw), &groups) != nil || groups == nil {
			return nil, errors.New("SESSIONBUS_GROUPS must be a JSON array")
		}
	}
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(ctx)
	b := &PeerBackend{identity: kit.PeerIdentity{Protocol: 1, Product: Product, SessionID: environmentValue(env, grokSessionIDEnv), Groups: groups, Info: map[string]any{"cwd": cwd}}, leader: environmentValue(env, grokLeaderSocketEnv), cwd: cwd, socket: first(environmentValue(env, host.SocketEnv), kit.Socket()), ctx: lifetime, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{}), changed: make(chan struct{}, 1), deliveryGate: make(chan struct{}, 1), slots: make(chan struct{}, maxACPPending)}
	b.deliveryGate <- struct{}{}
	if name := environmentValue(env, host.NameEnv); name != "" {
		won, e := claimInitialName(b.leader)
		if e != nil {
			cancel()
			return nil, e
		}
		if won {
			b.initialName = name
		}
	}
	b.caller = kit.NewCaller(b.Call)
	return b, nil
}
func claimInitialName(leader string) (bool, error) {
	dir := filepath.Dir(leader)
	info, err := os.Lstat(dir)
	if err != nil {
		return false, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return false, errors.New("Grok launch name claim requires a private runtime directory")
	}
	file, err := os.OpenFile(filepath.Join(dir, "initial-name.claim"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, file.Close()
}

// Native ACP must not run before Grok receives its MCP initialize response.
func (b *PeerBackend) Initialized() { b.initialized.Do(func() { go b.run() }) }
func (b *PeerBackend) run() {
	defer close(b.done)
	defer func() {
		b.mu.Lock()
		b.ending = true
		peer, o, process := b.peer, b.observer, b.process
		b.mu.Unlock()
		b.cancel()
		if peer != nil {
			peer.Shutdown()
			<-peer.Closed()
		}
		b.work.Wait()
		stopPeerClient(o, process)
	}()
	err := b.runOwner()
	b.mu.Lock()
	if b.err == nil {
		b.err = err
	}
	b.mu.Unlock()
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "sessionbus: Grok helper:", err)
	}
}
func (b *PeerBackend) runOwner() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	observer, process, err := startPeerClient(b.ctx, b.ctx, b.leader, b.cwd, func(frame acpFrame) {
		if !replayFrame(frame) && frame.Method == "_x.ai/sessions/changed" {
			select {
			case b.changed <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.observer, b.process = observer, process
	ending := b.ending
	b.mu.Unlock()
	if ending {
		return context.Canceled
	}
	var row peerSession
	for {
		row, err = roster(b.ctx, observer, b.identity.SessionID)
		if err == nil {
			break
		}
		if !errors.Is(err, errNoLeader) {
			return err
		}
		select {
		case <-b.changed:
		case <-observer.done:
			return errors.New("native actor ended before publication")
		case <-b.ctx.Done():
			return b.ctx.Err()
		}
	}
	if b.initialName != "" {
		var result struct {
			Success bool `json:"success"`
		}
		if err = observer.request(b.ctx, "_x.ai/session/rename", map[string]string{"sessionId": b.identity.SessionID, "title": b.initialName}, &result); err != nil {
			return err
		}
		if !result.Success {
			return errors.New("native initial rename failed")
		}
		row, err = roster(b.ctx, observer, b.identity.SessionID)
		if err != nil {
			return err
		}
		if row.Title != b.initialName {
			return errors.New("native initial title does not match requested name")
		}
	}
	if actual := kit.Socket(); actual != b.socket {
		return fmt.Errorf("Sessionbus socket changed before Grok peer startup: %q != %q", actual, b.socket)
	}
	b.mu.Lock()
	b.identity.Name = row.Title
	b.identity.Info = map[string]any{"cwd": row.Cwd}
	b.cwd = row.Cwd
	identity := b.identity
	b.mu.Unlock()
	peer, err := kit.ConnectPeer(identity, b.deliverOwned)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.peer = peer
	ending = b.ending
	b.mu.Unlock()
	if ending {
		peer.Shutdown()
		<-peer.Closed()
		return context.Canceled
	}
	select {
	case <-peer.Ready():
	case <-peer.Closed():
		if err = peer.Err(); err == nil {
			err = errors.New("Sessionbus owner connection ended before admission")
		}
		return err
	case <-observer.done:
		return errors.New("native actor ended before publication")
	case <-b.ctx.Done():
		return b.ctx.Err()
	}
	b.mu.Lock()
	b.admitted = true
	b.mu.Unlock()
	close(b.ready)
	for {
		select {
		case <-b.changed:
			row, err = roster(b.ctx, observer, b.identity.SessionID)
			if err != nil {
				return err
			}
			if err = b.publish(peer, row); err != nil {
				return err
			}
		case <-observer.done:
			return errors.New("Grok native observer ended")
		case <-peer.Closed():
			if err = peer.Err(); err != nil {
				return err
			}
			if err = b.ctx.Err(); err != nil {
				return err
			}
			return errors.New("Sessionbus peer ended without a terminal reason")
		case <-b.ctx.Done():
			return b.ctx.Err()
		}
	}
}
func (b *PeerBackend) publish(peer *kit.Peer, row peerSession) error {
	b.mu.Lock()
	if b.ending {
		b.mu.Unlock()
		return context.Canceled
	}
	next := b.identity
	next.Name = row.Title
	next.Info = map[string]any{"cwd": row.Cwd}
	if b.admitted && next.Name == b.identity.Name && row.Cwd == b.cwd {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()
	err := peer.Rehello(b.ctx, next.Name, next.Info)
	var failure *kit.ProtocolError
	if err != nil && (!errors.As(err, &failure) || failure.Code != protocol.NotConnected) {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.ending && b.peer == peer {
		b.identity = next
		b.cwd = row.Cwd
	}
	return nil
}
func (b *PeerBackend) waitReady(ctx context.Context) error {
	select {
	case <-b.ready:
	case <-b.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	if b.ending || !b.admitted {
		return errors.New("Grok peer is unavailable")
	}
	return nil
}
func (b *PeerBackend) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if err := b.waitReady(ctx); err != nil {
		return nil, err
	}
	b.mu.Lock()
	peer := b.peer
	b.mu.Unlock()
	if peer == nil {
		return nil, errors.New("Grok peer is unavailable")
	}
	return peer.Call(ctx, method, params)
}
func (b *PeerBackend) Caller() *kit.Caller                                  { return b.caller }
func (b *PeerBackend) Prepare(ctx context.Context, _ json.RawMessage) error { return b.waitReady(ctx) }
func (b *PeerBackend) Action(ctx context.Context, action string, args json.RawMessage) (json.RawMessage, error) {
	return b.caller.Action(ctx, action, args)
}
func (b *PeerBackend) End() { b.Shutdown() }
func (b *PeerBackend) Shutdown() {
	b.mu.Lock()
	b.ending = true
	peer, o := b.peer, b.observer
	b.mu.Unlock()
	b.cancel()
	if peer != nil {
		peer.Shutdown()
	}
	if o != nil {
		o.close()
	}
	b.initialized.Do(func() { close(b.done) })
	<-b.done
}
func (b *PeerBackend) deliverOwned(ctx context.Context, identity kit.PeerIdentity, request kit.DeliveryRequest) (kit.DeliveryReceipt, error) {
	b.mu.Lock()
	if b.ending {
		b.mu.Unlock()
		return kit.DeliveryReceipt{Disposition: "rejected", Reason: "closing"}, nil
	}
	select {
	case b.slots <- struct{}{}:
	default:
		b.mu.Unlock()
		return kit.DeliveryReceipt{}, &kit.ProtocolError{Code: protocol.Internal, Data: json.RawMessage(`"uncertain_native_admission"`)}
	}
	b.work.Add(1)
	b.mu.Unlock()
	defer b.work.Done()
	defer func() { <-b.slots }()
	receipt, err := b.deliver(ctx, identity, request)
	if err != nil {
		return kit.DeliveryReceipt{}, &kit.ProtocolError{Code: protocol.Internal, Data: json.RawMessage(`"uncertain_native_admission"`)}
	}
	return receipt, nil
}
func (b *PeerBackend) deliver(ctx context.Context, identity kit.PeerIdentity, request kit.DeliveryRequest) (kit.DeliveryReceipt, error) {
	if err := b.waitReady(ctx); err != nil {
		return kit.DeliveryReceipt{}, err
	}
	select {
	case <-ctx.Done():
		return kit.DeliveryReceipt{}, ctx.Err()
	case <-b.ctx.Done():
		return kit.DeliveryReceipt{}, b.ctx.Err()
	case <-b.deliveryGate:
	}
	defer func() { b.deliveryGate <- struct{}{} }()
	b.mu.Lock()
	observer, ending, id := b.observer, b.ending, b.identity.SessionID
	b.mu.Unlock()
	if ending || identity.SessionID != id {
		return kit.DeliveryReceipt{Disposition: "rejected", Reason: "shutting down"}, nil
	}
	message, err := host.RenderNativeMessage(request)
	if err != nil {
		return kit.DeliveryReceipt{}, err
	}
	if err = observer.interject(ctx, id, request.MessageID, message); err != nil {
		return kit.DeliveryReceipt{}, err
	}
	return kit.DeliveryReceipt{Disposition: "injected"}, nil
}
