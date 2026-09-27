// SPDX-License-Identifier: MIT
package grok

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"

	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/mcp"
)

type grokEndpoint struct {
	*host.PrivateEndpoint
	owner     *Wrapper
	mu        sync.Mutex
	clients   map[net.Conn]*laneToolOwner
	closed    bool
	calls     sync.WaitGroup
	ready     chan struct{}
	readyOnce sync.Once
}
type nativeHelperIdentity struct {
	SessionID string `json:"session_id"`
	Leader    string `json:"leader"`
}
type laneToolOwner struct {
	endpoint *grokEndpoint
	native   nativeHelperIdentity
}

func newGrokEndpoint(p *Wrapper) (*grokEndpoint, error) {
	listener, err := host.ListenPrivate(p.socket, p.key)
	if err != nil {
		return nil, err
	}
	e := &grokEndpoint{PrivateEndpoint: listener, owner: p, clients: map[net.Conn]*laneToolOwner{}, ready: make(chan struct{})}
	go e.serve()
	return e, nil
}
func (e *grokEndpoint) serve() {
	for {
		c, err := e.Accept()
		if err != nil {
			return
		}
		e.mu.Lock()
		if e.closed || len(e.clients) >= maxACPPending {
			e.mu.Unlock()
			_ = c.Close()
			continue
		}
		e.clients[c] = nil
		e.calls.Add(1)
		e.mu.Unlock()
		go e.serveClient(c)
	}
}
func (e *grokEndpoint) serveClient(c net.Conn) {
	defer e.calls.Done()
	defer func() { _ = c.Close(); e.mu.Lock(); delete(e.clients, c); e.mu.Unlock() }()
	reader := bufio.NewReaderSize(c, 4096)
	body, err := reader.ReadSlice('\n')
	if err != nil {
		return
	}
	var id nativeHelperIdentity
	if json.Unmarshal(body, &id) != nil || id.SessionID == "" || id.Leader != leaderSocket(e.owner.socket, e.owner.key) {
		return
	}
	e.owner.mu.Lock()
	known, closing := e.owner.sessionID, e.owner.closing
	if closing || known != "" && known != id.SessionID {
		e.owner.mu.Unlock()
		return
	}
	owner := &laneToolOwner{endpoint: e, native: id}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		e.owner.mu.Unlock()
		return
	}
	e.clients[c] = owner
	e.mu.Unlock()
	e.owner.mu.Unlock()

	if _, err = io.WriteString(c, "{}\n"); err != nil {
		owner.End()
		return
	}
	_ = mcp.ServeSessionbus(owner, &helperInput{Reader: reader, Closer: c}, c, mcp.ReportHandler{})
}

type helperInput struct {
	io.Reader
	io.Closer
}

func (e *grokEndpoint) validateSession(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, owner := range e.clients {
		if owner != nil && owner.native.SessionID != id {
			return errors.New("Grok native helper identity contradicts opened session")
		}
	}
	return nil
}
func (e *grokEndpoint) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	err := e.PrivateEndpoint.Close()
	for c := range e.clients {
		_ = c.Close()
	}
	e.mu.Unlock()
	e.calls.Wait()
	return err
}
func (o *laneToolOwner) Action(ctx context.Context, action string, args json.RawMessage) (json.RawMessage, error) {
	p := o.endpoint.owner
	p.mu.Lock()
	id, closing := p.sessionID, p.closing
	p.mu.Unlock()
	if id == "" || closing || id != o.native.SessionID {
		return nil, errors.New("Grok native helper does not belong to an open lane")
	}
	return p.caller.Action(ctx, action, args)
}
func (o *laneToolOwner) End() {
	p := o.endpoint.owner
	p.mu.Lock()
	if !p.closing && p.nativeFailure == nil {
		p.nativeFailure = errors.New("Grok native helper disconnected")
		if p.nativeFailed != nil {
			close(p.nativeFailed)
		}
	}
	closing, primary, child, run, shutdown := p.closing, p.primary, p.child, p.run, p.shutdown
	p.mu.Unlock()
	if closing {
		return
	}
	p.lossOnce.Do(func() {
		// Killing a failed native owner does not discard bytes already in its pipe.
		if child != nil {
			_ = child.cmd.Process.Kill()
		}
		if primary != nil {
			_ = primary.input.Close()
		}
		go func() {
			if primary != nil {
				<-primary.done
			}
			if run != nil {
				<-run.Done()
			}
			if shutdown != nil {
				shutdown()
			}
		}()
	})
}

// ForwardLane binds product-provided helper identity once, then carries native
// MCP verbatim on one resident socket. Request cancellation and EOF reach the
// same shared engine and sole Caller in the worker.
func ForwardLane(ctx context.Context, path string, input io.ReadCloser, output io.Writer) error {
	native := nativeHelperIdentity{os.Getenv(grokSessionIDEnv), os.Getenv(grokLeaderSocketEnv)}
	return forwardLane(ctx, path, native, input, output)
}
func forwardLane(ctx context.Context, path string, native nativeHelperIdentity, input io.ReadCloser, output io.Writer) error {
	if path == "" || native.SessionID == "" || native.Leader == "" {
		return errors.New("Grok lane forwarder requires native helper identity")
	}
	c, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { _ = c.Close(); _ = input.Close() })
	defer stop()
	if err = json.NewEncoder(c).Encode(native); err != nil {
		return err
	}
	reader := bufio.NewReaderSize(c, 4096)
	ack, err := reader.ReadSlice('\n')
	if err != nil {
		return err
	}
	if string(ack) != "{}\n" {
		return errors.New("invalid Grok endpoint acknowledgement")
	}
	sent := make(chan error, 1)
	go func() {
		_, e := io.Copy(c, input)
		if u, ok := c.(*net.UnixConn); ok {
			_ = u.CloseWrite()
		}
		sent <- e
	}()
	_, err = io.Copy(output, reader)
	_ = c.Close()
	_ = input.Close()
	return errors.Join(err, <-sent)
}

func (o *laneToolOwner) Initialized() { o.endpoint.readyOnce.Do(func() { close(o.endpoint.ready) }) }
func (e *grokEndpoint) waitReady(ctx context.Context) error {
	p := e.owner
	p.mu.Lock()
	primary, failed := p.primary, p.nativeFailed
	p.mu.Unlock()
	var nativeDone <-chan struct{}
	if primary != nil {
		nativeDone = primary.done
	}
	select {
	case <-e.ready:
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.nativeFailure != nil {
			return p.nativeFailure
		}
		select {
		case <-nativeDone:
			return errors.New("Grok primary ended before helper readiness")
		default:
		}
		return nil
	case <-failed:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.nativeFailure
	case <-nativeDone:
		return errors.New("Grok primary ended before helper readiness")
	case <-ctx.Done():
		return ctx.Err()
	}
}
