// SPDX-License-Identifier: MIT

package grok

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

const maxACPFrame = 1 << 20
const maxACPPending = 256

var errACPCapacity = errors.New("Grok ACP request capacity exhausted")

type acpFrame struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *acpError       `json:"error,omitempty"`
}

func validACPID(raw json.RawMessage) bool {
	if raw == nil || bytes.Equal(raw, []byte("null")) {
		return true
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return true
	}
	_, ok := numericACPID(raw)
	return ok
}

func numericACPID(raw json.RawMessage) (int64, bool) {
	if raw == nil || bytes.Equal(raw, []byte("null")) {
		return 0, false
	}
	var id int64
	return id, json.Unmarshal(raw, &id) == nil
}

type acpError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *acpError) Error() string { return fmt.Sprintf("Grok ACP error %d: %s", e.Code, e.Message) }

type interjectionNotice struct {
	SessionID      string `json:"sessionId"`
	InterjectionID string `json:"interjectionId"`
}
type acpReply struct {
	body json.RawMessage
	err  error
}

// acpRole is fixed at construction. Grok broadcasts shared interactions such
// as permission to every subscribed client and takes the first answer, so only
// the prompt-owning primary answers native client requests.
type acpRole int

const (
	acpObserver acpRole = iota
	acpPrimary
)

type acpClient struct {
	role       acpRole
	input      io.WriteCloser
	output     io.ReadCloser
	mu         sync.Mutex
	writeGate  chan struct{}
	next       int64
	err        error
	pending    map[int64]chan acpReply // nil retains only a cancelled call's drain ID.
	admissions map[interjectionNotice]chan error
	done       chan struct{}
	notify     func(acpFrame)
}

func newACPClient(role acpRole, input io.WriteCloser, output io.ReadCloser, notify func(acpFrame)) *acpClient {
	c := &acpClient{role: role, input: input, output: output, writeGate: make(chan struct{}, 1), pending: map[int64]chan acpReply{}, admissions: map[interjectionNotice]chan error{}, done: make(chan struct{}), notify: notify}
	c.writeGate <- struct{}{}
	go c.read(output)
	return c
}
func (c *acpClient) read(output io.ReadCloser) {
	defer output.Close()
	scanner := bufio.NewScanner(output)
	scanner.Buffer(make([]byte, 4096), maxACPFrame)
	for scanner.Scan() {
		var frame acpFrame
		if json.Unmarshal(scanner.Bytes(), &frame) != nil || frame.JSONRPC != "2.0" {
			c.finish(errors.New("malformed Grok ACP frame"))
			return
		}
		if !validACPID(frame.ID) {
			if frame.Method == "" || frame.Error != nil || len(frame.Result) != 0 {
				c.finish(errors.New("malformed Grok ACP frame"))
				return
			}
			if c.role == acpPrimary {
				reply := acpFrame{JSONRPC: "2.0", ID: json.RawMessage(`null`), Error: &acpError{Code: -32600, Message: "Invalid Request"}}
				if err := c.sendContext(context.Background(), reply); err != nil {
					c.finish(err)
					return
				}
			}
			continue
		}
		if frame.ID != nil && frame.Method != "" {
			if frame.Error != nil || len(frame.Result) != 0 {
				c.finish(errors.New("invalid Grok ACP request envelope"))
				return
			}
			// The product must not mistake a client-side exchange for approval:
			// the primary answers permission cancelled and any other method not
			// found. Both are non-approval results and keep the connection for the
			// turn. An observer leaves the request to the prompt owner unanswered.
			if c.role == acpPrimary {
				reply := acpFrame{JSONRPC: "2.0", ID: frame.ID, Error: &acpError{Code: -32601, Message: "Method not found"}}
				if frame.Method == "session/request_permission" {
					reply.Result, reply.Error = json.RawMessage(`{"outcome":{"outcome":"cancelled"}}`), nil
				}
				if err := c.sendContext(context.Background(), reply); err != nil {
					c.finish(err)
					return
				}
			}
		} else if frame.ID != nil {
			if (frame.Error == nil) == (len(frame.Result) == 0) {
				c.finish(errors.New("invalid Grok ACP response envelope"))
				return
			}
			id, ok := numericACPID(frame.ID)
			if !ok {
				c.finish(errors.New("invalid Grok ACP response ID"))
				return
			}
			c.mu.Lock()
			reply, known := c.pending[id]
			if known {
				delete(c.pending, id)
			}
			if reply != nil {
				var err error
				if frame.Error != nil {
					err = frame.Error
				}
				reply <- acpReply{frame.Result, err}
			}
			c.mu.Unlock()
			if !known {
				c.finish(errors.New("unmatched Grok ACP response"))
				return
			}
		} else {
			if frame.Method == "" || frame.Error != nil || len(frame.Result) != 0 {
				c.finish(errors.New("invalid Grok ACP notification"))
				return
			}
			if frame.Method == "_x.ai/session/interjection" {
				var notice interjectionNotice
				if json.Unmarshal(frame.Params, &notice) != nil || notice.SessionID == "" || notice.InterjectionID == "" {
					c.finish(errors.New("invalid Grok actor acknowledgement"))
					return
				}
				c.mu.Lock()
				if admitted := c.admissions[notice]; admitted != nil {
					delete(c.admissions, notice)
					admitted <- nil
				}
				c.mu.Unlock()
			}
			if c.notify != nil {
				c.notify(frame)
			}
		}
		select {
		case <-c.done:
			return
		default:
		}
	}
	err := scanner.Err()
	if err == nil {
		err = io.EOF
	}
	c.finish(err)
}
func (c *acpClient) finish(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	for id, reply := range c.pending {
		if reply != nil {
			reply <- acpReply{err: err}
		}
		delete(c.pending, id)
	}
	for key, reply := range c.admissions {
		reply <- err
		delete(c.admissions, key)
	}
	close(c.done)
	c.mu.Unlock()
	_ = c.input.Close()
	_ = c.output.Close()
}
func (c *acpClient) request(ctx context.Context, method string, params, result any) error {
	return c.requestStarted(ctx, method, params, result, nil)
}
func (c *acpClient) requestStarted(ctx context.Context, method string, params, result any, started chan<- error) error {
	return c.requestSubmitting(ctx, method, params, result, started, nil)
}
func (c *acpClient) requestSubmitting(ctx context.Context, method string, params, result any, started chan<- error, submit func() error) error {
	signal := func(err error) {
		if started != nil {
			started <- err
		}
	}
	if err := ctx.Err(); err != nil {
		signal(err)
		return err
	}
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		signal(err)
		return err
	}
	if len(c.pending) >= maxACPPending {
		c.mu.Unlock()
		signal(errACPCapacity)
		return errACPCapacity
	}
	c.next++
	id := c.next
	reply := make(chan acpReply, 1)
	c.pending[id] = reply
	c.mu.Unlock()
	err := c.sendSubmitting(ctx, map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}, submit)
	signal(err)
	if err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return fmt.Errorf("write Grok ACP %s: %w", method, err)
	}
	var response acpReply
	select {
	case response = <-reply:
	case <-ctx.Done():
		c.mu.Lock()
		_, unclaimed := c.pending[id]
		if unclaimed {
			c.pending[id] = nil
		}
		c.mu.Unlock()
		if unclaimed {
			return ctx.Err()
		}
		response = <-reply // Native completion claimed by reader wins later cancellation.
	}
	if response.err != nil {
		return response.err
	}
	if result != nil {
		if err := json.Unmarshal(response.body, result); err != nil {
			return fmt.Errorf("decode Grok ACP %s: %w", method, err)
		}
	}
	return nil
}
func (c *acpClient) sendContext(ctx context.Context, value any) error {
	return c.sendSubmitting(ctx, value, nil)
}
func (c *acpClient) sendSubmitting(ctx context.Context, value any, submit func() error) error {
	var body []byte
	var err error
	if frame, exactID := value.(acpFrame); exactID && frame.ID != nil {
		var encoded bytes.Buffer
		encoder := json.NewEncoder(&encoded)
		encoder.SetEscapeHTML(false)
		err = encoder.Encode(frame)
		body = bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})
	} else {
		body, err = json.Marshal(value)
	}
	if err != nil {
		return err
	}
	if len(body)+1 > maxACPFrame {
		return errors.New("Grok ACP frame exceeds size limit")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		c.mu.Lock()
		err = c.err
		c.mu.Unlock()
		return err
	case <-c.writeGate:
	}
	defer func() { c.writeGate <- struct{}{} }()
	c.mu.Lock()
	err = c.err
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if submit != nil {
		if err = submit(); err != nil {
			return err
		}
	}
	done := make(chan error, 1)
	go func() {
		n, e := c.input.Write(append(body, '\n'))
		if e == nil && n != len(body)+1 {
			e = io.ErrShortWrite
		}
		done <- e
	}()
	select {
	case err = <-done:
	case <-ctx.Done():
		select {
		case err = <-done:
		default:
			c.finish(ctx.Err())
			<-done
			err = ctx.Err()
		}
	}
	if err != nil {
		c.finish(err)
	}
	return err
}
func (c *acpClient) cancelContext(ctx context.Context, sessionID string) error {
	return c.sendContext(ctx, map[string]any{"jsonrpc": "2.0", "method": "session/cancel", "params": map[string]string{"sessionId": sessionID}})
}
func (c *acpClient) interject(ctx context.Context, sessionID, messageID, text string) error {
	key := interjectionNotice{sessionID, messageID}
	admitted := make(chan error, 1)
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return err
	}
	if _, exists := c.admissions[key]; exists || len(c.admissions) >= maxACPPending {
		c.mu.Unlock()
		return errACPCapacity
	}
	c.admissions[key] = admitted
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.admissions, key); c.mu.Unlock() }()
	var reply struct {
		Result struct {
			Status string `json:"status"`
		} `json:"result"`
	}
	err := c.request(ctx, "_x.ai/interject", map[string]string{"sessionId": sessionID, "text": text, "interjectionId": messageID}, &reply)
	if err != nil {
		select {
		case accepted := <-admitted:
			if accepted == nil {
				return nil
			}
		default:
		}
		return err
	}
	if reply.Result.Status != "queued" {
		return errors.New("Grok interjection was not queued")
	}
	select {
	case err = <-admitted:
		return err
	case <-ctx.Done():
		c.mu.Lock()
		_, unclaimed := c.admissions[key]
		if unclaimed {
			delete(c.admissions, key)
		}
		c.mu.Unlock()
		if unclaimed {
			return ctx.Err()
		}
		return <-admitted
	}
}
func (c *acpClient) close() { c.finish(io.ErrClosedPipe) }
