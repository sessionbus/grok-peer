// SPDX-License-Identifier: MIT

package grok

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/mcp"
)

const Product = "grok-peer"
const PrivateAlias = "grok-peer-mcp"

const grokReadyInterval = 25 * time.Millisecond

var command = exec.Command

type nativeProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

type Wrapper struct {
	socket, key   string
	caller        *sessionkit.Caller
	endpoint      *grokEndpoint
	mu            sync.Mutex
	primary       *acpClient
	observer      *acpClient
	child         *nativeProcess
	leader        *nativeProcess
	watcher       *nativeProcess
	sessionID     string
	run           *sessionkit.Run
	pendingPrompt *nativePrompt
	answers       map[string]*strings.Builder
	closing       bool
	ctx           context.Context
	cancel        context.CancelFunc
	opened        bool
	shutdown      func()
	lossOnce      sync.Once
	nativeFailed  chan struct{}
	nativeFailure error
	deliveryGate  chan struct{}
}

func New(socket, token string) *Wrapper {
	return &Wrapper{socket: socket, key: host.LaunchTokenDigest(token)}
}

func (p *Wrapper) SetShutdown(shutdown func()) { p.shutdown = shutdown }
func (p *Wrapper) SetCall(call func(context.Context, string, any) (json.RawMessage, error)) {
	p.SetCaller(sessionkit.NewCaller(call))
}

func (p *Wrapper) SetCaller(caller *sessionkit.Caller) { p.caller = caller }

func (*Wrapper) Hello(context.Context) (sessionkit.HelloDescription, error) {
	return sessionkit.HelloDescription{Product: Product, SupportsMessageRun: true,
		SupportedOpenFields: []string{"cwd", "permission_mode", "model", "reasoning_effort", "arguments"},
		ExtraArguments: []sessionkit.ExtraArgument{
			{Name: "--agent", Description: "Agent name or definition", TakesValue: true},
			{Name: "--disable-web-search", Description: "Disable web tools", TakesValue: false},
			{Name: "--no-plan", Description: "Disable plan mode", TakesValue: false},
			{Name: "--no-subagents", Description: "Disable subagents", TakesValue: false},
		},
	}, nil
}

func (p *Wrapper) Open(ctx context.Context, request sessionkit.OpenRequest) (result sessionkit.OpenResult, err error) {
	p.mu.Lock()
	if p.ctx != nil || p.closing {
		p.mu.Unlock()
		return result, errors.New("Grok worker already opened or closed")
	}
	p.ctx, p.cancel = context.WithCancel(context.WithoutCancel(ctx))
	p.nativeFailed = make(chan struct{})
	stopStartup := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		if !p.opened {
			p.cancel()
		}
		p.mu.Unlock()
	})
	p.mu.Unlock()
	defer stopStartup()
	defer func() {
		if err != nil {
			p.cancel()
			err = errors.Join(err, p.Close(context.Background(), sessionkit.SessionCloseRequest{}))
		}
	}()

	if p.caller == nil || p.socket == "" || p.key == "" {
		return sessionkit.OpenResult{}, errors.New("Grok lane host is incomplete")
	}
	cwd, err := filepath.Abs(first(request.Open.Cwd, "."))
	if err != nil {
		return sessionkit.OpenResult{}, fmt.Errorf("resolve Grok cwd: %w", err)
	}
	request.Open.Cwd = cwd
	name, err := namePart(request.Name)
	if err != nil {
		return sessionkit.OpenResult{}, err
	}
	primaryArgs, err := launchArguments(request, leaderSocket(p.socket, p.key))
	if err != nil {
		return sessionkit.OpenResult{}, err
	}
	nativeEnv := nativeEnvironment()
	if err = ensureSessionbusPermission(nativeEnv, request.Open.Cwd); err != nil {
		return sessionkit.OpenResult{}, err
	}
	endpoint, err := newGrokEndpoint(p)
	if err != nil {
		return result, err
	}
	p.mu.Lock()
	p.endpoint = endpoint
	p.mu.Unlock()

	leader, err := startLeader(p.ctx, p.socket, p.key, request.Open.Cwd, request.Open.PermissionMode, nativeEnv)
	if err != nil {
		return result, err
	}
	hold, holdProcess, err := p.startObserverClient(ctx, request.Open.Cwd)
	if err != nil {
		stopAux(leader)
		return result, err
	}
	releaseHold := func() {
		hold.close()
		stopAux(holdProcess)
	}
	fail := func(cause error) error {
		releaseHold()
		stopAux(leader)
		return cause
	}
	primaryCommand := command("grok", primaryArgs...)
	primaryCommand.Dir, primaryCommand.Stderr = request.Open.Cwd, os.Stderr
	primaryCommand.Env = nativeEnv
	child, input, output, err := startACPProcess(primaryCommand)
	if err != nil {
		return sessionkit.OpenResult{}, fail(fmt.Errorf("start Grok primary: %w", err))
	}
	primary := newACPClient(input, output, p.receive)
	p.mu.Lock()
	p.primary, p.child, p.leader = primary, child, leader
	p.mu.Unlock()
	if err = initializeACP(ctx, primary); err == nil {
		err = p.openSession(ctx, primary, request, endpoint.Path)
	}
	if err == nil {
		err = p.startObserver(ctx, request.Open.Cwd, name)
	}
	releaseHold()
	if err != nil {
		return sessionkit.OpenResult{}, err
	}
	err = p.commitOpen(ctx, stopStartup)

	if err != nil {
		return result, err
	}
	go p.watch(child)
	return sessionkit.OpenResult{SessionID: p.sessionID}, nil
}

func (p *Wrapper) commitOpen(ctx context.Context, stopStartup func() bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	if p.nativeFailure != nil {
		return p.nativeFailure
	}
	select {
	case <-p.primary.done:
		return errors.New("Grok primary ended before Open commit")
	default:
	}
	p.opened = true
	stopStartup()
	return nil
}

func (p *Wrapper) openSession(ctx context.Context, primary *acpClient, request sessionkit.OpenRequest, laneSocket string) error {
	server, err := mcpServer(laneSocket)
	if err != nil {
		return err
	}
	params := map[string]any{"cwd": request.Open.Cwd, "mcpServers": []any{server}}
	if request.Open.PermissionMode != "" {
		params["_meta"] = map[string]bool{"yoloMode": request.Open.PermissionMode == "bypassPermissions"}
	}
	method := "session/new"
	if request.ResumeSessionID != "" {
		method, params["sessionId"] = "session/load", request.ResumeSessionID
	}
	var opened struct {
		SessionID string `json:"sessionId"`
		Meta      struct {
			SessionID string `json:"sessionId"`
			Detail    struct {
				SessionID string `json:"sessionId"`
			} `json:"x.ai/sessionDetail"`
		} `json:"_meta"`
	}
	if err := primary.request(ctx, method, params, &opened); err != nil {
		return fmt.Errorf("open Grok session: %w", err)
	}
	identity := opened.SessionID
	if request.ResumeSessionID != "" {
		for _, returned := range []string{identity, opened.Meta.SessionID, opened.Meta.Detail.SessionID} {
			if returned != "" && returned != request.ResumeSessionID {
				return fmt.Errorf("Grok returned session identity %q instead of %q", returned, request.ResumeSessionID)
			}
		}
		identity = request.ResumeSessionID
	}
	if identity == "" {
		return errors.New("Grok returned no session identity")
	}

	p.mu.Lock()
	p.sessionID = identity
	err = p.endpoint.validateSession(identity)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.endpoint.waitReady(ctx)
}

func startLeader(ctx context.Context, socket, key, cwd, permission string, environment []string) (*nativeProcess, error) {
	arguments := sessionbusLeaderPolicy()
	if permission != "" {
		arguments = append(arguments, "--permission-mode", permission)
	}
	return startLeaderWithPolicy(ctx, socket, key, cwd, arguments, environment)
}
func startLeaderWithPolicy(ctx context.Context, socket, key, cwd string, policy, environment []string) (*nativeProcess, error) {
	path := leaderSocket(socket, key)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	_ = os.Remove(path)
	arguments := slices.Clone(policy)
	arguments = append(arguments, "agent", "leader", "--leader-socket", path, "--relay-on-demand", "--no-auto-update")
	cmd := command("grok", arguments...)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = cwd, environment, os.Stderr, os.Stderr
	process, err := startNative(cmd)
	if err != nil {
		return nil, err
	}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(grokReadyInterval)
	defer tick.Stop()
	for {
		select {
		case <-process.done:
			return nil, fmt.Errorf("Grok leader exited: %v", process.err)
		case <-ctx.Done():
			stopAux(process)
			return nil, ctx.Err()
		case <-deadline.C:
			_ = process.cmd.Process.Signal(syscall.SIGKILL)
			<-process.done
			return nil, errors.New("Grok leader socket was not ready")
		case <-tick.C:
			if grokSocketReady(path) {
				return process, nil
			}
		}
	}
}

func (p *Wrapper) startObserverClient(ctx context.Context, cwd string, notify ...func(acpFrame)) (*acpClient, *nativeProcess, error) {
	args := []string{"--no-auto-update", "--leader-socket", leaderSocket(p.socket, p.key), "agent", "--leader", "stdio"}
	cmd := command("grok", args...)
	cmd.Dir, cmd.Env, cmd.Stderr = cwd, nativeEnvironment(), os.Stderr
	var callback func(acpFrame)
	if len(notify) > 0 {
		callback = notify[0]
	}
	return startObserverClient(p.ctx, ctx, cmd, callback)
}

func startObserverClient(lifetimeCtx, requestCtx context.Context, cmd *exec.Cmd, notify func(acpFrame)) (*acpClient, *nativeProcess, error) {
	process, input, output, err := startACPProcess(cmd)
	if err != nil {
		return nil, nil, err
	}
	client := newACPClient(input, output, notify)
	go func() {
		select {
		case <-lifetimeCtx.Done():
			client.close()
		case <-client.done:
		}
	}()
	if err = initializeACP(requestCtx, client); err != nil {
		client.close()
		stopAux(process)
		return nil, nil, err
	}
	return client, process, nil
}

func (p *Wrapper) startObserver(ctx context.Context, cwd, title string) error {
	observer, process, err := p.startObserverClient(ctx, cwd)
	if err != nil {
		return fmt.Errorf("start Grok observer: %w", err)
	}
	var renamed struct {
		Success bool `json:"success"`
	}
	err = observer.request(ctx, "_x.ai/session/rename", map[string]string{"sessionId": p.sessionID, "title": title}, &renamed)
	if err == nil && !renamed.Success {
		err = errors.New("Grok did not apply the session title")
	}
	if err != nil {
		observer.close()
		stopAux(process)
		return err
	}
	p.mu.Lock()
	p.observer, p.watcher = observer, process
	p.mu.Unlock()
	return nil
}

func initializeACP(ctx context.Context, client *acpClient) error {
	var initialized struct {
		AuthMethods []struct {
			ID string `json:"id"`
		} `json:"authMethods"`
	}
	if err := client.request(ctx, "initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{"fs": map[string]bool{"readTextFile": false, "writeTextFile": false}, "terminal": false}}, &initialized); err != nil {
		return err
	}
	if !slices.ContainsFunc(initialized.AuthMethods, func(method struct {
		ID string `json:"id"`
	}) bool {
		return method.ID == "cached_token"
	}) {
		return errors.New("Grok cached_token authentication is unavailable")
	}
	return client.request(ctx, "authenticate", map[string]any{"methodId": "cached_token", "_meta": map[string]bool{"headless": true}}, &map[string]any{})
}

func (p *Wrapper) Run(ctx context.Context, run *sessionkit.Run, seed sessionkit.RunInput) (sessionkit.TurnResult, error) {
	return p.executeRun(ctx, run, seed, run.ReportDelivery)
}
func (p *Wrapper) executeRun(ctx context.Context, run *sessionkit.Run, seed sessionkit.RunInput, report func(sessionkit.DeliveryReceipt, error) error) (sessionkit.TurnResult, error) {
	reject := func(err error) (sessionkit.TurnResult, error) {
		if seed.Delivery != nil {
			if e := report(sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "not_submitted"}, nil); e != nil {
				return sessionkit.TurnResult{}, e
			}
		}
		return sessionkit.TurnResult{}, err
	}
	if (seed.Text == nil) == (seed.Delivery == nil) {
		return reject(errors.New("expected exactly one run input"))
	}
	if err := ctx.Err(); err != nil {
		return reject(err)
	}
	var input string
	if seed.Text != nil {
		input = *seed.Text
	} else {
		var err error
		input, err = host.RenderNativeMessage(*seed.Delivery)
		if err != nil {
			return reject(err)
		}
	}

	p.mu.Lock()
	p.retireCompletedRunLocked()
	if p.primary == nil || p.run != nil || p.closing || p.nativeFailure != nil {
		p.mu.Unlock()
		return reject(errors.New("Grok lane is not idle"))
	}
	p.run, p.answers = run, map[string]*strings.Builder{}
	primary, id := p.primary, p.sessionID
	p.mu.Unlock()

	if run.Interrupted() {
		if seed.Delivery != nil {
			return reject(errors.New("interrupted before submission"))
		}
		return sessionkit.TurnResult{Outcome: "interrupted"}, nil
	}
	turn, err := p.startPrompt(ctx, primary, id, input)
	var result sessionkit.TurnResult
	if seed.Delivery != nil {
		if err == nil {
			err = turn.admission(ctx)
		}
		if err != nil {
			if e := report(sessionkit.DeliveryReceipt{}, fmt.Errorf("uncertain_native_admission: %w", err)); e != nil {
				err = e
			}
		} else {
			run.Admitted()
			err = report(sessionkit.DeliveryReceipt{Disposition: "injected"}, nil)
		}
	}
	if err == nil {
		if run.Interrupted() {
			_ = turn.Interrupt(ctx)
		}
		result, err = turn.Wait(ctx)
	}

	if err != nil && turn != nil {
		turn.failOwned(err)
	}
	p.mu.Lock()
	p.answers = nil
	if p.pendingPrompt == turn {
		p.pendingPrompt = nil
	}
	p.mu.Unlock()
	return result, err
}

type nativeInterrupt struct {
	done chan struct{}
	err  error
}

type nativePrompt struct {
	interrupt   *nativeInterrupt
	ctx         context.Context
	changed     chan struct{}
	segments    []*nativeSegment
	delivery    *nativeDelivery
	retiring    bool
	outputBytes int
	failure     error
	owner       *Wrapper
	client      *acpClient
	sessionID   string
	promptText  string
	nativeID    string
	attempted   bool
	admitted    chan struct{}
	done        chan struct{}
	err         error
	result      struct {
		StopReason string `json:"stopReason"`
		Meta       struct {
			PromptID string `json:"promptId"`
		} `json:"_meta"`
	}
}

func (p *Wrapper) startPrompt(ctx context.Context, primary *acpClient, id, prompt string) (*nativePrompt, error) {
	t := &nativePrompt{ctx: ctx, changed: make(chan struct{}), owner: p, client: primary, sessionID: id, promptText: prompt, admitted: make(chan struct{}), done: make(chan struct{})}
	p.mu.Lock()
	// Publish the sole native request owner before any native-visible write.
	// Admission, delivery and interruption all consult this same owner.
	p.pendingPrompt = t
	p.mu.Unlock()
	started := make(chan error, 1)
	go func() {
		t.err = primary.requestSubmitting(ctx, "session/prompt", map[string]any{"sessionId": id, "prompt": []map[string]string{{"type": "text", "text": prompt}}}, &t.result, started, func() error {
			p.mu.Lock()
			t.attempted = true
			p.mu.Unlock()
			return nil
		})
		close(t.done)
	}()
	if err := <-started; err != nil {
		// The write may already have admitted a native event or an interrupt.
		// Keep its registered owner through failure cleanup and join the sender.
		<-t.done
		return t, err
	}
	return t, nil
}

func (t *nativePrompt) Wait(ctx context.Context) (sessionkit.TurnResult, error) {
	return t.waitOwned(ctx)
}

func (t *nativePrompt) Interrupt(ctx context.Context) error {
	p := t.owner
	p.mu.Lock()
	if !t.attempted || t.retiring || p.pendingPrompt != t || p.closing || p.nativeFailure != nil || len(t.segments) > 0 && t.segments[len(t.segments)-1].terminal {
		p.mu.Unlock()
		return nil
	}
	if operation := t.interrupt; operation != nil {
		p.mu.Unlock()
		<-operation.done
		return operation.err
	}
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return err
	}
	operation := &nativeInterrupt{done: make(chan struct{})}
	t.interrupt = operation
	p.mu.Unlock()

	// The callback and the startup fallback represent one SDK interrupt intent.
	// Run completion joins this send before the SDK cancels its Run context.
	err := t.client.cancelContext(ctx, t.sessionID)
	p.mu.Lock()
	operation.err = err
	close(operation.done)
	t.signal()
	p.mu.Unlock()
	return err
}

// Called under p.mu at admission. Shared Done, not a scheduled cleanup
// goroutine or the native terminal alone, makes the previous owner replaceable.
func (p *Wrapper) retireCompletedRunLocked() {
	if p.run != nil {
		select {
		case <-p.run.Done():
			p.run = nil
		default:
		}
	}
}

func (p *Wrapper) receive(frame acpFrame) {
	if replayFrame(frame) {
		return
	}
	p.receiveQueue(frame)
	p.receiveLifecycle(frame)
	if frame.Method != "session/update" {
		return
	}
	var update struct {
		SessionID string `json:"sessionId"`
		Update    struct {
			Kind    string `json:"sessionUpdate"`
			Content struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"update"`
		Meta struct {
			PromptID string `json:"promptId"`
		} `json:"_meta"`
	}
	if json.Unmarshal(frame.Params, &update) != nil || update.Update.Kind != "agent_message_chunk" || update.Meta.PromptID == "" {
		return
	}
	p.mu.Lock()
	t := p.pendingPrompt
	var overflow error
	if t != nil && update.SessionID == p.sessionID && t.hasSegment(update.Meta.PromptID) {
		if t.outputBytes+len(update.Update.Content.Text) > maxACPFrame {
			t.failure = errors.New("Grok output exceeds retention limit")
			overflow = t.failure
			t.signal()
		} else if t.failure == nil {
			if p.answers[update.Meta.PromptID] == nil {
				p.answers[update.Meta.PromptID] = &strings.Builder{}
			}
			p.answers[update.Meta.PromptID].WriteString(update.Update.Content.Text)
			t.outputBytes += len(update.Update.Content.Text)
		}
	}
	p.mu.Unlock()
	if overflow != nil {
		t.abortAccounting(overflow)
	}
}

func (p *Wrapper) Interrupt(ctx context.Context, run *sessionkit.Run) error {
	p.mu.Lock()
	native := p.pendingPrompt
	if p.run != run {
		native = nil
	}
	p.mu.Unlock()
	if native != nil {
		return native.Interrupt(ctx)
	}
	return nil
}
func (p *Wrapper) Deliver(ctx context.Context, request sessionkit.DeliveryRequest, _ *sessionkit.Run) (sessionkit.DeliveryReceipt, error) {
	message, err := host.RenderNativeMessage(request)
	if err != nil {
		return sessionkit.DeliveryReceipt{}, err
	}
	p.mu.Lock()
	p.retireCompletedRunLocked()
	if p.closing || p.primary == nil || p.nativeFailure != nil {
		p.mu.Unlock()
		return sessionkit.DeliveryReceipt{Disposition: "rejected", Reason: "lane_unavailable"}, nil
	}
	if p.run == nil {
		p.mu.Unlock()
		return sessionkit.DeliveryReceipt{}, host.NotRunning()
	}
	p.mu.Unlock()
	return p.deliverActive(ctx, request, message)
}

func (p *Wrapper) Close(ctx context.Context, _ sessionkit.SessionCloseRequest) error {
	p.mu.Lock()
	p.closing = true
	p.mu.Unlock()
	return p.closeProcesses(ctx)
}

func (p *Wrapper) closeProcesses(ctx context.Context) error {
	p.mu.Lock()
	primary, observer, child, watcher, leader, id, endpoint := p.primary, p.observer, p.child, p.watcher, p.leader, p.sessionID, p.endpoint
	opened := p.opened
	p.mu.Unlock()
	abort := func() {
		if primary != nil {
			primary.close()
		}
		if observer != nil {
			observer.close()
		}
		for _, process := range []*nativeProcess{child, watcher, leader} {
			if process != nil {
				_ = process.cmd.Process.Kill()
			}
		}
	}
	stop := context.AfterFunc(ctx, abort)
	defer stop()
	if !opened || ctx.Err() != nil {
		abort()
	}
	var failures []error
	if primary != nil {
		if opened && ctx.Err() == nil && id != "" {
			failures = append(failures, primary.request(ctx, "session/close", map[string]string{"sessionId": id}, &map[string]any{}))
		}
		_ = primary.input.Close()
		<-primary.done
		primary.mu.Lock()
		readErr := primary.err
		primary.mu.Unlock()
		if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrClosedPipe) {
			failures = append(failures, readErr)
		}
	}
	if child != nil {
		failures = append(failures, child.Wait())
	}
	if observer != nil {
		_ = observer.input.Close()
		<-observer.done
	}
	if watcher != nil {
		failures = append(failures, watcher.Wait())
	}
	failures = append(failures, closeNative("leader", leader))
	if endpoint != nil {
		failures = append(failures, endpoint.Close())
	}
	if p.cancel != nil {
		p.cancel()
	}
	path := leaderSocket(p.socket, p.key)
	failures = appendRemoveError(failures, path)
	failures = appendRemoveError(failures, strings.TrimSuffix(path, ".sock")+".lock")
	return errors.Join(failures...)
}

func (p *Wrapper) watch(child *nativeProcess) {
	err := child.Wait()
	p.mu.Lock()
	closing, run, shutdown := p.closing, p.run, p.shutdown
	p.mu.Unlock()
	if err != nil && !closing {
		fmt.Fprintf(os.Stderr, "sessionbus: Grok primary exited: %v\n", err)
	}
	if !closing && shutdown != nil {
		if run != nil {
			<-run.Done()
		}
		shutdown()
	}
}

func stopAux(process *nativeProcess) {
	if process == nil {
		return
	}
	pid := process.cmd.Process.Pid
	if process.cmd.SysProcAttr != nil && process.cmd.SysProcAttr.Setpgid {
		pid = -pid
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	<-process.done
}

func stopNative(process *nativeProcess) error {
	if err := process.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	<-process.done
	return nil
}

func closeNative(role string, process *nativeProcess) error {
	if process == nil {
		return nil
	}
	if err := stopNative(process); err != nil {
		return fmt.Errorf("close Grok %s: %w", role, err)
	}
	return nil
}

func appendRemoveError(failures []error, path string) []error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return append(failures, err)
	}
	return failures
}

func startNative(cmd *exec.Cmd) (*nativeProcess, error) {
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	process := &nativeProcess{cmd: cmd, done: make(chan struct{})}
	go func() { process.err = cmd.Wait(); close(process.done) }()
	return process, nil
}

func mcpServer(socket string) (map[string]any, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return map[string]any{"name": "sessionbus", "command": filepath.Join(filepath.Dir(executable), PrivateAlias), "args": []string{}, "env": []map[string]string{{"name": mcp.LaneSocketEnv, "value": socket}}}, nil
}

func leaderSocket(socket, key string) string {
	return filepath.Join(filepath.Dir(socket), "grok-"+key+".sock")
}

func grokSocketReady(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode()&os.ModeSocket != 0
}

func launchArguments(request sessionkit.OpenRequest, leader string) ([]string, error) {
	extra, err := extraArguments(request.Open.Arguments)
	if err != nil {
		return nil, err
	}
	arguments := []string{"--no-auto-update"}
	if request.Open.PermissionMode != "" {
		arguments = append(arguments, "--permission-mode", request.Open.PermissionMode)
	}
	if request.Open.ReasoningEffort != "" {
		arguments = append(arguments, "--reasoning-effort", request.Open.ReasoningEffort)
	}
	// Accepted extras are top-level native options. Native agent mode reads its
	// model only from the agent subcommand's own -m, so it follows "agent".
	arguments = append(arguments, extra...)
	arguments = append(arguments, "--leader-socket", leader, "agent")
	if request.Open.Model != "" {
		arguments = append(arguments, "-m", request.Open.Model)
	}
	return append(arguments, "--leader", "stdio"), nil
}

var argumentRules = []host.ArgumentRule{
	{Name: "--agent", TakesValue: true}, {Name: "--disable-web-search"},
	{Name: "--no-plan"}, {Name: "--no-subagents"},
	{Name: "--permission-mode", TakesValue: true, ConflictField: "permission_mode"},
	{Name: "--always-approve", ConflictField: "permission_mode"},
	{Name: "--reasoning-effort", TakesValue: true, ConflictField: "reasoning_effort"},
	{Name: "--effort", TakesValue: true, ConflictField: "reasoning_effort"},
	{Name: "-m", TakesValue: true, ConflictField: "model"},
	{Name: "--model", TakesValue: true, ConflictField: "model"},
	{Name: "--cwd", TakesValue: true, ConflictField: "cwd"},
	{Name: "--session-id", TakesValue: true, ConflictField: "session_id"},
	{Name: "--resume", TakesValue: true, ConflictField: "session_id"},
	{Name: "-r", TakesValue: true, ConflictField: "session_id"},
	{Name: "--leader", ConflictField: "leader"}, {Name: "--no-leader", ConflictField: "leader"},
	{Name: "--leader-socket", TakesValue: true, ConflictField: "leader"},
}

func extraArguments(arguments []string) ([]string, error) {
	return host.BuildArguments(arguments, argumentRules)
}

func nativeEnvironment() []string {
	return nativeEnvironmentFrom(os.Environ())
}

func nativeEnvironmentFrom(environment []string) []string {
	return slices.DeleteFunc(slices.Clone(environment), func(value string) bool {
		name, _, _ := strings.Cut(value, "=")
		return slices.Contains([]string{host.SocketEnv, host.LocalKeyEnv, host.TokenEnv, host.SessionIDEnv, host.NameEnv, host.GroupsEnv, mcp.LaneSocketEnv, ManagedEnv}, name)
	})
}

func namePart(name string) (string, error) {
	index := strings.LastIndexByte(name, '@')
	if index < 1 {
		return "", errors.New("Grok lane name is invalid")
	}
	return name[:index], nil
}

func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ sessionkit.WorkerCallbacks = (*Wrapper)(nil)

func (p *Wrapper) receiveQueue(frame acpFrame) {
	if replayFrame(frame) || frame.Method != "_x.ai/queue/changed" {
		return
	}
	var q struct {
		SessionID string `json:"sessionId"`
		PromptID  string `json:"runningPromptId"`
		Text      string `json:"runningText"`
		Kind      string `json:"runningKind"`
	}
	if json.Unmarshal(frame.Params, &q) != nil || q.PromptID == "" || q.Kind != "prompt" {
		return
	}
	p.mu.Lock()
	var overflow error
	defer func() {
		p.mu.Unlock()
		if overflow != nil {
			p.pendingFailure(overflow)
		}
	}()
	t := p.pendingPrompt
	if t == nil || !t.attempted || q.SessionID != t.sessionID {
		return
	}
	if t.nativeID == "" && nativeRunningTextMatches(t.promptText, q.Text) {
		t.nativeID = q.PromptID
		t.segments = append(t.segments, &nativeSegment{id: q.PromptID})
		close(t.admitted)
		t.signal()
		return
	}
	d := t.delivery
	if d != nil && d.attempted && d.acked && !d.classified && nativeRunningTextMatches(d.text, q.Text) && !t.hasSegment(q.PromptID) {
		if len(t.segments) >= maxACPPending {
			t.failure = errors.New("Grok continuation capacity exceeded")
			overflow = t.failure
			t.signal()
			return
		}
		t.segments = append(t.segments, &nativeSegment{id: q.PromptID})
		d.classified = true
		t.releaseDelivery(d)
	}

}

// Grok projects the one submitted text block into queue runningText by applying
// Rust str::trim. Keep the wire payload untouched and match that exact display
// projection; internal whitespace and every other admission key remain strict.
func nativeRunningTextMatches(submitted, running string) bool {
	return running == strings.TrimSpace(submitted)
}

func (t *nativePrompt) admission(ctx context.Context) error {
	t.owner.mu.Lock()
	observer := t.owner.primary
	t.owner.mu.Unlock()
	if observer == nil {
		return errors.New("Grok primary admission stream unavailable")
	}
	select {
	case <-t.admitted:
		return nil
	case <-t.done:
		select {
		case <-t.admitted:
			return nil
		default:
		}
		if t.err != nil {
			return t.err
		}
		return errors.New("Grok prompt ended without native admission event")
	case <-ctx.Done():
		select {
		case <-t.admitted:
			return nil
		default:
			return ctx.Err()
		}
	case <-observer.done:
		select {
		case <-t.admitted:
			return nil
		default:
			return errors.New("Grok primary ended before prompt admission")
		}
	}
}

func replayFrame(frame acpFrame) bool {
	var params struct {
		Meta struct {
			Replay bool `json:"isReplay"`
		} `json:"_meta"`
	}
	return json.Unmarshal(frame.Params, &params) == nil && params.Meta.Replay
}

func (p *nativeProcess) Done() <-chan struct{} { return p.done }
func (p *nativeProcess) Wait() error           { <-p.done; return p.err }
func startACPProcess(cmd *exec.Cmd) (*nativeProcess, *os.File, *os.File, error) {
	stdin, input, err := os.Pipe()
	if err != nil {
		return nil, nil, nil, err
	}
	output, stdout, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		_ = input.Close()
		return nil, nil, nil, err
	}
	cmd.Stdin, cmd.Stdout = stdin, stdout
	process, err := startNative(cmd)
	_ = stdin.Close()
	_ = stdout.Close()
	if err != nil {
		_ = input.Close()
		_ = output.Close()
		return nil, nil, nil, err
	}
	return process, input, output, nil
}
