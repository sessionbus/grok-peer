// SPDX-License-Identifier: MIT

package grok

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
)

type peerSession struct {
	SessionID string `json:"sessionId"`
	Title     string `json:"title"`
	Cwd       string `json:"cwd"`
	Activity  string `json:"activity"`
	Resident  *bool  `json:"resident"`
	Yolo      *bool  `json:"yolo"`
}

var errNoLeader = errors.New("no_leader")
var errRosterActorGone = errors.New("Grok roster actor is not live")

const ManagedEnv = "SESSIONBUS_GROK_MANAGED"

const grokSessionIDEnv = "GROK_SESSION_ID"
const grokLeaderSocketEnv = "GROK_LEADER_SOCKET"

func startPeerClient(lifetimeCtx, requestCtx context.Context, leaderPath, cwd string, notify func(acpFrame)) (*acpClient, *nativeProcess, error) {
	cmd := command("grok", "--no-auto-update", "--leader-socket", leaderPath, "agent", "--leader", "stdio")
	cmd.Dir, cmd.Env, cmd.Stderr, cmd.SysProcAttr = cwd, nativeEnvironment(), os.Stderr, &syscall.SysProcAttr{Setpgid: true}
	return startObserverClient(lifetimeCtx, requestCtx, cmd, notify)
}

func RunInteractive(ctx context.Context, plan host.ExecPlan) error {
	if environmentValue(plan.Env, ManagedEnv) == "" {
		path, err := exec.LookPath(plan.Path)
		if err != nil {
			return err
		}
		return syscall.Exec(path, append([]string{path}, plan.Args...), plan.Env)
	}
	socket := first(environmentValue(plan.Env, host.SocketEnv), sessionkit.Socket())
	cwd, err := interactiveCwd(plan.Args)
	if err != nil {
		return err
	}
	policy, err := interactivePolicy(plan.Args)
	if err != nil {
		return err
	}
	nativeEnv := peerNativeEnvironment(plan.Env)
	if err = ensureSessionbusPermission(nativeEnv, cwd); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(socket), 0700); err != nil {
		return err
	}
	runtime, err := os.MkdirTemp(filepath.Dir(socket), "grok-launch-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(runtime)
	// A fresh resource directory for each launch, never a native/session ID.
	privateSocket, key := filepath.Join(runtime, "presence.sock"), "native"
	leaderPath := leaderSocket(privateSocket, key)
	plan.Env = setEnvironment(plan.Env, ManagedEnv, leaderPath)
	nativeEnv = setEnvironment(nativeEnv, ManagedEnv, leaderPath)
	leader, err := startLeaderWithPolicy(ctx, privateSocket, key, cwd, policy, nativeEnv)
	if err != nil {
		return err
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	hold, holdProcess, err := startPeerClient(childCtx, childCtx, leaderPath, cwd, nil)
	if err != nil {
		return errors.Join(err, closeNative("leader", leader))
	}
	plan.Args = append([]string{"--leader", "--leader-socket", leaderPath}, plan.Args...)
	child := command(plan.Path, plan.Args...)
	child.Env, child.Stdin, child.Stdout, child.Stderr = plan.Env, os.Stdin, os.Stdout, os.Stderr
	if err = child.Start(); err != nil {
		stopPeerClient(hold, holdProcess)
		return errors.Join(err, closeNative("leader", leader))
	}
	childDone := make(chan error, 1)
	go func() {
		childDone <- child.Wait()
		close(childDone)
		cancel()
	}()
	finish := func(childErr error) error {
		stopPeerClient(hold, holdProcess)
		return errors.Join(childErr, closeNative("leader", leader))
	}
	select {
	case childErr := <-childDone:
		return finish(childErr)
	case <-ctx.Done():
		return finish(finishInteractiveChild(child, childDone, interactiveSignal(ctx)))
	case <-hold.done:
		childErr := finishInteractiveChild(child, childDone, syscall.SIGTERM)
		return finish(errors.Join(fmt.Errorf("Grok startup hold closed: %w", hold.err), childErr))
	}
}

func peerNativeEnvironment(environment []string) []string {
	result := nativeEnvironmentFrom(environment)
	for _, name := range []string{host.SocketEnv, host.GroupsEnv, host.NameEnv, ManagedEnv} {
		if value := environmentValue(environment, name); value != "" {
			result = setEnvironment(result, name, value)
		}
	}
	return result
}

func finishInteractiveChild(child *exec.Cmd, done <-chan error, stop os.Signal) error {
	if stop != nil {
		_ = child.Process.Signal(stop)
	}
	return <-done
}

func interactiveSignal(ctx context.Context) os.Signal {
	var caught interface{ CaughtSignal() os.Signal }
	if errors.As(context.Cause(ctx), &caught) {
		return caught.CaughtSignal()
	}
	return syscall.SIGTERM
}

func environmentValue(environment []string, name string) string {
	for _, value := range environment {
		if key, body, found := strings.Cut(value, "="); found && key == name {
			return body
		}
	}
	return ""
}

func setEnvironment(environment []string, name, value string) []string {
	result := slices.DeleteFunc(slices.Clone(environment), func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		return key == name
	})
	return append(result, name+"="+value)
}

func roster(ctx context.Context, observer *acpClient, id string) (peerSession, error) {
	sessions, err := rosterRows(ctx, observer)
	if err != nil {
		return peerSession{}, err
	}
	return exactRoster(sessions, id)
}

func rosterRows(ctx context.Context, observer *acpClient) ([]peerSession, error) {
	if observer == nil {
		return nil, errors.New("Grok observer is unavailable")
	}
	var reply struct {
		Result struct {
			Sessions []peerSession `json:"sessions"`
		} `json:"result"`
	}
	if err := observer.request(ctx, "_x.ai/sessions/list", map[string]any{}, &reply); err != nil {
		return nil, err
	}
	return reply.Result.Sessions, nil
}

func exactRoster(sessions []peerSession, id string) (peerSession, error) {
	var found peerSession
	matches := 0
	for _, session := range sessions {
		if session.SessionID == id {
			matches++
			found = session
		}
	}
	if matches == 0 {
		return peerSession{}, errNoLeader
	}
	if matches != 1 {
		return peerSession{}, fmt.Errorf("Grok roster returned %d exact rows for %s", matches, id)
	}
	if found.Resident == nil {
		return peerSession{}, errors.New("exact Grok session roster row has no resident state")
	}
	if !*found.Resident || slices.Contains([]string{"completed", "dormant", "dead"}, found.Activity) {
		return peerSession{}, fmt.Errorf("%w: %s", errRosterActorGone, id)
	}
	if found.Yolo == nil {
		return peerSession{}, errors.New("live Grok roster row has no yolo state")
	}
	if !slices.Contains([]string{"working", "needs_input", "idle"}, found.Activity) {
		return peerSession{}, fmt.Errorf("live Grok roster row has unsupported activity %q", found.Activity)
	}
	return found, nil
}

func stopPeerClient(client *acpClient, process *nativeProcess) {
	if client != nil {
		client.close()
	}
	stopAux(process)
}

func InteractivePlan(arguments, environment []string) (host.ExecPlan, error) {
	if environmentValue(environment, host.TokenEnv) != "" {
		return host.ExecPlan{}, errors.New("interactive launch cannot consume a lane token")
	}
	native := make([]string, 0, len(arguments))
	groups := []string{}
	name := ""
	for i := 0; i < len(arguments); i++ {
		arg := arguments[i]
		if arg == "--" {
			native = append(native, arguments[i:]...)
			break
		}
		key, value, attached := strings.Cut(arg, "=")
		switch key {
		case "-g", "--group", "-n", "--name", "--peer-name":
			if !attached {
				if i+1 == len(arguments) || arguments[i+1] == "--" {
					return host.ExecPlan{}, fmt.Errorf("%s requires a value", key)
				}
				i++
				value = arguments[i]
			}
			if key == "-g" || key == "--group" {
				if value != "" {
					groups = append(groups, strings.Split(value, ",")...)
				}
			} else {
				if value == "" {
					return host.ExecPlan{}, errors.New("name must not be empty")
				}
				name = value
			}
		case "--yolo":
			if attached {
				native = append(native, arg)
			} else {
				native = append(native, "--always-approve")
			}
		default:
			native = append(native, arg)
			if !attached && grokOptionTakesValue(key) && i+1 < len(arguments) && arguments[i+1] != "--" {
				i++
				native = append(native, arguments[i])
			}
		}
	}
	env := slices.DeleteFunc(slices.Clone(environment), func(entry string) bool {
		k, _, _ := strings.Cut(entry, "=")
		return slices.Contains([]string{host.SessionIDEnv, host.NameEnv, host.GroupsEnv, ManagedEnv}, k)
	})
	if grokPassthrough(native) {
		return host.ExecPlan{Path: "grok", Args: native, Env: env}, nil
	}
	if flag := grokHeadless(native); flag != "" {
		return host.ExecPlan{}, fmt.Errorf("%s is headless; use a Sessionbus Grok lane", flag)
	}
	for _, arg := range native {
		if arg == "--" {
			break
		}
		key, _, _ := strings.Cut(arg, "=")
		if slices.Contains([]string{"--leader", "--no-leader", "--leader-socket"}, key) {
			return host.ExecPlan{}, errors.New("grok-peer owns its private native leader; caller leader selection conflicts")
		}
	}
	if _, err := interactivePolicy(native); err != nil {
		return host.ExecPlan{}, err
	}
	raw, _ := json.Marshal(groups)
	env = setEnvironment(env, host.GroupsEnv, string(raw))
	env = setEnvironment(env, host.NameEnv, name)
	env = setEnvironment(env, host.SocketEnv, first(environmentValue(environment, host.SocketEnv), sessionkit.Socket()))
	env = setEnvironment(env, ManagedEnv, "launch")
	return host.ExecPlan{Path: "grok", Args: native, Env: env}, nil
}

func interactiveCwd(arguments []string) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	if value := nativeOption(arguments, "--cwd"); value != "" {
		cwd, err = filepath.Abs(value)
	}
	return cwd, err
}

func nativeOption(arguments []string, target string) string {
	value := ""
	for i, arg := range arguments {
		if arg == "--" {
			break
		}
		key, v, attached := strings.Cut(arg, "=")
		if key == target {
			if !attached && i+1 < len(arguments) {
				v = arguments[i+1]
			}
			value = v
		}
	}
	return value
}

func grokHeadless(arguments []string) string {
	for _, arg := range arguments {
		if arg == "--" {
			break
		}
		key, _, _ := strings.Cut(arg, "=")
		if slices.Contains([]string{"-p", "--single", "--prompt-file", "--prompt-json", "--output-format", "--json-schema", "--max-turns", "--include-partial-messages"}, key) || strings.HasPrefix(arg, "-p") && !strings.HasPrefix(arg, "--") {
			return key
		}
	}
	return ""
}

var grokCommands = map[string]bool{
	"agent": true, "clone": true, "completions": true, "dashboard": true,
	"doctor": true, "du": true, "disk-usage": true, "export": true,
	"help": true, "inspect": true, "leader": true, "login": true,
	"logout": true, "mcp": true, "memory": true, "models": true,
	"plugin": true, "sessions": true, "setup": true, "trace": true,
	"update": true, "version": true, "v": true, "worktree": true,
	"wrap": true,
}

func grokPassthrough(arguments []string) bool {
	if len(arguments) > 0 && grokCommands[arguments[0]] {
		return true
	}
	for _, arg := range arguments {
		if arg == "--" {
			break
		}
		if slices.Contains([]string{"-h", "--help", "-v", "--version"}, arg) {
			return true
		}
	}
	return false
}

func ManagedHelper(environment []string) bool {
	marker := environmentValue(environment, ManagedEnv)
	return marker != "" && marker == environmentValue(environment, grokLeaderSocketEnv) && environmentValue(environment, grokSessionIDEnv) != ""
}

// Required top-level values from native Grok 1.0.24 help, including its listed
// compatibility aliases. Resume and worktree have optional values and are absent.
func grokOptionTakesValue(option string) bool {
	switch option {
	case "--agent", "--agent-profile", "--agents", "--allow", "--allowedTools",
		"--cli-chat-proxy-base-url", "--cwd", "--debug-file", "--deny", "--disallowedTools",
		"--disallowed-tools", "--grok-ws-origin", "--grok-ws-url", "--json-schema",
		"--leader-socket", "--max-turns", "--output-format", "--permission-mode", "--plugin-dir",
		"--prompt-file", "--prompt-json", "--reasoning-effort", "--effort", "--rules",
		"--sandbox", "--system-prompt-override", "--system-prompt", "--tools",
		"--worktree-ref", "--ref", "--xai-api-base-url", "-m", "--model", "-p", "--single",
		"-s", "--session-id":
		return true
	}
	return false
}
