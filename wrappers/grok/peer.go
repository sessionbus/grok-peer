// SPDX-License-Identifier: MIT

package grok

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

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

// launchFactsFile is the one file a managed launch writes into its launch directory
// before native starts. The helper's registration carries only that directory, so the
// helper takes what the launch asked for from this file, never from its environment.
// It holds four fields in this order, separated by one NUL byte and otherwise as they
// are: bus socket, groups as their JSON array text, requested name, working directory.
const launchFactsFile = "launch.facts"

type launchFacts struct {
	Socket string
	Groups []string
	Name   string
	Cwd    string
}

func writeLaunchFacts(directory, socket, groups, name, cwd string) error {
	return os.WriteFile(filepath.Join(directory, launchFactsFile), []byte(strings.Join([]string{socket, groups, name, cwd}, "\x00")), 0600)
}

// readLaunchFacts accepts only whole facts: exactly four fields, an absolute bus
// socket and working directory, and groups that decode as a JSON array of strings.
func readLaunchFacts(directory string) (launchFacts, bool) {
	body, err := os.ReadFile(filepath.Join(directory, launchFactsFile))
	fields := strings.Split(string(body), "\x00")
	if err != nil || len(fields) != 4 || !filepath.IsAbs(fields[0]) || !filepath.IsAbs(fields[3]) {
		return launchFacts{}, false
	}
	var groups []string
	if json.Unmarshal([]byte(fields[1]), &groups) != nil || groups == nil {
		return launchFacts{}, false
	}
	return launchFacts{Socket: fields[0], Groups: groups, Name: fields[2], Cwd: fields[3]}, true
}

func startPeerClient(lifetimeCtx, requestCtx context.Context, cwd string, notify func(acpFrame)) (*acpClient, *nativeProcess, error) {
	cmd := command("grok", "agent", "--leader", "stdio")
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
	// The helper accepts only an absolute launch directory and compares this socket
	// with the absolute one the kit resolves.
	socket, err := filepath.Abs(first(environmentValue(plan.Env, host.SocketEnv), sessionkit.Socket()))
	if err != nil {
		return err
	}
	cwd, err := interactiveCwd(plan.Args)
	if err != nil {
		return err
	}
	groups := first(environmentValue(plan.Env, host.GroupsEnv), "[]")
	var list []string
	if json.Unmarshal([]byte(groups), &list) != nil || list == nil {
		return errors.New("SESSIONBUS_GROUPS must be a JSON array")
	}
	if err = ensureManagedConfig(plan.Env, cwd); err != nil {
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
	// A fresh resource directory for each launch, never a native/session ID. Its path
	// is the one value the helper's registration interpolates, and native expands a
	// reference inside it.
	if strings.Contains(runtime, "$") {
		return fmt.Errorf("managed launch directory %s contains \"$\", which Grok would expand in the Sessionbus helper registration; use a Sessionbus socket directory without it", runtime)
	}
	if !utf8.ValidString(runtime) {
		return fmt.Errorf("managed launch directory %q is not valid UTF-8, which Grok cannot pass on in the Sessionbus helper registration; use a Sessionbus socket directory with a UTF-8 path", runtime)
	}
	if err = writeLaunchFacts(runtime, socket, groups, environmentValue(plan.Env, host.NameEnv), cwd); err != nil {
		return err
	}
	// A leader this TUI starts inherits its environment; the facts file carries these values.
	plan.Env = slices.DeleteFunc(slices.Clone(plan.Env), func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		return key == host.GroupsEnv || key == host.NameEnv
	})
	plan.Env = setEnvironment(plan.Env, ManagedEnv, runtime)
	// Grok's own leader serves the session; the launcher starts the TUI alone.
	child := command(plan.Path, append([]string{"--leader"}, plan.Args...)...)
	child.Env, child.Stdin, child.Stdout, child.Stderr = plan.Env, os.Stdin, os.Stdout, os.Stderr
	if err = child.Start(); err != nil {
		return err
	}
	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()
	select {
	case childErr := <-childDone:
		return childErr
	case <-ctx.Done():
		return finishInteractiveChild(child, childDone, interactiveSignal(ctx))
	}
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
	// A refusal is decided where this pass reads a flag. A token it steps over as an
	// option's required value is that option's value, whatever it spells.
	headless := ""
	var conflict error
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
			valued := attached
			if !attached && grokOptionTakesValue(key) && i+1 < len(arguments) && arguments[i+1] != "--" {
				i++
				value, valued = arguments[i], true
				native = append(native, value)
			}
			if grokHeadless(arg, key) {
				headless = first(headless, key)
			} else if conflict == nil && slices.Contains([]string{"--leader", "--no-leader", "--leader-socket"}, key) {
				conflict = errors.New("grok-peer selects Grok's own leader; caller leader selection conflicts")
			} else if conflict == nil && key == "--sandbox" && valued && value != "" && value != "off" && value != "none" {
				// Native reads the exact values off and none as no sandbox and an empty
				// value as no override; any other value, a custom profile name included,
				// confines the session. A missing value is left to native.
				conflict = fmt.Errorf("--sandbox %q makes Grok run the session without a leader, so it cannot be on Sessionbus; start a sandboxed session with plain grok", value)
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
	if headless != "" {
		return host.ExecPlan{}, fmt.Errorf("%s is headless; use a Sessionbus Grok lane", headless)
	}
	if conflict != nil {
		return host.ExecPlan{}, conflict
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

// grokHeadless reports a headless flag; arg stands where InteractivePlan reads a flag.
func grokHeadless(arg, key string) bool {
	return slices.Contains([]string{"-p", "--single", "--prompt-file", "--prompt-json", "--output-format", "--json-schema", "--max-turns", "--include-partial-messages", "--print", "--memory-flush"}, key) || strings.HasPrefix(arg, "-p") && !strings.HasPrefix(arg, "--")
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

// managedLaunch resolves a helper's managed launch: the marker is an absolute path that
// is positively a directory, the facts file in it reads whole, and native gave the
// helper a session id. Anything else leaves the helper inert: the unexpanded reference
// a plain Grok client hands on, a relative path, a regular file or any stat error.
func managedLaunch(environment []string) (string, launchFacts, bool) {
	marker := environmentValue(environment, ManagedEnv)
	if !filepath.IsAbs(marker) || environmentValue(environment, grokSessionIDEnv) == "" {
		return "", launchFacts{}, false
	}
	if info, err := os.Lstat(marker); err != nil || !info.IsDir() {
		return "", launchFacts{}, false
	}
	facts, whole := readLaunchFacts(marker)
	if !whole {
		return "", launchFacts{}, false
	}
	return marker, facts, true
}

func ManagedHelper(environment []string) bool {
	_, _, managed := managedLaunch(environment)
	return managed
}

// launchEnded reports that a managed launch is over: its launch directory, the marker,
// no longer exists. Only the launcher removes it, when it returns. Any other stat error
// is not taken as the end of the launch.
func launchEnded(marker string) bool {
	_, err := os.Stat(marker)
	return errors.Is(err, fs.ErrNotExist)
}

// launchPollInterval is how often a running managed helper checks its launch.
var launchPollInterval = 2 * time.Second

// WithManagedLaunch returns a context that is cancelled once the helper's managed
// launch has ended. Grok's leader can keep a session and its helper running after the
// launcher has quit; cancelling the helper's context ends its MCP frontend and its
// Sessionbus presence through their ordinary shutdown paths.
func WithManagedLaunch(ctx context.Context, environment []string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	marker, interval := environmentValue(environment, ManagedEnv), launchPollInterval
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if launchEnded(marker) {
					cancel()
					return
				}
			}
		}
	}()
	return ctx, cancel
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
