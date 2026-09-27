// SPDX-License-Identifier: MIT
package grok

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/testsocket"
)

// Native agent mode reads its model only from the agent subcommand's own -m;
// a top-level -m is parsed for the TUI and ignored by agent mode. The native
// grammar proof is in docs/designs/grok-0.5.0/LANE-MODEL-PLACEMENT.md.

func TestLaneTypedModelFollowsNativeAgentSubcommand(t *testing.T) {
	request := sessionkit.OpenRequest{Open: sessionkit.OpenOptions{
		PermissionMode:  "bypassPermissions",
		Model:           "test-model",
		ReasoningEffort: "low",
		Arguments:       []string{"--agent", "architect", "--disable-web-search", "--no-plan", "--no-subagents"},
	}}
	got, err := launchArguments(request, "/tmp/sessionbus-grok-leader.sock")
	must(t, err)
	want := []string{
		"--no-auto-update",
		"--permission-mode", "bypassPermissions",
		"--reasoning-effort", "low",
		"--agent", "architect", "--disable-web-search", "--no-plan", "--no-subagents",
		"--leader-socket", "/tmp/sessionbus-grok-leader.sock",
		"agent", "-m", "test-model", "--leader", "stdio",
	}
	check(t, slices.Equal(got, want), "launch arguments = %#v, want %#v", got, want)
}

func TestLaneArgumentsWithoutTypedModelKeepPlacement(t *testing.T) {
	for _, test := range []struct {
		name string
		open sessionkit.OpenOptions
		want []string
	}{
		{"empty", sessionkit.OpenOptions{}, []string{"--no-auto-update", "--leader-socket", "/tmp/leader.sock", "agent", "--leader", "stdio"}},
		{"typed and extras", sessionkit.OpenOptions{PermissionMode: "default", ReasoningEffort: "high", Arguments: []string{"--disable-web-search"}},
			[]string{"--no-auto-update", "--permission-mode", "default", "--reasoning-effort", "high", "--disable-web-search", "--leader-socket", "/tmp/leader.sock", "agent", "--leader", "stdio"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := launchArguments(sessionkit.OpenRequest{Open: test.open}, "/tmp/leader.sock")
			must(t, err)
			check(t, slices.Equal(got, test.want), "launch arguments = %#v, want %#v", got, test.want)
		})
	}
}

func TestLaneSpawnPassesTypedModelOnlyToPrimaryAgent(t *testing.T) {
	root, recordPath := testsocket.Directory(t), filepath.Join(t.TempDir(), "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	p := New(filepath.Join(root, "sessionbus.sock"), "typed-model-placement")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	_, err := p.Open(context.Background(), sessionkit.OpenRequest{Name: "lane@local", Open: sessionkit.OpenOptions{Cwd: root, Model: "grok-test-model"}})
	must(t, err)
	defer p.Close(context.Background(), sessionkit.SessionCloseRequest{})
	frames := records(t, recordPath)
	withModel := 0
	for _, raw := range frames {
		var record struct {
			Kind  string `json:"kind"`
			Value struct {
				Arguments []string `json:"arguments"`
			} `json:"value"`
		}
		if json.Unmarshal(raw, &record) != nil || record.Kind != "START" {
			continue
		}
		arguments := record.Value.Arguments
		index := slices.Index(arguments, "-m")
		if index < 0 {
			continue
		}
		withModel++
		check(t, index > 0 && arguments[index-1] == "agent" && index+1 < len(arguments) && arguments[index+1] == "grok-test-model" &&
			slices.Equal(arguments[index+2:], []string{"--leader", "stdio"}), "native model is not an agent argument: %#v", arguments)
	}
	check(t, withModel == 1, "typed model reached %d native processes, want only the primary client", withModel)
	open := findFrame(frames, "session/new")
	check(t, open != nil && !strings.Contains(string(open), "modelId"), "wrapper must leave modelId to native leader injection: %s", open)
}
