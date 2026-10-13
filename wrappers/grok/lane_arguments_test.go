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
	got, err := launchArguments(request)
	must(t, err)
	want := []string{
		"--permission-mode", "bypassPermissions",
		"--reasoning-effort", "low",
		"--agent", "architect", "--disable-web-search", "--no-plan", "--no-subagents",
		"agent", "-m", "test-model", "--no-leader", "stdio",
	}
	check(t, slices.Equal(got, want), "launch arguments = %#v, want %#v", got, want)
}

func TestLaneArgumentsWithoutTypedModelKeepPlacement(t *testing.T) {
	for _, test := range []struct {
		name string
		open sessionkit.OpenOptions
		want []string
	}{
		{"empty", sessionkit.OpenOptions{}, []string{"agent", "--no-leader", "stdio"}},
		{"typed and extras", sessionkit.OpenOptions{PermissionMode: "default", ReasoningEffort: "high", Arguments: []string{"--disable-web-search"}},
			[]string{"--permission-mode", "default", "--reasoning-effort", "high", "--disable-web-search", "agent", "--no-leader", "stdio"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := launchArguments(sessionkit.OpenRequest{Open: test.open})
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
			slices.Equal(arguments[index+2:], []string{"--no-leader", "stdio"}), "native model is not an agent argument: %#v", arguments)
	}
	check(t, withModel == 1, "typed model reached %d native processes, want the lane's one", withModel)
	open := findFrame(frames, "session/new")
	check(t, open != nil && strings.Contains(string(open), `"modelId":"grok-test-model"`), "fresh lane's session request does not name its typed model: %s", open)
}

// Only a fresh lane with a typed model names it in its session request: an
// untyped lane leaves the choice to native and a resumed session keeps its own.
func TestLaneSessionRequestNamesOnlyAFreshTypedModel(t *testing.T) {
	for _, test := range []struct{ name, resume, model, method string }{
		{"untyped", "", "", "session/new"},
		{"resumed", testSessionID, "grok-test-model", "session/load"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, recordPath := testsocket.Directory(t), filepath.Join(t.TempDir(), "record")
			t.Setenv("GROK_TEST_RECORD", recordPath)
			t.Setenv("HOME", t.TempDir())
			p := New(filepath.Join(root, "sessionbus.sock"), "session-model-"+test.name)
			p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
			_, err := p.Open(context.Background(), sessionkit.OpenRequest{Name: "lane@local", ResumeSessionID: test.resume, Open: sessionkit.OpenOptions{Cwd: root, Model: test.model}})
			must(t, err)
			defer p.Close(context.Background(), sessionkit.SessionCloseRequest{})
			request := findFrame(records(t, recordPath), test.method)
			check(t, request != nil && !strings.Contains(string(request), "modelId"), "%s names a model: %s", test.method, request)
		})
	}
}
