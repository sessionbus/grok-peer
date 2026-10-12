// SPDX-License-Identifier: MIT
package grok

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPackagedPrivateEntryIsInertAndOnlyGenericSkillIsInstalled(t *testing.T) {
	root, err := filepath.Abs("../..")
	must(t, err)
	stage := t.TempDir()
	build := exec.Command("sh", "scripts/package-product", "grok", stage)
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("package: %v %s", err, out)
	}
	archive := filepath.Join(stage, "grok-peer-"+runtime.GOOS+"-"+runtime.GOARCH+".tar.gz")
	unpack := exec.Command("tar", "-xzf", archive, "-C", stage)
	if out, err := unpack.CombinedOutput(); err != nil {
		t.Fatalf("unpack: %v %s", err, out)
	}
	target, err := os.Readlink(filepath.Join(stage, PrivateAlias))
	must(t, err)
	check(t, target == Product, "alias target=%s", target)
	skills, err := os.ReadDir(filepath.Join(stage, "plugin/skills"))
	must(t, err)
	check(t, len(skills) == 1 && skills[0].Name() == "sessionbus", "active skills=%v", skills)
	// The plugin carries the skill only. A helper definition in it would be expanded
	// in the leader, with the values of whichever launch started that leader.
	for _, removed := range []string{"plugin/.mcp.json", "plugin/scripts"} {
		_, err := os.Lstat(filepath.Join(stage, removed))
		check(t, os.IsNotExist(err), "package still holds %s: %v", removed, err)
	}
	// Execute the actual shipped alias as the registration starts it for a plain Grok
	// client: the marker's reference arrives unexpanded and the other names are empty.
	cmd := exec.Command(filepath.Join(stage, PrivateAlias))
	cmd.Env = append(os.Environ(), ManagedEnv+"=${"+ManagedEnv+"}", "SESSIONBUS_GROUPS=", "SESSIONBUS_SESSION_NAME=", "SESSIONBUS_SOCKET=", "SESSIONBUS_LANE_SOCKET=", "SESSIONBUS_LAUNCH_TOKEN=", "SESSIONBUS_SESSION_ID=", "SESSIONBUS_LOCAL_KEY=", "GROK_SESSION_ID="+testSessionID)
	in, err := cmd.StdinPipe()
	must(t, err)
	out, err := cmd.StdoutPipe()
	must(t, err)
	cmd.Stderr = os.Stderr
	must(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	enc, dec := json.NewEncoder(in), json.NewDecoder(out)
	for _, request := range []map[string]any{
		{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": "2025-06-18"}},
		{"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": map[string]any{}},
	} {
		must(t, enc.Encode(request))
		var reply struct {
			ID     int `json:"id"`
			Result struct {
				Tools []json.RawMessage `json:"tools"`
			} `json:"result"`
			Error json.RawMessage `json:"error"`
		}
		must(t, dec.Decode(&reply))
		check(t, reply.ID == request["id"] && len(reply.Error) == 0, "unexpected reply=%+v", reply)
		if reply.ID == 2 {
			check(t, reply.Result.Tools != nil && len(reply.Result.Tools) == 0, "ordinary tools=%s", reply.Result.Tools)
		}
	}
	must(t, in.Close())
	must(t, cmd.Wait())
}
