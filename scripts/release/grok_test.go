// SPDX-License-Identifier: MIT
package release_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGrokReinstallUsesNativeRepositoryInventory(t *testing.T) {
	home, src, bin := t.TempDir(), t.TempDir(), t.TempDir()
	script, err := os.ReadFile("install-product")
	if err != nil {
		t.Fatal(err)
	}
	put := func(p, s string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(s), 0700); err != nil {
			t.Fatal(err)
		}
	}
	put(filepath.Join(src, "install"), string(script))
	for _, f := range []string{"LICENSE", "SOURCE.txt", "THIRD-PARTY-NOTICES.txt", "README.md", "grok-peer", "plugin/manifest"} {
		put(filepath.Join(src, f), "fixture")
	}
	put(filepath.Join(src, "ROLE"), "grok")
	put(filepath.Join(bin, "grok"), `#!/bin/sh
set -eu
case "$2" in
 list) if [ -f "$HOME/native-repo" ]; then printf '[{"name":"sessionbus","repo_key":"plugin-1234"}]\n'; else printf '[]\n'; fi;;
 uninstall) test "$3" = sessionbus; test "$4" = --keep-data; rm "$HOME/native-repo";;
 install) test ! -f "$HOME/native-repo"; printf installed > "$HOME/native-repo";;
 add) test "$1" = mcp; test -f "$HOME/native-repo"; test ! -f "$HOME/refuse-add" || { echo 'native refused the registration' >&2; exit 3; }; printf '%s\n' "$@" > "$HOME/native-mcp";;
 *) exit 4;;
esac
`)
	root := filepath.Join(home, ".local/libexec/sessionbus/grok")
	put(filepath.Join(root, "plugin/skills/obsolete/SKILL.md"), "old")
	put(filepath.Join(home, "unrelated"), "keep")
	for i := 0; i < 2; i++ {
		c := exec.Command("sh", filepath.Join(src, "install"))
		c.Env = append(os.Environ(), "HOME="+home, "PATH="+bin+":"+os.Getenv("PATH"))
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("install %d: %v %s", i, err, out)
		}
		if target, err := os.Readlink(filepath.Join(root, "grok-peer-mcp")); err != nil || target != "grok-peer" {
			t.Fatalf("private alias %q: %v", target, err)
		}
		if _, err := os.Stat(filepath.Join(root, "plugin/skills/obsolete/SKILL.md")); !os.IsNotExist(err) {
			t.Fatalf("obsolete payload survived: %v", err)
		}
		if data, err := os.ReadFile(filepath.Join(home, "unrelated")); err != nil || string(data) != "keep" {
			t.Fatalf("unrelated data changed: %q %v", data, err)
		}
		// The helper's registration reaches native after the plugin install: only the
		// managed marker is a reference, unexpanded, and the other seven names are empty.
		registered, err := os.ReadFile(filepath.Join(home, "native-mcp"))
		want := strings.Join([]string{"mcp", "add", "sessionbus", "-s", "user",
			"-e", "SESSIONBUS_GROK_MANAGED=${SESSIONBUS_GROK_MANAGED}",
			"-e", "SESSIONBUS_GROUPS=", "-e", "SESSIONBUS_SESSION_NAME=", "-e", "SESSIONBUS_SOCKET=",
			"-e", "SESSIONBUS_LANE_SOCKET=", "-e", "SESSIONBUS_LAUNCH_TOKEN=", "-e", "SESSIONBUS_SESSION_ID=",
			"-e", "SESSIONBUS_LOCAL_KEY=", "--", filepath.Join(root, "grok-peer-mcp")}, "\n") + "\n"
		if err != nil || string(registered) != want {
			t.Fatalf("install %d registered %q (%v), want %q", i, registered, err, want)
		}
		if err := os.Remove(filepath.Join(home, "native-mcp")); err != nil {
			t.Fatal(err)
		}
	}
	// A refused registration is the installer's own failure: native's message and a
	// non-zero status, no success line. Running the installer again completes it.
	put(filepath.Join(home, "refuse-add"), "")
	c := exec.Command("sh", filepath.Join(src, "install"))
	c.Env = append(os.Environ(), "HOME="+home, "PATH="+bin+":"+os.Getenv("PATH"))
	out, err := c.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "native refused the registration") || strings.Contains(string(out), "Installed ") {
		t.Fatalf("refused registration: %v %s", err, out)
	}
}
