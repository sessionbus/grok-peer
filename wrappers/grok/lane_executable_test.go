// SPDX-License-Identifier: MIT
package grok

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/testsocket"
)

// The tests in this file run the real lookup. Every file named grok is a stub
// in the test's own temporary directories, both homes are such directories too,
// and no test starts a stub: a command is built only to read what it would run.

// stubGrok makes an executable file named grok in directory.
func stubGrok(t *testing.T, directory string) string {
	t.Helper()
	must(t, os.MkdirAll(directory, 0o700))
	path := filepath.Join(directory, "grok")
	must(t, os.WriteFile(path, []byte("#!/bin/sh\nexit 97\n"), 0o700))
	return path
}

const grokNotOnPath = `exec: "grok": executable file not found in $PATH`

func TestLaneRunsGrokFromPathWhenPathHasOne(t *testing.T) {
	path, home := t.TempDir(), t.TempDir()
	onPath := stubGrok(t, path)
	stubGrok(t, filepath.Join(home, "bin"))
	t.Setenv("PATH", path)
	name := grokExecutable([]string{"HOME=" + t.TempDir(), "GROK_HOME=" + home}, t.TempDir())
	check(t, name == "grok", "PATH hit selected %q", name)
	built := exec.Command(name)
	check(t, built.Err == nil && built.Path == onPath && built.Args[0] == "grok", "PATH hit builds %q %q (%v)", built.Path, built.Args, built.Err)
}

func TestLaneRunsGrokFromItsOwnHomeWhenPathHasNone(t *testing.T) {
	for name, layout := range map[string]func(home, other, cwd string) (environment []string, directory string){
		"GROK_HOME": func(home, other, _ string) ([]string, string) {
			return []string{"HOME=" + other, "GROK_HOME=" + home}, filepath.Join(home, "bin")
		},
		"default under HOME": func(home, _, _ string) ([]string, string) {
			return []string{"HOME=" + home, "GROK_HOME="}, filepath.Join(home, ".grok", "bin")
		},
		// A relative home is the native child's, so it lies under the lane's
		// working directory, not under the worker's.
		"relative GROK_HOME": func(_, other, cwd string) ([]string, string) {
			return []string{"HOME=" + other, "GROK_HOME=relative-grok-home"}, filepath.Join(cwd, "relative-grok-home", "bin")
		},
	} {
		t.Run(name, func(t *testing.T) {
			cwd := t.TempDir()
			environment, directory := layout(t.TempDir(), t.TempDir(), cwd)
			installed := stubGrok(t, directory)
			t.Setenv("PATH", t.TempDir())
			selected := grokExecutable(environment, cwd)
			check(t, selected == installed, "selected %q, want %q", selected, installed)
			built := exec.Command(selected)
			check(t, built.Err == nil && built.Path == installed && built.Args[0] == installed, "builds %q %q (%v)", built.Path, built.Args, built.Err)
		})
	}
}

func TestLaneStartsEveryRoleFromGroksOwnHome(t *testing.T) {
	root, home := testsocket.Directory(t), t.TempDir()
	installed := stubGrok(t, filepath.Join(home, "bin"))
	t.Setenv("GROK_HOME", home)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	// The package's fake native ignores the name it is given; record it first.
	var mu sync.Mutex
	var selected []string
	previous := command
	command = func(name string, arguments ...string) *exec.Cmd {
		mu.Lock()
		selected = append(selected, name)
		mu.Unlock()
		return previous(name, arguments...)
	}
	t.Cleanup(func() { command = previous })
	p := New(filepath.Join(root, "sessionbus.sock"), "native-home")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	_, err := p.Open(context.Background(), sessionkit.OpenRequest{Name: "lane@local", Open: sessionkit.OpenOptions{Cwd: root}})
	must(t, err)
	must(t, p.Close(context.Background(), sessionkit.SessionCloseRequest{}))
	mu.Lock()
	defer mu.Unlock()
	// Private leader, startup hold, primary and observer.
	check(t, len(selected) == 4 && !slices.ContainsFunc(selected, func(name string) bool { return name != installed }), "lane starts ran %q, want four of %s", selected, installed)
}

func TestLaneKeepsRefusalOfRelativePathHit(t *testing.T) {
	directory, home := t.TempDir(), t.TempDir()
	stubGrok(t, directory)
	stubGrok(t, filepath.Join(home, "bin"))
	t.Chdir(directory)
	t.Setenv("PATH", ".")
	name := grokExecutable([]string{"HOME=" + t.TempDir(), "GROK_HOME=" + home}, directory)
	check(t, name == "grok", "relative PATH hit selected %q", name)
	built := exec.Command(name)
	check(t, errors.Is(built.Err, exec.ErrDot), "relative PATH hit builds %q (%v)", built.Path, built.Err)
}

func TestLaneKeepsMissingErrorWithoutUsableGrok(t *testing.T) {
	for name, prepare := range map[string]func(t *testing.T, installed string){
		"missing":        func(*testing.T, string) {},
		"not executable": func(t *testing.T, installed string) { must(t, os.WriteFile(installed, []byte("#!/bin/sh\n"), 0o600)) },
		"directory":      func(t *testing.T, installed string) { must(t, os.Mkdir(installed, 0o700)) },
		"dangling link":  func(t *testing.T, installed string) { must(t, os.Symlink(installed+"-absent", installed)) },
	} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			must(t, os.Mkdir(filepath.Join(home, "bin"), 0o700))
			prepare(t, filepath.Join(home, "bin", "grok"))
			t.Setenv("PATH", t.TempDir())
			selected := grokExecutable([]string{"HOME=" + t.TempDir(), "GROK_HOME=" + home}, t.TempDir())
			check(t, selected == "grok", "selected %q", selected)
			built := exec.Command(selected)
			check(t, errors.Is(built.Err, exec.ErrNotFound) && built.Err.Error() == grokNotOnPath, "builds %q (%v)", built.Path, built.Err)
		})
	}
}
