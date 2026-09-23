// SPDX-License-Identifier: MIT
package grok

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/mcp"
	"github.com/sessionbus/peer-common/testsocket"
)

func TestInitialNameClaimIsEmptyAndExclusivePerLaunch(t *testing.T) {
	for range 2 {
		root := testsocket.Directory(t)
		leader := filepath.Join(root, "leader.sock")
		env := managedPeerEnv(os.Environ(), testSessionID, leader)
		env = setEnvironment(env, host.NameEnv, "initial")
		invalid := setEnvironment(env, grokLeaderSocketEnv, filepath.Join(root, "foreign.sock"))
		if b, err := NewPeerBackend(context.Background(), invalid); err == nil {
			b.Shutdown()
			t.Fatal("foreign leader consumed claim")
		}
		check(t, !exists(filepath.Join(root, "initial-name.claim")), "invalid helper consumed name")
		var wg sync.WaitGroup
		results := make(chan *PeerBackend, 2)
		for i := range 2 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				e := setEnvironment(env, grokSessionIDEnv, []string{testSessionID, "second-native-id"}[i])
				b, err := NewPeerBackend(context.Background(), e)
				if err != nil {
					t.Error(err)
					return
				}
				results <- b
			}(i)
		}
		wg.Wait()
		close(results)
		winners := 0
		for b := range results {
			if b.initialName != "" {
				winners++
			}
			b.Shutdown()
		}
		check(t, winners == 1, "name winners=%d", winners)
		info, err := os.Stat(filepath.Join(root, "initial-name.claim"))
		must(t, err)
		check(t, info.Size() == 0 && info.Mode().Perm() == 0600, "claim retained session metadata or loose permissions")
	}
}
func TestMCPInitializationPrecedesHeldInitialNativeRename(t *testing.T) {
	root := testsocket.Directory(t)
	socket := filepath.Join(root, "bus")
	recordPath := filepath.Join(root, "record")
	release := filepath.Join(root, "rename-release")
	server, hellos := fakeDaemon(t, socket)
	defer server.Close()
	t.Setenv(host.SocketEnv, socket)
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_RENAME_BLOCK", release)
	env := setEnvironment(managedPeerEnv(os.Environ(), testSessionID, filepath.Join(root, "leader.sock")), host.NameEnv, "requested-initial")
	b, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	defer b.Shutdown()
	input, writer := io.Pipe()
	reader, output := io.Pipe()
	served := make(chan error, 1)
	go func() { served <- mcp.ServeSessionbus(b, input, output, mcp.ReportHandler{}) }()
	enc, scan := json.NewEncoder(writer), bufio.NewScanner(reader)
	response := mcpResponse(t, enc, scan, 1, "initialize", map[string]any{"protocolVersion": "2025-06-18"})
	check(t, response["result"] != nil, "native initialize withheld by rename")
	waitFrame(t, recordPath, "_x.ai/session/rename", 1)
	check(t, mcpResponse(t, enc, scan, 2, "tools/list", map[string]any{})["result"] != nil, "held native rename blocks MCP reader")
	select {
	case <-hellos:
		t.Fatal("invented title published before rename finished")
	default:
	}
	must(t, os.WriteFile(release, nil, 0600))
	hello := <-hellos
	check(t, hello.SessionID == testSessionID && hello.Name == "requested-initial", "name applied to wrong identity: %+v", hello)
	hello.ack <- true
	<-b.ready
	must(t, writer.Close())
	must(t, <-served)
}
func TestFailedInitialRenameNeverTransfersClaim(t *testing.T) {
	root := testsocket.Directory(t)
	leader := filepath.Join(root, "leader.sock")
	t.Setenv("GROK_TEST_SESSION_ID", testSessionID)
	t.Setenv("GROK_TEST_RENAME_ERROR", "1")
	env := setEnvironment(managedPeerEnv(os.Environ(), testSessionID, leader), host.NameEnv, "requested")
	b, err := NewPeerBackend(context.Background(), env)
	must(t, err)
	b.Initialized()
	<-b.done
	check(t, b.err != nil, "failed native rename accepted")
	later, err := NewPeerBackend(context.Background(), setEnvironment(env, grokSessionIDEnv, "later-native-id"))
	must(t, err)
	defer later.Shutdown()
	check(t, later.initialName == "", "failed rename transferred to later helper")
}
