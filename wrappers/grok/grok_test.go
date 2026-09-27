// SPDX-License-Identifier: MIT

package grok

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/testsocket"
)

const testSessionID = "01a075aa-7c7e-7f21-87dd-28c7b43f5bbc"

func TestMain(m *testing.M) {
	if os.Getenv("GROK_TEST_CHILD") != "" {
		fakeGrok()
		os.Exit(0)
	}
	home, err := os.MkdirTemp("", "sessionbus-grok-test-home-")
	if err != nil {
		panic(err)
	}
	_ = os.Setenv("GROK_HOME", home)
	_ = os.Setenv("GROK_TEST_CHILD", "1")
	command = func(_ string, arguments ...string) *exec.Cmd {
		args := append([]string{"-test.run=^$", "--"}, arguments...)
		cmd := exec.Command(os.Args[0], args...)
		cmd.Env = append(os.Environ(), "GROK_TEST_CHILD=1")
		return cmd
	}
	code := m.Run()
	_ = os.RemoveAll(home)
	os.Exit(code)
}

func fakeGrok() {
	index := slices.Index(os.Args, "--")
	arguments := os.Args[index+1:]
	cwd, _ := os.Getwd()
	record("START", map[string]any{"pid": os.Getpid(), "arguments": arguments, "cwd": cwd, "laneSocket": os.Getenv("SESSIONBUS_LANE_SOCKET"), "environment": map[string]string{host.SocketEnv: os.Getenv(host.SocketEnv), host.GroupsEnv: os.Getenv(host.GroupsEnv), ManagedEnv: os.Getenv(ManagedEnv)}})
	if path := os.Getenv("GROK_TEST_INTERACTIVE_STARTED"); path != "" && slices.Contains(arguments, "--leader") && !slices.Contains(arguments, "stdio") {
		publishTestFile(path, []byte("started"))
	}
	if slices.Contains(arguments, "leader") && !slices.Contains(arguments, "stdio") {
		if pidPath := os.Getenv("GROK_TEST_LEADER_PID"); pidPath != "" {
			publishTestFile(pidPath, []byte(strconv.Itoa(os.Getpid())))
		}
		path := option(arguments, "--leader-socket")
		listener, err := net.Listen("unix", path)
		if err != nil {
			os.Exit(2)
		}
		defer listener.Close()
		time.Sleep(24 * time.Hour)
	}
	if slices.Contains(arguments, "--leader") && !slices.Contains(arguments, "stdio") {
		if path := os.Getenv("GROK_TEST_INTERACTIVE_PID"); path != "" {
			publishTestFile(path, []byte(strconv.Itoa(os.Getpid())))
		}
		if code, _ := strconv.Atoi(os.Getenv("GROK_TEST_INTERACTIVE_EXIT")); code != 0 {
			if path := os.Getenv("GROK_TEST_INTERACTIVE_EXIT_BARRIER"); path != "" {
				<-fileReady(path)
			}
			os.Exit(code)
		}
		time.Sleep(24 * time.Hour)
	}
	if path := os.Getenv("GROK_TEST_OBSERVER_PID"); path != "" && slices.Contains(arguments, "--leader") && slices.Contains(arguments, "stdio") {
		publishTestFile(path, []byte(strconv.Itoa(os.Getpid())))
	}
	if path := os.Getenv("GROK_TEST_DESCENDANT_PID"); path != "" {
		child := exec.Command("sleep", "3600")
		if child.Start() == nil {
			publishTestFile(path, []byte(strconv.Itoa(child.Process.Pid)))
		}
	}
	title, cancelled, rosterCalls := "", make(chan struct{}, 1), 0
	titles := strings.Split(os.Getenv("GROK_TEST_TITLES"), ",")
	var write sync.Mutex
	encoder := json.NewEncoder(os.Stdout)
	reply := func(value any) { write.Lock(); _ = encoder.Encode(value); write.Unlock() }
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), maxACPFrame)
	for scanner.Scan() {
		var request map[string]any
		_ = json.Unmarshal(scanner.Bytes(), &request)
		request["_testPID"] = os.Getpid()
		record("FRAME", request)
		method, id := request["method"], request["id"]
		params, _ := request["params"].(map[string]any)
		switch method {
		case "initialize":
			reply(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"protocolVersion": 1, "authMethods": []map[string]string{{"id": "cached_token"}}}})
		case "authenticate":
			reply(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{}})
			if path := os.Getenv("GROK_TEST_HOLD_EXIT"); path != "" && slices.Contains(arguments, "stdio") {
				<-fileReady(path)
				return
			}
		case "session/new", "session/load":
			session := testSessionID
			if loaded, ok := params["sessionId"].(string); ok {
				session = loaded
			}
			if servers, ok := params["mcpServers"].([]any); ok && len(servers) > 0 {
				server, _ := servers[0].(map[string]any)
				env, _ := server["env"].([]any)
				var endpoint string
				for _, raw := range env {
					entry, _ := raw.(map[string]any)
					if entry["name"] == "SESSIONBUS_LANE_SOCKET" {
						endpoint, _ = entry["value"].(string)
					}
				}
				if endpoint != "" {
					c, e := net.Dial("unix", endpoint)
					if e != nil {
						os.Exit(4)
					}
					defer c.Close()
					enc, dec := json.NewEncoder(c), json.NewDecoder(c)
					_ = enc.Encode(nativeHelperIdentity{session, option(arguments, "--leader-socket")})
					var ack map[string]any
					if dec.Decode(&ack) != nil {
						os.Exit(5)
					}
					_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": "2025-06-18"}})
					if dec.Decode(&ack) != nil {
						os.Exit(6)
					}
				}
			}
			if method == "session/load" {
				session = first(os.Getenv("GROK_TEST_LOAD_ID"), session)
				reply(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"models": map[string]any{}, "_meta": map[string]any{"sessionId": session, "x.ai/sessionDetail": map[string]string{"sessionId": session}}}})
				if path := os.Getenv("GROK_TEST_LOAD_EXIT_BLOCK"); path != "" {
					<-fileReady(path)
				}
			} else {
				reply(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"sessionId": session, "models": map[string]any{}}})
			}
		case "_x.ai/session/rename":
			if path := os.Getenv("GROK_TEST_RENAME_BLOCK"); path != "" {
				<-fileReady(path)
			}
			if os.Getenv("GROK_TEST_RENAME_ERROR") != "" {
				reply(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32603, "message": "rename refused"}})
				continue
			}
			title, _ = params["title"].(string)
			reply(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"success": true}})
		case "_x.ai/sessions/list":
			rosterCalls++
			if path := os.Getenv("GROK_TEST_ROSTER_BLOCK"); path != "" {
				<-fileReady(path)
			}
			if os.Getenv("GROK_TEST_ROSTER_DELAY") != "" && rosterCalls == 1 {
				reply(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"result": map[string]any{"sessions": []any{}}}})
				reply(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/sessions/changed", "params": map[string]any{}})
				continue
			}
			titleIndex := rosterCalls - 1
			if os.Getenv("GROK_TEST_ROSTER_DELAY") != "" {
				titleIndex--
			}
			_, explicitTitles := os.LookupEnv("GROK_TEST_TITLES")
			if explicitTitles && titleIndex >= 0 && titleIndex < len(titles) {
				title = titles[titleIndex]
			}
			session := first(os.Getenv("GROK_TEST_SESSION_ID"), testSessionID)
			if ids := strings.Split(os.Getenv("GROK_TEST_SESSION_IDS"), ","); len(ids) > 0 && ids[0] != "" {
				session = ids[min(rosterCalls-1, len(ids)-1)]
			}
			reply(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"result": map[string]any{"sessions": []map[string]any{{"sessionId": session, "title": title, "cwd": first(os.Getenv("GROK_TEST_CWD"), cwd), "activity": first(os.Getenv("GROK_TEST_ACTIVITY"), "working"), "resident": true, "yolo": true}}}}})
			if path := os.Getenv("GROK_TEST_ROSTER_CHANGE"); path != "" && rosterCalls == 1 {
				go func() {
					<-fileReady(path)
					reply(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/sessions/changed", "params": map[string]any{}})
				}()
			}
			if path := os.Getenv("GROK_TEST_OBSERVER_EXIT"); path != "" && slices.Contains(arguments, "stdio") {
				<-fileReady(path)
				return
			}
		case "_x.ai/interject":
			if path := os.Getenv("GROK_TEST_INTERJECT_BLOCK"); path != "" {
				<-fileReady(path)
			}
			reply(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"result": map[string]string{"status": "queued"}}})
			reply(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/session/interjection", "params": params})
		case "session/prompt":
			go func(id any, params map[string]any) {
				session, _ := params["sessionId"].(string)
				promptID := fmt.Sprintf("prompt-%v", id)
				prompt := fmt.Sprint(params["prompt"])
				values, _ := params["prompt"].([]any)
				text := ""
				if len(values) > 0 {
					v, _ := values[0].(map[string]any)
					text, _ = v["text"].(string)
				}
				reply(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/queue/changed", "params": map[string]any{"sessionId": session, "runningPromptId": promptID, "runningText": text, "runningKind": "prompt"}})
				stop := "end_turn"
				if strings.Contains(prompt, "hold") {
					select {
					case <-cancelled:
						stop = "cancelled"
					case <-released():
					}
				}
				answer := "answer"
				if size, _ := strconv.Atoi(os.Getenv("GROK_TEST_OUTPUT_SIZE")); size > 0 {
					answer = strings.Repeat("x", size)
				}
				if os.Getenv("GROK_TEST_FOREIGN_CHUNK") != "" {
					reply(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": session, "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"type": "text", "text": "foreign"}}, "_meta": map[string]string{"promptId": "foreign-prompt"}}})
				}
				reply(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": session, "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"type": "text", "text": answer}}, "_meta": map[string]string{"promptId": promptID}}})
				reply(map[string]any{"jsonrpc": "2.0", "method": "_x.ai/session_notification", "params": map[string]any{"sessionId": session, "update": map[string]any{"sessionUpdate": "turn_completed", "prompt_id": promptID, "stop_reason": stop}}})
				reply(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"stopReason": stop, "_meta": map[string]string{"promptId": promptID}}})
				if os.Getenv("GROK_TEST_EXIT_AFTER_PROMPT") != "" {
					os.Exit(0)
				}
			}(id, params)
		case "session/cancel":
			select {
			case cancelled <- struct{}{}:
			default:
			}
		case "session/close":
			if os.Getenv("GROK_TEST_CLOSE_ERROR") != "" {
				reply(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32603, "message": "close failed"}})
			} else {
				reply(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"_meta": map[string]string{"x.ai/closeOutcome": "closed"}}})
			}
		}
	}
}

func released() <-chan time.Time {
	return fileReady(os.Getenv("GROK_TEST_RELEASE"))
}

func fileReady(path string) <-chan time.Time {
	ready := make(chan time.Time, 1)
	go func() {
		for {
			if _, err := os.Stat(path); err == nil {
				ready <- time.Now()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	return ready
}

func record(kind string, value any) {
	path := os.Getenv("GROK_TEST_RECORD")
	if path == "" {
		return
	}
	body, _ := json.Marshal(map[string]any{"kind": kind, "value": value})
	file, _ := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if file != nil {
		_, _ = file.Write(append(body, '\n'))
		_ = file.Close()
	}
}

func publishTestFile(path string, body []byte) {
	temporary := fmt.Sprintf("%s.%d.tmp", path, os.Getpid())
	if os.WriteFile(temporary, body, 0o600) == nil {
		_ = os.Rename(temporary, path)
	}
}

func TestFreshLaneNativeLifecycle(t *testing.T) {
	root, recordPath := testsocket.Directory(t), filepath.Join(t.TempDir(), "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_RELEASE", filepath.Join(root, "release"))
	p := New(filepath.Join(root, "sessionbus.sock"), "single-use-token")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	request := sessionkit.OpenRequest{Name: "parent/grok@local", Groups: []string{"group"}, Open: sessionkit.OpenOptions{Cwd: root, PermissionMode: "bypassPermissions", Model: "grok-4.6", ReasoningEffort: "low", Arguments: []string{"--disable-web-search"}}}
	opened, err := p.Open(context.Background(), request)
	must(t, err)
	defer p.Close(context.Background(), sessionkit.SessionCloseRequest{})
	check(t, opened.SessionID == testSessionID, "session id = %q", opened.SessionID)
	check(t, !exists(filepath.Join(root, "locks")), "adapter session lock recreated")
	frames := records(t, recordPath)
	check(t, containsStart(frames, "--permission-mode", "bypassPermissions", "--reasoning-effort", "low", "-m", "grok-4.6", "--disable-web-search"), "typed argv not preserved")
	check(t, containsStart(frames, "--allow", "MCPTool(sessionbus__sessionbus)", "--relay-on-demand"), "lane leader omitted exact Sessionbus grant")
	check(t, countStartsContaining(frames, "--allow", "MCPTool(sessionbus__sessionbus)") == 1, "Sessionbus grant escaped the one private leader")
	check(t, containsStart(frames, "--relay-on-demand") && !containsStart(frames, "--no-exit-on-disconnect"), "leader argv did not preserve relay-on-demand")
	check(t, allStartsContain(frames, "--no-auto-update"), "an ACP client omitted --no-auto-update")
	check(t, countFrames(frames, "initialize") == 3, "authenticated startup hold absent: %d handshakes", countFrames(frames, "initialize"))
	open := findFrame(frames, "session/new")
	check(t, !strings.Contains(string(open), "--session-id") && strings.Contains(string(open), `"sessionbus"`) && strings.Contains(string(open), `"SESSIONBUS_LANE_SOCKET"`) && strings.Contains(string(open), `"yoloMode":true`), "fresh open = %s", open)
	idle, err := p.Deliver(context.Background(), delivery("idle"), nil)
	var notRunning *sessionkit.ProtocolError
	check(t, errors.As(err, &notRunning) && notRunning.Code == -32004 && idle.Disposition == "", "idle = %#v, %v", idle, err)
	check(t, countFrames(records(t, recordPath), "_x.ai/interject") == 0, "idle delivery started native work")
	must(t, p.Close(context.Background(), sessionkit.SessionCloseRequest{}))
	check(t, !exists(filepath.Join(root, "lanes", p.key+".sock")), "lane socket remains")
}

func TestInterruptAndResume(t *testing.T) {
	root := testsocket.Directory(t)
	t.Setenv("GROK_TEST_RECORD", filepath.Join(root, "record"))
	t.Setenv("GROK_TEST_RELEASE", filepath.Join(root, "never"))
	_, _, reader := startGrokWorker(t, root)
	writeWorkerRequest(t, reader, 2, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/1", "input": "hold"})
	check(t, readWorkerResponse(t, reader, 2).Error == nil, "execute admission failed")
	waitFrame(t, os.Getenv("GROK_TEST_RECORD"), "session/prompt", 1)
	writeWorkerRequest(t, reader, 3, "turn.interrupt", map[string]string{"session_id": testSessionID + "@local"})
	var terminal sessionkit.TurnResult
	must(t, json.Unmarshal(readWorkerTerminal(t, reader, 2), &terminal))
	check(t, terminal.Outcome == "interrupted", "interrupt terminal changed")
	check(t, readWorkerResponse(t, reader, 3).Result != nil, "interrupt ack absent")
	writeWorkerRequest(t, reader, 4, "session.close", map[string]string{"session_id": testSessionID + "@local"})
	closeResult := readWorkerResponse(t, reader, 4)
	check(t, closeResult.Result != nil, "close response absent: %+v", closeResult)
	p := New(filepath.Join(root, "sessionbus.sock"), "resume-token")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	_, err := p.Open(context.Background(), sessionkit.OpenRequest{Name: "lane@local", ResumeSessionID: testSessionID, Open: sessionkit.OpenOptions{Cwd: root}})
	must(t, err)
	frames := records(t, os.Getenv("GROK_TEST_RECORD"))
	load := findFrame(frames, "session/load")
	check(t, strings.Contains(string(load), `"sessionId":"`+testSessionID+`"`), "resume load = %s", load)
	check(t, !containsStart(frames, "--resume", testSessionID), "resume was selected in both argv and session/load")
	check(t, containsStart(frames, "--allow", "MCPTool(sessionbus__sessionbus)", "--relay-on-demand"), "resumed lane leader omitted exact Sessionbus grant")
	must(t, p.Close(context.Background(), sessionkit.SessionCloseRequest{}))
}

func TestResumeIdentityFailureRepliesBeforeCleanup(t *testing.T) {
	root := testsocket.Directory(t)
	// Closing ACP input may otherwise let the fake child exit successfully
	// before Kill. Force a real cleanup error for the diagnostic assertion.
	exitGate := filepath.Join(root, "release-load-exit")
	t.Setenv("GROK_TEST_LOAD_EXIT_BLOCK", exitGate)
	t.Cleanup(func() { _ = os.WriteFile(exitGate, nil, 0o600) })
	t.Setenv("GROK_TEST_RECORD", filepath.Join(root, "record"))
	t.Setenv("GROK_TEST_LOAD_ID", "different-product-id")
	socket := filepath.Join(root, "sessionbus.sock")
	listener, err := net.Listen("unix", socket)
	must(t, err)
	t.Setenv(host.SocketEnv, socket)
	t.Setenv(host.TokenEnv, "resume-error-token")
	p := New(socket, "resume-error-token")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	worker := sessionkit.NewWorker(p)
	go func() { _ = worker.Serve(context.Background()) }()
	connection, err := listener.Accept()
	must(t, err)
	t.Cleanup(func() {
		_ = connection.Close()
		<-worker.Closed()
		_ = listener.Close()
	})
	reader := &workerReader{connection: connection, requestIDs: map[int]int{}, ready: map[string]map[string]any{}, reader: bufio.NewReader(connection), pending: map[int]workerResponse{}}
	readLine(t, reader.reader)
	_, err = connection.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n"))
	must(t, err)
	writeWorkerRequest(t, reader, 1, "session.open", map[string]any{"name": "lane@local", "groups": []string{"group"}, "resume_session_id": testSessionID, "open": map[string]any{"cwd": root}})
	response := readWorkerResponse(t, reader, 1)
	check(t, strings.Contains(string(response.Error), `"message":"spawn_failed"`) && strings.Contains(string(response.Error), `Grok returned session identity`), "open error = %s", response.Error)
	check(t, strings.Contains(string(response.Error), "signal: killed"), "Open dropped cleanup diagnostic: %s", response.Error)
	_ = connection.Close()
	<-worker.Closed()
	p.mu.Lock()
	child, leader := p.child, p.leader
	p.mu.Unlock()
	select {
	case <-child.Done():
	default:
		t.Fatal("failed Open child not reaped")
	}
	select {
	case <-leader.done:
	default:
		t.Fatal("failed Open leader not reaped")
	}
	_ = listener.Close()
}

func TestOmittedCwdAndSocketReadiness(t *testing.T) {
	root, recordPath := testsocket.Directory(t), filepath.Join(t.TempDir(), "record")
	regular := filepath.Join(root, "not-a-socket")
	must(t, os.WriteFile(regular, nil, 0o600))
	check(t, !grokSocketReady(regular), "regular file reported ready")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	p := New(filepath.Join(root, "sessionbus.sock"), "cwd-token")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	_, err := p.Open(context.Background(), sessionkit.OpenRequest{Name: "lane@local"})
	must(t, err)
	want, err := os.Getwd()
	must(t, err)
	for _, frame := range records(t, recordPath) {
		if strings.Contains(string(frame), `"kind":"START"`) {
			check(t, strings.Contains(string(frame), `"cwd":`+strconv.Quote(want)), "child cwd = %s", frame)
		}
	}
	check(t, strings.Contains(string(findFrame(records(t, recordPath), "session/new")), `"cwd":`+strconv.Quote(want)), "ACP cwd was empty")
	must(t, p.Close(context.Background(), sessionkit.SessionCloseRequest{}))
}

func TestArgumentsAndHello(t *testing.T) {
	hello, err := (&Wrapper{}).Hello(context.Background())
	must(t, err)
	check(t, hello.Product == Product && reflect.DeepEqual(hello.SupportedOpenFields, []string{"cwd", "permission_mode", "model", "reasoning_effort", "arguments"}), "hello = %#v", hello)
	for _, test := range []struct{ argument, want string }{{"--model=x", "model"}, {"--resume=x", "session_id"}, {"--leader", "leader"}, {"text", "unsupported argument"}} {
		_, err := extraArguments([]string{test.argument})
		check(t, err != nil && strings.Contains(err.Error(), test.want), "%s error = %v", test.argument, err)
	}
}

func TestLaneArgumentValidationPrecedesConfigWrite(t *testing.T) {
	home := t.TempDir()
	t.Setenv("GROK_HOME", home)
	p := New(filepath.Join(testsocket.Directory(t), "sessionbus.sock"), "token")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return nil, nil })
	_, err := p.Open(context.Background(), sessionkit.OpenRequest{
		Name: "lane@local",
		Open: sessionkit.OpenOptions{Arguments: []string{"--unsupported"}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported argument") {
		t.Fatalf("invalid arguments accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(home, grokConfigFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("config written before argument validation: %v", err)
	}
}

func TestLaneConfigFailurePrecedesEndpointAndNativeStart(t *testing.T) {
	home, recordPath := t.TempDir(), filepath.Join(t.TempDir(), "record")
	t.Setenv("GROK_HOME", home)
	t.Setenv("GROK_TEST_RECORD", recordPath)
	if err := os.WriteFile(filepath.Join(home, grokConfigFile), []byte("token = PRIVATE_VALUE @\n"), 0600); err != nil {
		t.Fatal(err)
	}
	p := New(filepath.Join(testsocket.Directory(t), "sessionbus.sock"), "token")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return nil, nil })
	_, err := p.Open(context.Background(), sessionkit.OpenRequest{Name: "lane@local"})
	if err == nil || strings.Contains(err.Error(), "PRIVATE_VALUE") || !strings.Contains(err.Error(), "invalid TOML at line") {
		t.Fatalf("config failure = %v", err)
	}
	if _, err := os.Stat(recordPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native started before config validation: %v", err)
	}
}

func TestCloseReturnsNativeErrorAfterCleanup(t *testing.T) {
	root := testsocket.Directory(t)
	t.Setenv("GROK_TEST_CLOSE_ERROR", "1")
	p := New(filepath.Join(root, "sessionbus.sock"), "close-error")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	_, err := p.Open(context.Background(), sessionkit.OpenRequest{Name: "lane@local", Open: sessionkit.OpenOptions{Cwd: root}})
	must(t, err)
	err = p.Close(context.Background(), sessionkit.SessionCloseRequest{})
	check(t, err != nil && strings.Contains(err.Error(), "close failed"), "close error = %v", err)
	check(t, !exists(filepath.Join(root, "lanes", p.key+".sock")), "endpoint survived failed native close")
}

func TestCancelledCloseJoinsNativeProcesses(t *testing.T) {
	root := testsocket.Directory(t)
	p := New(filepath.Join(root, "sessionbus.sock"), "cancelled-close")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	_, err := p.Open(context.Background(), sessionkit.OpenRequest{Name: "lane@local", Open: sessionkit.OpenOptions{Cwd: root}})
	must(t, err)
	p.mu.Lock()
	watcher, leader := p.watcher, p.leader
	p.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = p.Close(ctx, sessionkit.SessionCloseRequest{})
	for _, process := range []*nativeProcess{watcher, leader} {
		select {
		case <-process.done:
		default:
			t.Fatal("Close returned before a native process was reaped")
		}
	}
}

func TestKitRunTokenCrossings(t *testing.T) {
	root, recordPath := testsocket.Directory(t), filepath.Join(t.TempDir(), "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	p, _, reader := startGrokWorker(t, root)
	p.mu.Lock()
	writeWorkerRequest(t, reader, 2, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/1", "input": "plain"})
	writeWorkerRequest(t, reader, 3, "turn.interrupt", map[string]string{"session_id": testSessionID + "@local"})
	writeWorkerRequest(t, reader, 4, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/2", "input": "second"})
	check(t, readWorkerResponse(t, reader, 4).Error != nil, "second run was not busy")
	p.mu.Unlock()
	check(t, readWorkerTerminal(t, reader, 2) != nil, "interrupted terminal absent")
	check(t, readWorkerResponse(t, reader, 3).Result != nil, "interrupt ack absent")
	check(t, countFrames(records(t, recordPath), "session/prompt") == 0, "pre-write interrupt reached Grok")
	writeWorkerRequest(t, reader, 5, "session.close", map[string]string{"session_id": testSessionID + "@local"})
	check(t, readWorkerResponse(t, reader, 5).Result != nil, "close response absent")
}

func TestIdleDeliveryJoinsOwnedPromptAndForeignChunksAreIgnored(t *testing.T) {
	root, recordPath := testsocket.Directory(t), filepath.Join(t.TempDir(), "record")
	t.Setenv("GROK_TEST_RECORD", recordPath)
	t.Setenv("GROK_TEST_FOREIGN_CHUNK", "1")
	_, _, reader := startGrokWorker(t, root)
	writeWorkerRequest(t, reader, 2, "message.deliver", delivery("idle-wire-token"))
	response := readWorkerResponse(t, reader, 2)
	check(t, response.Error != nil && strings.Contains(string(response.Error), `"code":-32004`), "idle response = %+v", response)
	check(t, countFrames(records(t, recordPath), "_x.ai/interject") == 0, "idle delivery used native interject")
	writeWorkerRequest(t, reader, 3, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/1", "input": "caller-input"})
	check(t, readWorkerResponse(t, reader, 3).Error == nil, "execute admission failed")
	var terminal sessionkit.TurnResult
	must(t, json.Unmarshal(readWorkerTerminal(t, reader, 3), &terminal))
	check(t, terminal.Outcome == "completed" && terminal.Result == "answer", "terminal = %#v", terminal)
	prompt := string(findFrame(records(t, recordPath), "session/prompt"))
	check(t, !strings.Contains(prompt, "idle-wire-token") && strings.Contains(prompt, "caller-input"), "owned prompt = %s", prompt)
	writeWorkerRequest(t, reader, 4, "session.close", map[string]string{"session_id": testSessionID + "@local"})
	closeResult := readWorkerResponse(t, reader, 4)
	check(t, closeResult.Result != nil, "close response absent: %+v", closeResult)
}

func TestChildExitWaitsForRunDoneBeforeShutdown(t *testing.T) {
	root := testsocket.Directory(t)
	t.Setenv("GROK_TEST_OUTPUT_SIZE", "300000")
	t.Setenv("GROK_TEST_EXIT_AFTER_PROMPT", "1")
	p, _, reader := startGrokWorker(t, root)
	writeWorkerRequest(t, reader, 2, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/1", "input": "plain"})
	check(t, readWorkerResponse(t, reader, 2).Error == nil, "execute admission failed")
	<-p.child.Done()
	ready := readWorkerReady(t, reader, 2)
	check(t, ready["state"] == "unavailable" && ready["reason"] == "native result failed validation", "oversized result metadata before EOF = %v", ready)
	// Worker retirement invalidates its cursor; output length is covered by
	// the native reader tests, not by an old terminal-body RPC here.
	_, err := reader.reader.ReadBytes('\n')
	check(t, errors.Is(err, io.EOF), "worker did not shut down after terminal: %v", err)
}

type workerResponse struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

type workerRead struct {
	response workerResponse
	err      error
}

type workerReader struct {
	frames     <-chan workerRead
	connection net.Conn
	nextID     int
	requestIDs map[int]int
	ready      map[string]map[string]any
	reader     *bufio.Reader
	pending    map[int]workerResponse
}

func startGrokWorker(t *testing.T, root string) (*Wrapper, net.Conn, *workerReader) {
	socket := filepath.Join(root, "sessionbus.sock")
	listener, err := net.Listen("unix", socket)
	must(t, err)
	t.Setenv(host.SocketEnv, socket)
	t.Setenv(host.TokenEnv, "worker-token")
	p := New(socket, "worker-token")
	p.SetCall(func(context.Context, string, any) (json.RawMessage, error) { return json.RawMessage(`{}`), nil })
	worker := sessionkit.NewWorker(p)
	p.SetShutdown(worker.Shutdown)
	go func() { _ = worker.Serve(context.Background()) }()
	connection, err := listener.Accept()
	must(t, err)
	reader := &workerReader{connection: connection, requestIDs: map[int]int{}, ready: map[string]map[string]any{}, reader: bufio.NewReader(connection), pending: map[int]workerResponse{}}
	readLine(t, reader.reader)
	_, err = connection.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n"))
	must(t, err)
	writeWorkerRequest(t, reader, 1, "session.open", map[string]any{"name": "parent/grok@local", "groups": []string{"group"}, "open": map[string]any{"cwd": root}})
	opened := readWorkerResponse(t, reader, 1)
	check(t, opened.Result != nil, "open failed: %s", opened.Error)
	t.Cleanup(func() { _ = connection.Close(); <-worker.Closed(); _ = listener.Close() })
	return p, connection, reader
}

func writeWorkerRequest(t *testing.T, reader *workerReader, id int, method string, params any) {
	reader.nextID++
	reader.requestIDs[reader.nextID] = id
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": reader.nextID, "method": method, "params": params})
	must(t, err)
	_, err = reader.connection.Write(append(body, '\n'))
	must(t, err)
}

func readWorkerResponse(t *testing.T, reader *workerReader, id int) workerResponse {
	if response, ok := reader.pending[id]; ok {
		delete(reader.pending, id)
		return response
	}
	for {
		response := reader.readResponse(t)
		if workerReady(t, reader, response) {
			continue
		}
		response.ID = reader.requestIDs[response.ID]
		if response.ID == id {
			return response
		}
		reader.pending[response.ID] = response
	}
}

// Continuation fixtures may own an asynchronous frame reader so an early bus
// response can be observed while waiting for the native observer. All other
// fixtures keep their original synchronous reader.
func (reader *workerReader) readResponse(t *testing.T) workerResponse {
	t.Helper()
	if reader.frames != nil {
		next, ok := <-reader.frames
		if !ok {
			t.Fatal("worker frame reader closed")
		}
		must(t, next.err)
		return next.response
	}
	var response workerResponse
	must(t, json.Unmarshal(readLine(t, reader.reader), &response))
	return response
}

func workerReady(t *testing.T, reader *workerReader, response workerResponse) bool {
	if response.Method != "turn.ready" {
		return false
	}
	var ready map[string]any
	must(t, json.Unmarshal(response.Params, &ready))
	reader.ready[ready["run_id"].(string)] = ready
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": response.ID, "result": map[string]any{}})
	must(t, err)
	_, err = reader.connection.Write(append(body, '\n'))
	must(t, err)
	return true
}
func readWorkerReady(t *testing.T, reader *workerReader, id int) map[string]any {
	return readWorkerReadyID(t, reader, "g/1")
}
func readWorkerReadyID(t *testing.T, reader *workerReader, key string) map[string]any {
	for reader.ready[key] == nil {
		response := reader.readResponse(t)
		if !workerReady(t, reader, response) {
			response.ID = reader.requestIDs[response.ID]
			reader.pending[response.ID] = response
		}
	}
	return reader.ready[key]
}
func readWorkerTerminal(t *testing.T, reader *workerReader, id int) json.RawMessage {
	readWorkerReady(t, reader, id)
	writeWorkerRequest(t, reader, 1000+id, "turn.status", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/1"})
	response := readWorkerResponse(t, reader, 1000+id)
	var status sessionkit.RunStatus
	must(t, json.Unmarshal(response.Result, &status))
	check(t, status.Result != nil, "missing result: %s", response.Result)
	result, err := json.Marshal(status.Result)
	must(t, err)
	return result
}

func readLine(t *testing.T, reader *bufio.Reader) []byte {
	t.Helper()
	line, err := reader.ReadBytes('\n')
	must(t, err)
	return line
}

func delivery(body string) sessionkit.DeliveryRequest {
	return sessionkit.DeliveryRequest{MessageID: "message-1", From: sessionkit.DeliverySource{SessionID: "source@local", Name: "source@local", Product: "example", Groups: []string{"group"}}, Body: body}
}

func waitFrame(t *testing.T, path, method string, count int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		found := 0
		for _, raw := range records(t, path) {
			if strings.Contains(string(raw), `"method":"`+method+`"`) {
				found++
			}
		}
		if found >= count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("did not observe %s", method)
}

func records(t *testing.T, path string) []json.RawMessage {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	result := make([]json.RawMessage, 0, len(lines))
	for _, line := range lines {
		if line != "" {
			result = append(result, json.RawMessage(line))
		}
	}
	return result
}

func findFrame(records []json.RawMessage, method string) json.RawMessage {
	for _, raw := range records {
		if strings.Contains(string(raw), `"method":"`+method+`"`) {
			return raw
		}
	}
	return nil
}

func countFrames(records []json.RawMessage, method string) int {
	count := 0
	for _, raw := range records {
		if strings.Contains(string(raw), `"method":"`+method+`"`) {
			count++
		}
	}
	return count
}

func containsStart(records []json.RawMessage, values ...string) bool {
	for _, raw := range records {
		if !strings.Contains(string(raw), `"kind":"START"`) {
			continue
		}
		body := string(raw)
		if slices.ContainsFunc(values, func(value string) bool { return !strings.Contains(body, value) }) {
			continue
		}
		return true
	}
	return false
}

func countStartsContaining(records []json.RawMessage, values ...string) int {
	count := 0
	for _, raw := range records {
		if !strings.Contains(string(raw), `"kind":"START"`) {
			continue
		}
		body := string(raw)
		if slices.ContainsFunc(values, func(value string) bool { return !strings.Contains(body, value) }) {
			continue
		}
		count++
	}
	return count
}

func allStartsContain(records []json.RawMessage, value string) bool {
	found := false
	for _, raw := range records {
		if !strings.Contains(string(raw), `"kind":"START"`) {
			continue
		}
		found = true
		if !strings.Contains(string(raw), value) {
			return false
		}
	}
	return found
}

func option(arguments []string, name string) string {
	for index, argument := range arguments {
		if argument == name && index+1 < len(arguments) {
			return arguments[index+1]
		}
	}
	return ""
}

func exists(path string) bool { _, err := os.Stat(path); return err == nil }
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func check(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(format, args...)
	}
}

var _ = syscall.SIGTERM

func TestCompletedWorkerRunIsRetiredAtNextAdmission(t *testing.T) {
	for _, mode := range []string{"direct", "seed"} {
		t.Run(mode, func(t *testing.T) {
			recordPath := filepath.Join(t.TempDir(), "record")
			t.Setenv("GROK_TEST_RECORD", recordPath)
			p, _, reader := startGrokWorker(t, testsocket.Directory(t))
			writeWorkerRequest(t, reader, 10, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/1", "input": "first"})
			check(t, readWorkerResponse(t, reader, 10).Error == nil, "first run refused")
			check(t, readWorkerReadyID(t, reader, "g/1")["state"] == "done", "first run incomplete")
			p.mu.Lock()
			previous := p.run
			p.mu.Unlock()
			check(t, previous != nil, "completed owner was not retained for synchronous admission")
			<-previous.Done() // Real Worker publication, no synthetic Run/private SDK fields.
			if mode == "seed" {
				d := delivery("second-seeded")
				d.RunID = "g/2"
				writeWorkerRequest(t, reader, 12, "message.deliver", d)
				response := readWorkerResponse(t, reader, 12)
				check(t, response.Error == nil && strings.Contains(string(response.Result), "injected"), "seeded run refused: %s %s", response.Error, response.Result)
			} else {
				writeWorkerRequest(t, reader, 12, "turn.execute", map[string]any{"session_id": testSessionID + "@local", "run_id": "g/2", "input": "second"})
				check(t, readWorkerResponse(t, reader, 12).Error == nil, "second run refused")
			}
			ready := readWorkerReadyID(t, reader, "g/2")
			check(t, ready["state"] == "done", "second run failed: %#v", ready)
			p.mu.Lock()
			replacement := p.run
			p.mu.Unlock()
			check(t, replacement != nil && replacement != previous, "old completion cleared replacement owner")
			writeWorkerRequest(t, reader, 13, "session.close", map[string]string{"session_id": testSessionID + "@local"})
			check(t, readWorkerResponse(t, reader, 13).Error == nil, "close failed")
		})
	}
}
