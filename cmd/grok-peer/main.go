// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/sessionbus/grok-peer/wrappers/grok"
	"github.com/sessionbus/peer-common/host"
	"github.com/sessionbus/peer-common/mcp"
	"github.com/sessionbus/peer-common/peerversion"
)

func main() {
	arguments, report, handled := peerversion.Resolve("grok-peer", os.Args[0], os.Args[1:])
	if handled {
		fmt.Fprintln(os.Stdout, report)
		return
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer func() { signal.Stop(signals); cancel(nil) }()
	go func() {
		select {
		case caught := <-signals:
			cancel(signalCause{caught})
		case <-ctx.Done():
		}
	}()
	if err := runEntry(ctx, filepath.Base(os.Args[0]), arguments); err != nil {
		cancel(nil)
		var exited *exec.ExitError
		if errors.As(err, &exited) {
			if status, ok := exited.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				signal.Reset(status.Signal())
				_ = syscall.Kill(os.Getpid(), status.Signal())
			}
			os.Exit(exited.ExitCode())
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type signalCause struct{ signal os.Signal }

func (s signalCause) Error() string           { return s.signal.String() }
func (s signalCause) CaughtSignal() os.Signal { return s.signal }

func runEntry(ctx context.Context, basename string, arguments []string) error {
	if basename == grok.PrivateAlias {
		if len(arguments) != 0 {
			return errors.New("private MCP entry accepts no arguments")
		}
		return runMCP(ctx)
	}
	return run(ctx, arguments)
}

func run(ctx context.Context, arguments []string) error {
	if !host.LaneMode() {
		plan, err := grok.InteractivePlan(arguments, os.Environ())
		if err != nil {
			return err
		}
		return grok.RunInteractive(ctx, plan)
	}
	if len(arguments) != 0 {
		return errors.New("lane mode accepts no arguments")
	}
	product := grok.New(os.Getenv(host.SocketEnv), os.Getenv(host.TokenEnv))
	worker := sessionkit.NewWorker(product)
	product.SetShutdown(worker.Shutdown)
	product.SetCaller(worker.Caller())
	return worker.Serve(ctx)
}

func runMCP(ctx context.Context) error {
	if os.Getenv(mcp.LaneSocketEnv) == "" && !grok.ManagedHelper(os.Environ()) {
		stop := context.AfterFunc(ctx, func() { _ = os.Stdin.Close() })
		defer stop()
		return mcp.ServeInactiveSessionbus(os.Stdin, os.Stdout)
	}
	if path := os.Getenv(mcp.LaneSocketEnv); path != "" {
		return grok.ForwardLane(ctx, path, os.Stdin, os.Stdout)
	}

	backend, err := grok.NewPeerBackend(ctx, os.Environ())
	if err != nil {
		return err
	}
	defer backend.Shutdown()
	return serveMCP(ctx, backend, os.Stdin, os.Stdout)
}

// The native MCP process owns its stdin and the lifetime of its public calls.
// A lane forwarder uses the Worker's sole Caller through its private endpoint.
type grokMCPOwner struct {
	action func(context.Context, string, json.RawMessage) (json.RawMessage, error)
	end    func()
}

func (o grokMCPOwner) Action(ctx context.Context, action string, args json.RawMessage) (json.RawMessage, error) {
	return o.action(ctx, action, args)
}
func (o grokMCPOwner) End() {
	if o.end != nil {
		o.end()
	}
}
func serveMCP(ctx context.Context, owner mcp.SessionbusOwner, input io.ReadCloser, output io.Writer) error {
	stop := context.AfterFunc(ctx, func() { owner.End(); _ = input.Close() })
	defer stop()
	return mcp.ServeSessionbus(owner, input, output, mcp.ReportHandler{})
}
