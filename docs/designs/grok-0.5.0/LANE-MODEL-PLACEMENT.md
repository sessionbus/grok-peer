# Grok lane typed model placement

Status: source and controlled-test correction after the extraction commit
`120e6b93fc1955bfb9263644d0cce2d1829abbdb`. No native model, installation,
user configuration or Sessionbus daemon was used. Fresh installed evidence is
still required before this is an accepted product behavior.

## Defect

The lane primary client was launched as:

```text
grok --no-auto-update [--permission-mode M] [--reasoning-effort E] [-m MODEL] [EXTRAS]
     --leader-socket L agent --leader stdio
```

Native Grok parses that `-m` into the top-level pager arguments. The `agent`
subcommand does not read them, so a typed lane `open.model` had no effect. The
session used whatever default the private leader resolved.

## Native grammar

Installed native `grok 1.0.34 (3736acbc8658)` has no published source. Public
`xai-org/grok-build` commits `4827113` (1.0.32) and `a28ee2b` (1.0.35)
bracket it. The same regions were also checked at `4247f66` (1.0.38) and at the
latest public commit `07e35a3` (1.0.41, 2026-09-22). All eight grammar regions
below are byte-identical across the four commits. Line numbers are for
`a28ee2b`.

- `crates/codegen/xai-grok-pager/src/app/cli.rs:255-304`: `AgentArgs`, the `agent`
  subcommand, declares its own `-m/--model` (`:264`), `--reasoning-effort`,
  `--always-approve` and `--leader`.
- `cli.rs:416-443`: in `PagerArgs`, only `--leader-socket`, `--debug` and
  `--debug-file` are `global = true`.
- `cli.rs:513`: `PagerArgs` has its own non-global `-m/--model`.
- `cli.rs:600,628,653,681`: `--no-plan`, `--no-subagents`, `--agent` and
  `--disable-web-search` exist only in `PagerArgs`.
- `crates/codegen/xai-grok-pager-bin/src/main.rs:2184-2205`: the
  `Command::Agent` branch passes only `permission_mode_flag`, `trust`,
  `no_auto_update` and `disable_web_search` from the top level into
  `run_agent_command`.
- `main.rs:2427`: the top-level `args.model` is consumed only by non-`agent`
  paths, such as the single-turn headless run.
- `main.rs:1274`: `default_model_override = agent_args.model`.
- `main.rs:1392-1401`: in leader-client mode, `ClientCapabilities.default_model`
  is that override, falling back to the configured default.
- `crates/codegen/xai-grok-shell/src/leader/server.rs:662-716`:
  `inject_session_request_context` inserts `_meta.modelId` from the client's
  `default_model` into a `session/new` request that has none.

The installed binary's help on host `pdev` agrees. Binary
`grok-1.0.34-linux-x86_64`, sha256
`be5905e107d2b8b5f3c142d21ecfe4c8fd32a913d2fd551b788707930c4dc80d`, was run with
an empty temporary HOME.

- `grok agent --help` lists `-m/--model`, `--reasoning-effort`,
  `--always-approve` and `--leader`, plus the globals `--debug`, `--debug-file`
  and `--leader-socket`.
- It does not list `--agent`, `--no-plan`, `--no-subagents`,
  `--disable-web-search` or `--permission-mode`.

This is host-qualified help output, not UMKA installed evidence.

## Change

The typed model now follows `agent`:

```text
grok --no-auto-update [--permission-mode M] [--reasoning-effort E] [EXTRAS]
     --leader-socket L agent [-m MODEL] --leader stdio
```

Everything else keeps its placement and bytes. Accepted raw extras and typed
permission and reasoning options stay top-level. Leader and observer argv are
unchanged, and so is the wrapper's `session/new` payload: it still carries no
`modelId`, leaving injection to the native leader. Regressions:
`wrappers/grok/lane_arguments_test.go` covers the exact argv with and without a
model. It also covers the spawned fake-native argv, where only the primary
client carries the model, and the absence of a wrapper `modelId`.

## Not established or unsupported

- `reasoning_effort` keeps its top-level placement, which agent mode ignores.
  After `agent` it would only set the primary client's local override:
  `ClientCapabilities` (`xai-grok-shell/src/leader/protocol.rs:119`) has no
  reasoning field, and source shows no propagation to the leader session.
  Unchanged and not claimed.
- Raw `--agent`, `--no-plan` and `--no-subagents` are accepted by the wrapper
  but are top-level only. Native agent mode does not receive them, and the
  `agent` subcommand would reject them. They have no demonstrated lane effect.
  Unchanged and not claimed. `--disable-web-search` and `--permission-mode` do
  reach agent mode from the top level.
- The leader injects the model only on `session/new`. Resumed lanes use
  `session/load`, so the typed model is not claimed for resumed lanes.
- Native handling of an unknown or unavailable model id is not tested.
- Acceptance needs one fresh lane on the permanent installation with a
  non-default available `open.model`. Record the native version and hash. Show
  the session's actual native model from native records, not from wrapper argv.
