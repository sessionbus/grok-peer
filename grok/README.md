# Grok Sessionbus support

`grok-peer` runs native Grok interactively or, when launched by Sessionbus,
as a lane. Both modes use one Go binary and the same permanent plugin.

## Install

Install native Grok first, then run the public installer in your normal login:

```sh
curl -fsSL https://raw.githubusercontent.com/sessionbus/grok-peer/main/scripts/install-grok.sh | sh
```

For a source build, run `scripts/package-product grok /absolute/output`, extract
the resulting archive, and run its `./install`. The recipe installs under
`~/.local/libexec/sessionbus/grok` and links `~/.local/bin/grok-peer`.
Keep `~/.local/bin` on your login PATH. Reinstallation replaces the owned plugin
payload, including removal of obsolete skills, and uses Grok's native plugin
inventory/uninstall/install commands. As its last step the installer registers
the Sessionbus helper in your Grok user config with `grok mcp add sessionbus -s
user`. That replaces an existing `sessionbus` entry and re-enables one you had
disabled. If the step fails, the installer exits with Grok's message; while the
config holds no entry, managed interactive launches refuse, and running the
installer again completes it. To go back, remove the entry with `grok mcp
remove sessionbus -s user` before installing an earlier grok-peer; the removal
does not restore earlier settings. Other native configuration stays native.

The private `grok-peer-mcp` alias points to the same binary. The registered
`sessionbus` entry starts that permanent alias; it is not a public launcher
mode. The plugin carries no helper definition; only
`skills/sessionbus/SKILL.md` is active guidance.

## Interactive use

```sh
grok-peer --resume NAME -g team --group review,dev --yolo
grok-peer -n child --group team
```

`-g` and `--group` are repeatable comma-separated group lists, including
`--group=value`. They accumulate in order anywhere before native `--`.
`-n`/`--name` select the initial native session's title; `--peer-name` is a
compatibility alias. `--yolo` translates to native `--always-approve`.
All other native arguments retain their bytes and order. Native `--` ends
wrapper parsing. The launcher starts one process, the TUI as `grok --leader`
with those arguments, so the session runs on Grok's own leader. Explicit
leader-selection flags (`--leader`, `--no-leader`, `--leader-socket`) conflict
with that and are rejected. Headless flags, including `--print` and
`--memory-flush`, are rejected: use Sessionbus lanes for headless model work.
A `--sandbox` value that confines is rejected, because Grok then runs the
session without a leader and it cannot be on the bus; start a sandboxed session
with plain `grok`. The exact values `off` and `none` and an empty value are
passed on, and a `--sandbox` without a value is left to Grok.

Native `--resume`/`--continue`/session selectors remain native. Bare `--resume`
uses Grok's native behavior (retained help says most recent), without a wrapper
picker or title resolver. The launcher creates no native session ID. The first
valid helper for that launch claims the optional initial name for its native
session; later `/new` sessions keep their native names. Renaming happens after
MCP initialization and must be confirmed natively. If Grok replaces that helper
before the name is confirmed, the next helper of the same session applies it,
and requests nothing when the session already shows that title; until then it
can overwrite a title set in between. When the completion mark cannot be
written, a later helper of the same session may request the name again over a
title changed since; that can happen at any time while that launch lives, not
only at startup. Failed naming does not transfer to later sessions. Blank
native names stay blank; native title events update the same bus owner.

Before a managed interactive or lane launch, the wrapper idempotently ensures
one exact global Grok permission rule for
`MCPTool(sessionbus__sessionbus)` in the real `GROK_HOME/config.toml` (or
the resolved user home's `.grok/config.toml`). This persistent config update is
the native surface that applies to leader sessions. It preserves every
other config byte, uses Grok's active compact or structured permission
representation, follows a config-file symlink, and publishes atomically while
serializing concurrent wrapper starts through Grok's `.config-init.lock`. An
invalid or unsupported config shape fails the launch without rewriting the
file. Native deny and ask rules remain present, and Grok evaluates deny before
ask before allow. From that same read, a managed interactive launch refuses
before native starts when the config holds no `mcp_servers.sessionbus` entry,
and names the installer. A managed interactive launch also refuses when the `sessionbus`
entry is disabled in the user's Grok config (`enabled = false`, or its name in
`disabled_mcp_servers`); `grok mcp enable sessionbus`, Grok's command for it
in its 1.0.35 source, enables it again.

A lane passes no `--allow` of its own: Grok's `agent` subcommand does not take
permission rules from the command line, and every session reads the global
rules when it starts (both per the 1.0.35 source), so the config rule above is
the lane's Sessionbus grant. Interactive caller allow, deny,
permission-mode and bypass controls reach the TUI with their bytes and order.
Grok does not apply `--allow` and `--deny` given on the command line in leader
mode (Grok's own behaviour per its 1.0.35 source; observed for one rule on Grok
1.0.52); grok-peer passes them on unchanged; rules belong in Grok's config.
`--yolo`/`--always-approve` and the config grant coexist. Help, version, and
native subcommand passthrough do not touch the config.

Grok leader mode currently ignores per-process `--plugin-dir`. Consequently
the plugin is globally registered and ordinary `grok` discovers the generic
skill. The helper is registered in the user's Grok config, so ordinary `grok`
starts it too; there it is inert: initialize/ping succeed and tools/list is
empty, and it creates no bus owner or native observer. The registration hands
the helper one value from the client that opens the session, the launch
directory. Managed activation requires that this is the directory of a live
`grok-peer` launch, holding the facts file the launcher wrote before it started
Grok (bus socket, groups, requested name, working directory), and native
`GROK_SESSION_ID`. The launch directory is created beside the bus socket; a
managed launch refuses when that path contains `$`, which Grok would expand, or
is not valid UTF-8, which Grok cannot pass on. Both restrict only where the
Sessionbus socket lives, its runtime directory. The launcher refuses nothing
about the session name, the groups or the working directory: the name and the
working directory reach the helper byte for byte, and groups travel as the JSON
list they are today.

Grok's own leader outlasts a launch and serves other Grok clients as well. The
wrapper prevents none of the following:

- One helper exists per session, and the client that opened or loaded the
  session last decides it. A second `grok-peer` launch that loads the same
  session takes it over with its own groups and name, and the row goes when
  that launch quits, even while the first TUI is open.
- A plain `grok --leader` client that loads a managed session replaces its
  helper with an inert one. The session is off the bus until a `grok-peer`
  launch loads it again.
- A session opened by a client that sends no helper definition (another ACP
  client, for example) runs the leader's own copy with the environment of the
  client that started the leader. If that was a `grok-peer` launch, the session
  is managed with that launch's values while that launch lives.
- The helper's observer is `grok agent --leader stdio`, started in the launch's
  working directory with the helper's environment. It reaches the leader Grok
  resolves from there; a leader chosen any other way is not followed. This
  scope rests on the Grok 1.0.35 source, not on observation. If that directory
  is gone, the helper publishes nothing. A session Grok runs without a leader
  (a saved confining sandbox profile, a sandbox from environment or config, a
  failed leader connect) has no row.

Grok's leader can keep a session loaded, with its Sessionbus helper, after the
TUI has quit. The helper therefore checks its launch every two seconds. Once
the launcher has quit and removed its launch directory, the helper ends its
Sessionbus presence and exits, so such a session's row retires a few seconds
after `/quit` rather than at once. If Grok starts the helper again after that,
it stays inactive: it offers no Sessionbus tool and publishes no presence.

## Lanes and delivery

Use the single public `sessionbus__sessionbus` tool with `{action, arguments}`.
`describe` selects capabilities by product; `spawn` uses `product:"grok-peer"`.
No product-specific lane skill is installed. The skill documents
spawn/start/run/status/wait/ack/interrupt/close/forget and completion pointers.

A lane runs `grok` from the Sessionbus daemon service's `PATH` and, when that
`PATH` has none, `bin/grok` in Grok's home (`$GROK_HOME`, default `~/.grok`); a
Grok installed elsewhere with `GROK_BIN_DIR` must be on the service `PATH`.

A fresh lane's typed `model` is passed to the native `agent` subcommand and
named as `modelId` in the lane's session request, the field a Grok leader
writes for its client's model (per Grok 1.0.35 source). Typed
`reasoning_effort` and the
accepted raw `--agent`, `--no-plan` and `--no-subagents` options remain
top-level native options that native agent mode does not apply. A resumed lane's
session request names no model; its session restores the model stored with it (per
Grok 1.0.35 source). The native grammar record, written when a lane still had
a private leader, is `docs/designs/grok-0.5.0/LANE-MODEL-PLACEMENT.md` in the
source repository.

An idle lane delivery returns NotRunning before native submission, so the daemon
starts one owned prompt. Active native actor acknowledgment means `injected`
admission, not consumption; it is emitted at interject enqueue and does not wait
for the current tool batch. A late interjection becomes an automatically started
fallback prompt.
If Grok continues a delivery after the original prompt terminal, the same
shared run remains owned until that native continuation settles. Attempted
writes are never requeued on cancellation, uncertainty or terminal races.

Reads do not consume; `ack` advances the oldest terminal cursor. A `done` record
contains the result; an `unavailable` record carries a reason and also requires
acknowledgment after that reason is recorded. Never acknowledge `running`.
Canceling a wait only cancels that wait. Worker retirement loses retained output.

`persistent` controls owner-exit lifetime and `auto_close_ms` controls post-native-
terminal retirement. Every lane message starts or schedules native work.
There is no wrapper database, durable output, journal or restart recovery.
Native session history remains native-owned. The temporary naming claim in the
launch directory records only the native session ID and whether the name was
confirmed; it contains no result metadata and is never reused by another
launch. The launcher removes the launch directory when it returns.

Installed acceptance and retained historical evidence are recorded separately
in `docs/designs/grok-0.5.0`; intermediate builds are not full acceptance.

## Daemon outages

Interactive presence reconnects automatically after a daemon outage while the
native session remains alive. Calls made during the outage fail; interrupted
calls and deliveries are not replayed. Reconnection republishes the latest
native identity and title. Native session end, supersession and owner shutdown
remain terminal. Daemon-managed Worker lanes do not reconnect after losing
their launch connection.

## Lane process

A lane is one native process, `grok [options] agent [-m model] --no-leader
stdio`, on one connection from the wrapper. The wrapper passes `--no-leader` so
that a lane does not join the user's own leader when Grok's config turns leader
mode on (per Grok 1.0.35 source). Closing the lane ends that process. An
interactive launch starts no leader and no startup hold of its own either; it
forwards signals to the TUI and exits with the TUI's own status.

A lane start may trigger native's own background update: a running lane is not
restarted by it, and a lane started after the update has finished runs the new
version (per Grok 1.0.35 source; the wrapper does not suppress updates).
