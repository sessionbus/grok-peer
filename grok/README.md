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
inventory/uninstall/install commands. Other native configuration stays native.

The private `grok-peer-mcp` alias points to the same binary. The registered
plugin's `scripts/native-entry` resolves that permanent alias; it is not a
public launcher mode. Only `skills/sessionbus/SKILL.md` is active guidance.

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
wrapper parsing. Explicit leader-selection flags conflict with the private
managed leader and are rejected. Use Sessionbus lanes for headless model work.

Native `--resume`/`--continue`/session selectors remain native. Bare `--resume`
uses Grok's native behavior (retained help says most recent), without a wrapper
picker or title resolver. The launcher creates no native session ID. The first
valid helper for that launch claims the optional initial name; later `/new`
sessions keep their native names. Renaming happens after MCP initialization and
must be confirmed natively. Failed naming does not transfer to later sessions.
Blank native names stay blank; native title events update the same bus owner.

Before a managed interactive or lane launch, the wrapper idempotently ensures
one exact global Grok permission rule for
`MCPTool(sessionbus__sessionbus)` in the real `GROK_HOME/config.toml` (or
the resolved user home's `.grok/config.toml`). This persistent config update is
the native surface that applies to private-leader sessions. It preserves every
other config byte, uses Grok's active compact or structured permission
representation, follows a config-file symlink, and publishes atomically while
serializing concurrent wrapper starts through Grok's `.config-init.lock`. An
invalid or unsupported config shape fails the launch without rewriting the
file. Native deny and ask rules remain present, and Grok evaluates deny before
ask before allow.

The wrapper retains the exact `--allow` projection on its private leader for
compatibility, although public Grok 1.0.13 through 1.0.38 do not forward those
top-level rules into leader sessions. Interactive caller allow, deny,
permission-mode and bypass controls retain their bytes and order. A caller
deny which the bridged native parser applies to the managed Sessionbus tool is
rejected before the config write or any native launch; unrelated rules remain
native-owned. `--yolo`/`--always-approve` and the config grant coexist. Help,
version, and native subcommand passthrough do not touch the config.

Grok leader mode currently ignores per-process `--plugin-dir`. Consequently
the plugin is globally registered: ordinary `grok` discovers the generic skill
and starts an inert helper, whose initialize/ping succeed and tools/list is
empty. It creates no bus owner or native observer. Managed activation requires
both this launch's exact private leader socket and native `GROK_SESSION_ID`.
An inherited marker with a different leader stays inert.

## Lanes and delivery

Use the single public `sessionbus__sessionbus` tool with `{action, arguments}`.
`describe` selects capabilities by product; `spawn` uses `product:"grok-peer"`.
No product-specific lane skill is installed. The skill documents
spawn/start/run/status/wait/ack/interrupt/close/forget and completion pointers.

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
Native session history remains native-owned. The empty temporary naming claim
contains no session/result metadata and is never reused by another launch.

Installed acceptance and retained historical evidence are recorded separately
in `docs/designs/grok-0.5.0`; intermediate builds are not full acceptance.

## Daemon outages

Interactive presence reconnects automatically after a daemon outage while the
native session remains alive. Calls made during the outage fail; interrupted
calls and deliveries are not replayed. Reconnection republishes the latest
native identity and title. Native session end, supersession and owner shutdown
remain terminal. Daemon-managed Worker lanes do not reconnect after losing
their launch connection.
