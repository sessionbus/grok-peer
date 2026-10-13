# Grok product facts

> Historical source note: citations to pre-split Sessionbus paths resolve in
> the Forgejo `ai/sessionbus` repository through its `legacy-*` branches.
> Citations to product source resolve in the external repository and full
> commit recorded by the split archive manifest. Host evidence paths are
> immutable external artifacts, not repository paths.

## Current Go integration (2026-09-10)

Mandatory-wake source update (2026-09-21): the active lane retains native
`x.ai/interject`, whose matching broadcast is emitted by the actor at enqueue
and is independent of tool progress. A late interjection becomes an automatic
fallback prompt. An idle lane returns NotRunning before interject submission so
daemon v0.5.7 seeds a managed Run. Interactive Grok keeps its native interject
carrier in both states. See
[the all-product boundary](../designs/mandatory-message-wake-20260921/NATIVE-BOUNDARIES.md).

The rewrite starts at peers eb727701 and retains the native evidence below as historical knowledge. Current implementation and installation are described in [Grok README](../../grok/README.md). The 0.5.0 [implementation contract](../designs/grok-0.5.0/CONTRACT.md), with activation, naming and delivery amendments alongside it, is the design record; the interactive activation described there is superseded by the 2026-10-12 move to Grok's own leader. The source boundary below observed native Grok 1.0.34 in the real permanent UMKA installation.

Current behavior supersedes historical wrapper choices below: no generated native IDs or native selector resolver; repeated mixed -g/--group before --; no session lock, reconnect loop, broad server-wildcard permission grant or forced autoMode; native titles including empty names update the same owner connection. Managed launches maintain only the exact `MCPTool(sessionbus__sessionbus)` global permission grant. The private MCP alias shares one Go binary with launcher and lane, and only the generic Sessionbus skill is installed. Ordinary Grok still discovers that global plugin because native leader mode rejects per-launch plugin-dir. Since 2026-10-12 a managed interactive session runs on Grok's own leader: the launcher starts only `grok --leader`, the plugin defines no helper, and the installer registers the helper in the user's Grok config, so ordinary Grok starts it there as an inert, zero-tool helper.

The shared Worker owns message-seeded runs and nonconsuming result cursors with
explicit ack. Persistence means owner-exit lifetime, not durable storage. Idle
messages start managed runs. Definite pre-submission active/terminal races use
the daemon's bounded automatic next-run scheduling; attempted deliveries never
replay. Actor admission stays injected across a native terminal. A
product-created continuation remains in the original shared Run until its
matched native completion, with bounded aggregate output. No wrapper database,
result journal or restart recovery exists.

Native Grok derives the queue notification's `runningText` display from the
single submitted text block with Rust `str::trim`; it does not echo the raw
wire text byte-for-byte at the edges. The wrapper therefore correlates that
display with Go `strings.TrimSpace` while preserving the original submitted
payload. Session identity, prompt kind, replay exclusion, prompt ID, terminal
identity and all internal whitespace remain exact. The same rule applies when
a stranded interjection becomes a continuation prompt.

The installed `GWK921A` setup input was 91 bytes with one trailing newline;
its first managed run returned unavailable with `Grok terminal lacks matching
native admission` before any inbound send. That packet does not contain the raw
native stream, so it establishes the wrapper failure and exact input rather
than the missing frame itself. The public 1.0.32/1.0.35 source bracket and a
faithful ACP regression establish the trimmed-display mismatch and correction.
Evidence: `/home/antst/sessionbus-evidence/grok-message-wake-installed-dev1-20260921/cells-gwk921a/grok-lane-idle-gwk921a`,
`SHA256SUMS` SHA-256
`29a47dfad2a2cb9ff8044378e0530f9fec1734539d478d59d9b0542a18f142d0`.

Installed evidence scope and remaining limits are mapped in [acceptance](../designs/grok-0.5.0/ACCEPTANCE.md). The retained earlier isolated-home/daemon experiments are historical evidence, not the current installation or acceptance recipe. Current changes do not require repeating already sealed native discoveries.

## Current managed grant source boundary (2026-09-19)

The installed UMKA version observed for this change was Grok 1.0.34 revision
`3736acbc8658`; its exact source is private. Public 1.0.32 and 1.0.35 source
brackets that build. Their CLI policy declarations and extracted leader
permission-merge block are byte-identical, while their complete `spawn.rs`
files differ. Both pin Rust `glob` 0.3.3. The retained source bridge, hashes
and exact-version parser/glob oracle are recorded in
`/home/antst/sessionbus-evidence/grok-comms-grant-design-refined-dev1-20260919`.

Public Grok 1.0.13 through 1.0.38 declare `--allow` and `--deny` on the pager
command, but the `agent leader` dispatch does not copy those rule vectors into
leader session configuration. The sole source-backed narrow native surface is
the global user config: non-empty `GROK_HOME`, otherwise the resolved user home
plus `.grok`, with no cwd fallback when neither home resolves. Every session
spawn reads its global permission rules. Native parsing selects compact
`deny`/`allow`/`ask` arrays if any one is an array and otherwise uses structured
`permission.rules`; mixing the forms therefore requires updating the compact
form. Both forms enter the same order-independent evaluator, where deny wins
over ask and allow.

Managed peer and lane launches now ensure the exact Sessionbus allow in that
active representation after argument validation and before native start. The
edit preserves unrelated bytes and rule values, refuses invalid or unsupported
TOML without rewriting it, follows a leaf config symlink, preserves mode and
owner, and uses a same-directory fsynced temporary file plus rename. The
native `.config-init.lock` serializes cooperating wrapper starts. A final
identity and byte comparison refuses a detected concurrent in-place native
edit, without claiming that the advisory lock eliminates every native writer
race. Help, version and native subcommand passthrough remain read-only.

Since 2026-10-13 a lane is one native process without a leader and passes no
`--allow` of its own: the Grok 1.0.35 source does not wire command-line rules
into the `agent` subcommand, so the config rule is the lane's grant.
Interactive caller allow/deny, permission mode and bypass controls
retain native ownership and reach the TUI unchanged. Since 2026-10-12 the
wrapper no longer checks a caller deny against the managed tool: the
interactive session runs on Grok's own leader, and rules belong in Grok's
config. The Grok 1.0.35 source lists caller `--allow` and `--deny` as
unsupported for a leader client; on Grok 1.0.52 one deny rule was observed not
to deny. A future launch-scoped replacement is tracked in peers issue 55.

On 2026-10-13 on UMKA, Grok 1.0.52 wrote
`{"jsonrpc":"2.0","id":"skills-reload","result":{"result":{"reloaded":1}}}` to the
ACP output of two idle lanes, 2.79 s after another Grok agent was started, 63
minutes after the previous rewrite of Grok's `bundled` directory. At that
previous rewrite, of 434 files, the wrapper's reader had refused a 73-byte
response in three lanes about two seconds later; its bytes were not kept. Per
Grok 1.0.35 source an agent renews the bundle once an hour, every agent
watches that directory for skills, and on a change it injects a request with a
string ID into its own input and answers it on its output. The wrapper's ACP
reader accepted integer response IDs only and ended the connection; it now
skips a response whose ID is a string. Evidence:
`/home/antst/sessionbus-evidence/grok-full-matrix-run-dev2-20261013/NOLEADER-ROLLOUT-DC2/DIAGNOSTIC-REFUSEDLINE-45B/TIMED-RESULT-FROZEN`.

Installed acceptance on UMKA used signed source
`e26da0b885126589d30ff5ac715b3b214fc33c97` and native Grok 1.0.34. The first
managed start appended exactly the compact permission block to a config with
no prior permission representation, preserved its mode and owner, and left
one active exact allow with no active deny or ask. The bypass lane and both
interactive peers then left the post-update bytes and metadata unchanged,
demonstrating idempotence in the real home. Ordinary and bypass lanes plus
ordinary and `--yolo` peers each completed exact list/send with a settled
receipt and direct receiver observation; no Sessionbus approval interaction
was observed. The exact managed deny still rejected before config or native
changes, and final roster/process cleanup was empty. Evidence:
`/home/antst/sessionbus-evidence/grok-config-grant-installed-dev1-20260920`,
`SHA256SUMS` SHA-256
`1707f8e1cb8496bf192fabde2765c42198bf88b8836c9f0d1a67fa74ee1d1bed`
(90 payloads). The intervening signed source
`8c9768cada7df8477549488e6109b00cbbd2dd9f` only removes an unused four-line
method, so no native rerun is attributed to that source-only cleanup or this
evidence-only documentation update.

## Historical native captures and earlier wrapper behavior

All entries below retain their original versions and evidence. Unqualified source paths and descriptions of old generated IDs, permission grants, locks, lazy observers, durable rows, reconnect and consumed t-1 handles describe the earlier implementation; they are not current requirements. In particular, old picker/restricted-resume wording does not override native selector behavior.

- verified: Grok Build 1.0.13 (`5e9a58528b76`, stable) was installed at `/home/antst/.local/bin/grok` on `umka-dev1`. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P0-version-help.txt`
- verified: Grok Build 1.0.13 — Grok's product interface is ACP over stdio; `grok agent stdio` and `grok agent leader` are native commands, and Grok is an MCP client rather than an MCP server. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P0-version-help.txt`
- verified: Grok Build 1.0.13 — ACP initialization uses protocol version 1 and advertises `cached_token`; unattended authentication is `authenticate {methodId:"cached_token",_meta:{headless:true}}`. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`
- verified: Grok Build 1.0.13 — a fresh ACP session is created with `session/new`; the returned product-minted `sessionId` is the Sessionbus session id. Passing `--session-id` to the ACP process did not determine the id returned by `session/new`. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`
- verified: Grok Build 1.0.13 — the P7 capture loaded an already-live session with `session/load {sessionId:<product-id>}`; it did not prove an offline resume because `session/close` followed the load. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt:547-705`
- verified: Grok Build 1.0.13 — an offline lane launches the ACP primary without `--resume`; `session/load {sessionId:<product-id>}` is the sole resume selector. source: `c5b280d:internal/bridge/grok_native_session.go:191-227`; `/home/antst/agentbus-evidence/grok-w2-rerun-20260906T125544Z/retry-strace.1090844`
- verified: Grok Build 1.0.13 — an offline `session/load` result may omit top-level `sessionId`; `_meta.sessionId` and `_meta.x.ai/sessionDetail.sessionId` carry the requested identity. Every returned identity is checked and the requested id is the fallback, matching the 0.4.0 closed parser. source: `/home/antst/agentbus-evidence/grok-w2-rerun-20260906T125544Z/retry-strace.1090823`; `c5b280d:internal/bridge/grok_native_session.go:227-237`
- verified: Grok Build 1.0.13 — an offline row resumed with its original product id, completed one `Reply exactly W2_OK` turn with native stop reason `end_turn`, and closed with no wrapper, Grok, MCP, or zombie process left by the run. source: `/home/antst/agentbus-evidence/grok-w2-20260906T130703Z`
- verified: Grok Build 1.0.13 — killing an isolated daemon during one active turn made the caller lose its result, the wrapper reaped its Grok and MCP processes, and a restarted daemon loaded the durable row offline; the same product id resumed and then closed with no wrapper, Grok, MCP, or zombie process left. source: `/home/antst/agentbus-evidence/grok-w8-rerun-20260906T133939Z`
- verified: Grok Build 1.0.13 — one active turn exposed `running:true`, rejected a second run as `busy`, accepted `turn.interrupt`, and terminated as `interrupted` with native stop reason `cancelled`. source: `/home/antst/agentbus-evidence/grok-w4-w7-20260906T132250Z`
- verified: Grok Build 1.0.13 — closing during one active turn issued the native cancellation, rejected a concurrent public interrupt as `busy`, wrote the interrupted terminal before close `{}`, and left no wrapper, Grok, MCP, or zombie process. source: `/home/antst/agentbus-evidence/grok-w4-w7-20260906T132250Z`
- verified: the Sessionbus schema, closed-type decoder, malformed/trailing/unmatched response handling, 1 MiB frame bound, separate name/session-id grammar, and invalid-product no-exec rows passed under race on `umka-dev1` without a Grok turn or a new process/zombie. source: `/home/antst/agentbus-evidence/grok-w9-20260906T135014Z`
- verified: Grok Build 1.0.13 — the final renamed lane gate completed `W4_OK` with native stop `end_turn`, returned `not_running` to an interrupt after that terminal, exposed the next run as `running:true`, rejected a second run as `busy`, and crossed interrupt with close using one native cancel. The interrupted terminal with native stop `cancelled` preceded close `{}`, the row became offline, and the reference-worker W4/W6/W7 plus malformed/oversize W9 rows passed under race without another product turn. source: `/home/antst/sessionbus-evidence/grok-final-cells-20260906T212056Z`
- verified: Grok Build 1.0.13 runtime cells use one isolated checkout, binary prefix, daemon, socket, and evidence directory per product run; unrelated installed binaries, daemons, and sessions stay untouched. source: `/home/antst/agentbus-evidence/grok-w2-20260906T130703Z/RUN.txt`; `/home/antst/agentbus-evidence/grok-w2-20260906T130703Z/processes-final.txt`
- verified: Grok Build 1.0.13 — `permission_mode`, `reasoning_effort`, `model`, and supported extra arguments are process flags on the primary ACP command; `cwd` is the child's working directory and the `session/new` or `session/load` `cwd` field. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`; `wrappers/grok/grok_test.go`
- verified: Grok Build 1.0.13 — the pinned CLI accepts permission modes `default`, `auto`, `plan`, `acceptEdits`, `dontAsk`, and `bypassPermissions`, and accepts reasoning efforts `low`, `medium`, `high`, and `xhigh`. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P0-version-help.txt`
- verified: Grok Build 1.0.13 — the session open object carries `cwd`, `mcpServers`, `_meta.yoloMode`, and `_meta.autoMode`; bypass permission maps to `yoloMode:true`, while default maps to `false`. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`; `c5b280d:internal/bridge/grok_native_session.go:211-240`
- verified: Grok Build 1.0.13 — the lane's private MCP entry is described in `session/new` or `session/load`; Grok invokes it as an MCP client during a turn. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P8-product-mcp-raw-v5.txt`; `/home/antst/agentbus-evidence/grok-20260906T072333Z/P8-mcp-raw-v5.txt`
- verified: Grok Build 1.0.13 — the wrapper starts a private leader with `agent leader`, `--leader-socket`, `--relay-on-demand`, and `--no-auto-update`; default permission also grants `MCPTool(sessionbus__*)`. source: `c5b280d:internal/bridge/grok_native_session.go:31-72`
- verified: Grok Build 1.0.13 — the launcher opens one authenticated client before the TUI and keeps it forever quiet as the startup hold; it receives no `_x.ai/sessions/changed`, and a roster request on that connection never receives a response even after the session exists. The product-spawned resident helper opens a separate observer only on its first delivery. source: `c5b280d:internal/bridge/grok_native_session.go:83-135`; `c5b280d:internal/bridge/grok_native_observer.go:22-45`; `/home/antst/agentbus-evidence/grok-new-20260906T164113Z`; `/home/antst/agentbus-evidence/grok-artifact-20260906T172356Z`; `/home/antst/agentbus-evidence/grok-helper-lifetime-20260906T182054Z`
- verified: Grok Build 1.0.13 — the session base is `GROK_HOME` when set and `~/.grok` otherwise. A TUI session was stored at `<base>/sessions/<url.PathEscape(cwd)>/<session-id>`; the captured cwd with a space and dot encoded `/` as `%2F` and space as `%20` while preserving the dot. The entry directory appeared before the atomic `summary.json` commit. The accepted peer topology does not inspect this product storage. source: `/home/antst/agentbus-evidence/grok-artifact-20260906T172356Z`; `/home/antst/agentbus-evidence/grok-resume-artifact-20260906T173128Z`; `/home/antst/agentbus-evidence/grok-new-reviewed-20260906T180145Z`
- UNVERIFIED: Grok Build 1.0.13 documents a slug-plus-hash session directory and `.cwd` file when an escaped cwd exceeds 255 bytes, but its exact algorithm is not captured. The wrapper neither recreates nor reads this layout. source: `/home/antst/agentbus-evidence/grok-artifact-20260906T172356Z/trace.1178297`
- verified: Grok Build 1.0.13 — managed resume atomically rewrites the pre-existing session's `summary.json` after TUI exec, along with `chat_history.jsonl` and related files; a later observer listed the requested id as resident without a model turn. This is a product artifact fact, not a Sessionbus readiness mechanism. source: `/home/antst/agentbus-evidence/grok-resume-artifact-20260906T173128Z`
- verified: Grok Build 1.0.13 — native title changes use `_x.ai/session/rename {sessionId,title}` and are confirmed by a later exact roster read. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`; `c5b280d:internal/bridge/grok_native_observer.go:226-245`
- verified: Grok Build 1.0.13 — `_x.ai/sessions/list` is the authority for an interactive peer's current title, cwd, activity, resident state, and yolo state. Exactly one matching live row is required. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`; `c5b280d:internal/bridge/grok_roster.go:9-25`; `c5b280d:internal/bridge/grok_roster.go:58-116`
- verified: Grok Build 1.0.13 — the retired 0.4.0 status projection mapped `working` to busy, `needs_input` to waiting, and `idle` to idle; Sessionbus has no corresponding status surface. The wrapper validates all three native states. Peer delivery uses native interject and reports `injected` after the actor acknowledges it in every state; only a lane's active-token path chooses native interject versus the shared idle FIFO. Missing `resident` or `yolo`, duplicate exact rows, and unknown live activity are authority errors. source: `c5b280d:internal/bridge/grok_roster.go:58-116`; `wrappers/grok/peer.go`; `wrappers/grok/lane.go`
- verified: Grok Build 1.0.13 — a run is `session/prompt` with the exact session id and a text prompt; `agent_message_chunk` notifications are accumulated, while `agent_thought_chunk` is not part of the returned text. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`; `c5b280d:internal/bridge/grok_native_session.go:270-309`; `c5b280d:internal/bridge/grok_native_session.go:331-360`
- verified: Grok Build 1.0.13 — `session/prompt` returns `stopReason:end_turn` for completion and `stopReason:cancelled` after cancellation; the native stop reason is preserved. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`
- verified: Grok Build 1.0.13 — interrupt is the ACP notification `session/cancel {sessionId}`; there is no response, and the run terminal supplies the result. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`; `c5b280d:internal/bridge/grok_native_session.go:311-322`
- verified: Grok Build 1.0.13 — delivery uses `_x.ai/interject {sessionId,text,interjectionId}` and is acknowledged by the actor notification `_x.ai/session/interjection` carrying the same identifiers. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`
- verified: Grok Build 1.0.13 — an interjection while the actor is working is injected into that run. An idle interjection instead starts Grok's own `interject-fallback` prompt and queues a later `session/prompt` behind it, so lane-idle delivery uses the shared wrapper FIFO and native interject is active-only. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt:286-302`; `/home/antst/agentbus-evidence/grok-runtime-20260906T120424Z/cells/W5-idle-delivery.txt`
- verified: Grok Build 1.0.13 — streamed chunks and the terminal response carry a product prompt ID; the wrapper selects only chunks matching the ID returned for its own `session/prompt`. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt:303-493`
- verified: Grok Build 1.0.13 — only exact absence of the asserted actor maps to `no_leader`; malformed, duplicate, nonresident, or otherwise invalid roster state remains its own diagnostic. source: `c5b280d:internal/bridge/grok_roster.go:9-25`; `c5b280d:internal/bridge/grok_roster.go:58-116`
- verified: Grok Build 1.0.13 — native orderly close is `session/close {sessionId}` on the primary. Sessionbus then stops the leader and releases the wrapper-owned endpoint and session lock; the daemon's close bound is the only close clock. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt`; `c5b280d:internal/bridge/grok_native_session.go:324-329`
- verified: Grok Build 1.0.13 — the early direct MCP probe could observe and interject through the user's default Grok leader. That probe helper was written to exit after one action, so its lifetime was a harness artefact and did not prove that Grok kills configured helpers. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P8-product-mcp-raw-v5.txt`; `/home/antst/agentbus-evidence/grok-20260906T072333Z/P8-no-leader-raw-v5.txt`; `/home/antst/agentbus-evidence/grok-helper-lifetime-20260906T182054Z`
- verified: Grok Build 1.0.13 — an MCP server named `sessionbus` exposing the `sessionbus` tool is presented to the model as `sessionbus__sessionbus`; the model invoked that exact namespaced name successfully. source: `/home/antst/agentbus-evidence/grok-peer-20260906T135533Z/C1-rerun-export.md`; `/home/antst/agentbus-evidence/grok-peer-20260906T135533Z/work/.grok/config.toml`
- verified: Grok Build 1.0.13 — the private leader spawned one configured stdio MCP helper at session initialization, kept the same PID across more than 71 seconds idle and one tool call, and kept it alive afterwards. `/new` spawned a second helper for the new session while the first remained alive; launcher shutdown ended both. source: `/home/antst/agentbus-evidence/grok-helper-lifetime-20260906T182054Z`
- verified: Grok Build 1.0.13 — each resident helper receives immutable `GROK_SESSION_ID` and `GROK_LEADER_SOCKET` values from the product. Two helpers under one leader had identical environment-name sets and differed only in `GROK_SESSION_ID`; `SESSIONBUS_SESSION_ID` was absent, MCP initialize and tools/list bodies carried no identity, and the second helper's roster contained both exact live sessions. source: `/home/antst/agentbus-evidence/grok-helper-identity-20260906T183220Z`
- verified: Grok Build 1.0.13 — the final zero-turn `/new` runtime gate exposed the initial exact id `33333333-4444-4555-8666-777777777777@local`, then added `01a07891-6b9c-7411-b538-4f2170fa0f81@local` as a second simultaneous peer while the first stayed connected; both inherited group `grok-new` and had distinct product-spawned resident helpers. `/quit` removed both peers and every launcher, leader, hold, TUI, helper, and daemon process owned by the isolated run, with an empty orphan inventory. source: `/home/antst/sessionbus-evidence/grok-new-traced-20260906T210804Z`
- verified: Grok Build 1.0.13 — the final helper-owned peer gate served `sessionbus__sessionbus` action `list` in-process, then `/rename Grok Gate Title` was published by the helper's same-ID re-hello. A delivery whose roster read was `idle` and an immediate second delivery whose roster read was `working` each received its exact native interjection acknowledgement and public `injected` receipt; the idle delivery started the one product fallback turn. `/quit` removed the peer and every launcher, leader, hold, TUI, helper, and observer process owned by the run. source: `/home/antst/sessionbus-evidence/grok-final-cells-20260906T212056Z`
- verified: Grok Build 1.0.13 — in interactive Sessionbus mode the launcher owns the private leader, TUI, and quiet startup hold only. Each product-spawned `grok-peer mcp` helper owns one immutable session peer, one caller, and in-process MCP tools; it lazily opens and keeps its own observer on first delivery. `/new` creates another helper and peer rather than replacing the first. source: `/home/antst/agentbus-evidence/grok-helper-lifetime-20260906T182054Z`; `/home/antst/agentbus-evidence/grok-helper-identity-20260906T183220Z`; `cmd/grok-peer/main.go`; `wrappers/grok/peer.go`
- verified: Grok Build 1.0.13 — the managed interactive surface ensures an exact identity: a fresh invocation without a selector mints and prepends a UUIDv4 `--session-id`, while resume accepts a valued `--resume` or `--load`. It preserves native informational commands and rejects bare resume, `--continue`, `--fork-session`, caller-owned leader selection, and headless-only flags; headless work uses a Sessionbus Grok lane. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P0-version-help.txt`; `wrappers/grok/peer.go`; `wrappers/grok/peer_test.go`; `c5b280d:internal/launcher/grok_peer.go:1-240`
- verified: Grok Build 1.0.13 — the 0.4.0 launcher searched configured and product-specific fallback locations for Grok. That behavior is retired: the split-ready wrapper follows the product-independent Sessionbus rule and resolves the real `grok` executable from `PATH`. source: `c5b280d:internal/launcher/grok_peer.go:478-547`
- verified: Grok Build 1.0.13 — inherited session locks, the token-digest provisional lane socket, MCP message rendering, and peer reconnect/identity cancellation are shared host or SDK mechanisms rather than Grok-specific lifecycle state. In peer mode the launcher owns the quiet-hold process group, while each resident helper owns and joins its lazily opened observer process group. source: `wrappers/grok/peer.go`; `c5b280d:internal/bridge/grok_process_unix.go:1-49`; `c5b280d:internal/sessiontools/envelope.go:1-78`
- verified: Grok Build 1.0.13 — the pre-correction resident cell used the product-presented `sessionbus__sessionbus` tool, accepted an active default-leader interjection, and stayed resident after the native turn; the exported transcript proves tool naming and the delivery path. Later probes corrected both ownership assumptions: the launcher owns its private leader and quiet hold, while product-spawned resident helpers own per-session presence and observers. source: `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C1-export.md`; `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C5-active-send.json`; `/home/antst/agentbus-evidence/grok-helper-lifetime-20260906T182054Z`; `/home/antst/agentbus-evidence/grok-helper-identity-20260906T183220Z`; `wrappers/grok/peer.go`
- verified: Grok Build 1.0.13 — the resident launcher kept one caller registry across separate short action connections: start returned `t-1`, status and a bounded wait remained `running`, interrupt returned `{}`, and the collected terminal was `interrupted`. source: `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C3-start.json`; `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C3-status.json`; `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C3-wait-timeout.json`; `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C4-wait-terminal.json`
- verified: Grok Build 1.0.13 — `/rename Grok Renamed Title` changed the native title, and Sessionbus preserved the spaced title exactly after a same-ID re-hello. The resident helper now performs that exact roster/re-hello publication when it first receives a delivery, not during an unrelated tool action. source: `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C8-rename-pane.txt`; `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C8-title-rehello.json`; `wrappers/grok/peer.go`
- verified: Grok Build 1.0.13 — killing only the isolated Sessionbus daemon while an example lane turn was outstanding settled the resident local turn as `unavailable` with `result unavailable, lane resumable`; a call while disconnected returned `not_connected`, and the same Grok identity reconnected after the daemon restarted. source: `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C4-C8-status-after-eof.json`; `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C8-call-disconnected.json`; `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C8-list-after-reconnect.json`
- verified: Grok Build 1.0.13 — a `--single` headless invocation completed its prompt but did not remain a live roster row for peer attachment; the installed peer cell therefore used the interactive TUI, which stayed resident between turns. source: `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C-product.stdout`; `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C-list-now.json`
- verified: Grok Build 1.0.13 — that discarded `--single` attempt auto-started a detached default leader which outlived the command; the stale leader then held later interactive startup at `Starting session`. Sessionbus interactive mode therefore rejects headless flags and starts, routes to, joins, and removes one private leader of its own. source: `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C-headless-leader-before-cleanup.txt`; `/home/antst/agentbus-evidence/grok-peer-resident-20260906T153610Z/C-headless-leader-after-cleanup.txt`; `/home/antst/agentbus-evidence/grok-clear-20260906T162206Z/pane-not-ready.txt`; `wrappers/grok/peer.go`
- UNVERIFIED: Grok Build versions after 1.0.13 may add or change ACP methods, roster fields, activity values, permission values, or interactive flags; a version update requires fresh captures before changing these closed surfaces. source: `/home/antst/agentbus-evidence/grok-20260906T072333Z/P0-version-help.txt`

## Exact captured frames typed by the wrapper

Each line below is copied byte-for-byte, excluding its terminating newline, from a `STDIN` frame in `/home/antst/agentbus-evidence/grok-20260906T072333Z/P1-P7-acp-raw-v3.txt` (Grok Build 1.0.13).

- `initialize`

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{"fs":{"readTextFile":false,"writeTextFile":false},"terminal":false}}}
```

- `authenticate`

```json
{"jsonrpc":"2.0","id":2,"method":"authenticate","params":{"methodId":"cached_token","_meta":{"headless":true}}}
```

- `session/new`

```json
{"jsonrpc":"2.0","id":30,"method":"session/new","params":{"cwd":"/home/antst/agentbus-evidence/grok-20260906T072333Z/work","mcpServers":[],"_meta":{"yoloMode":false,"autoMode":false}}}
```

- `session/load`

```json
{"jsonrpc":"2.0","id":303,"method":"session/load","params":{"sessionId":"01a075a0-43ac-7d70-919f-f5e0f6f5e4e2","cwd":"/home/antst/agentbus-evidence/grok-20260906T072333Z/work","mcpServers":[],"_meta":{"yoloMode":false,"autoMode":false}}}
```

- `session/prompt`

```json
{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{"sessionId":"01a075a0-43ac-7d70-919f-f5e0f6f5e4e2","prompt":[{"type":"text","text":"Reply with exactly GROK_PROBE_OK after waiting five seconds."}]}}
```

- `session/cancel`

```json
{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"01a075a0-43ac-7d70-919f-f5e0f6f5e4e2"}}
```

- `_x.ai/interject`

```json
{"jsonrpc":"2.0","id":106,"method":"_x.ai/interject","params":{"sessionId":"01a075a0-43ac-7d70-919f-f5e0f6f5e4e2","text":"Also include ACTIVE_INTERJECT_OK in the same answer.","interjectionId":"active-message-1"}}
```

- `_x.ai/sessions/list`

```json
{"jsonrpc":"2.0","id":103,"method":"_x.ai/sessions/list","params":{}}
```

- `session/close`

```json
{"jsonrpc":"2.0","id":305,"method":"session/close","params":{"sessionId":"01a075a0-43ac-7d70-919f-f5e0f6f5e4e2"}}
```
