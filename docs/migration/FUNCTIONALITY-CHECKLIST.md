# Stable functionality checklist — Grok separation

Baseline: original peers main `710e5d33369cba4fb9468cd24fea0fe844a0219d`.
The F01–F20 requirement IDs are shared with the migration checklist. Product
semantics and known limitations stay explicit; a failed check does not remove a
requirement. Extraction source preservation and fresh installed behavior are
separate evidence. Current status: local source checks pass; independent review,
permanent installation and fresh installed acceptance are pending.

| ID | Preserved functionality | Existing regression coverage | Installed evidence / limit |
|---|---|---|---|
| F01 | Complete archive install/update, native plugin/MCP entry/generic skill, private `grok-peer-mcp` alias, checksum and archive-role safety | package_test.go; scripts/release/grok_test.go, download_test.go, selection_test.go | Install and reinstall the exact archive in the real home; native plugin inventory, `uninstall --keep-data` and `install --trust`; only owned payload replaced. The baseline has no separate Grok uninstaller and none is added |
| F02 | One public binary, wrapper/native version and token-selected lane dispatch | cmd/grok-peer/main_test.go; version_test.go; common peerversion | Exact installed source/version and private alias |
| F03 | Native argv ownership/order, literal --, repeated groups/name, `--yolo` -> `--always-approve`, native resume/continue | alias_values_test.go; peer_test.go; grok_test.go | Preserve native selector and title semantics. Open: lane typed model/reasoning precede native `agent` (unmerged b41738a, not included) |
| F04 | Default native policy plus exact persistent `MCPTool(sessionbus__sessionbus)` config rule; reject defeating deny before config write or launch | config_permission_test.go; permission_test.go; peer_test.go; grok_test.go | Normal-policy native tool use; unrelated rules and native representation unchanged. Launch-scoped replacement remains peers issue 55 |
| F05 | Ordinary Grok stays ordinary: global plugin starts an inert zero-tool helper, no bus owner or observer | package_test.go; cmd/grok-peer/main_test.go | Zero-input plain Grok has no managed marker, observer or bus row; global skill discovery remains visible |
| F06 | Native identity/title, exclusive initial name claim, /new, rename, resume and fork | name_test.go; peer_test.go; grok_test.go | Exact native/public identity join; preserve native history |
| F07 | Discovery, single/multiple/group sends, public schema/error semantics | peer_test.go; common MCP tests and public SDK | Native list/send with authenticated receiver correlation |
| F08 | Zero-input lane open/readiness, spawn/resume | grok_test.go; review_readiness_test.go | Ready only on actual native confirmation; no model turn before run |
| F09 | Run/start/status/wait/ack, result cursor, exact outcome and interruption | grok_test.go; interrupt_test.go; wake_worker_test.go; lane_endpoint_test.go; common lane tests | Native result retained and collected before acknowledgement |
| F10 | Parent lifecycle, completion notification and direct-child tracing | common trace and lane policy tests; public SDK | Authority and scheduling remain daemon-owned; prior parent/pointer acceptance keeps its original build |
| F11 | Interactive idle inbound autonomously wakes | peer_test.go; native_display_test.go; prompt_ownership_test.go | One inbound -> exact native reply/final with no later harness input |
| F12 | Interactive active delivery via interject; actor acknowledgment is admission, not consumption | continuation_test.go; acp_test.go; peer_test.go | Original active turn evidence -> actual receipt -> reply/final |
| F13 | Managed idle inbound starts one owned prompt | wake_worker_test.go; grok_test.go; prompt_ownership_test.go | One inbound -> actual automatic native turn and managed result |
| F14 | Managed active interject, late-interjection fallback prompt, continuation ownership, no requeue after attempted write | continuation_test.go; continuation_diagnostics_test.go; review_continuation_test.go; review_terminal_failure_test.go; native_display_test.go | Follow Grok's actual admission semantics; never substitute another product's contract |
| F15 | Interactive reconnect, latest identity, supersession terminal, no replay or worker resurrection | peer_test.go; interrupt_test.go | Preserve source-tested reconnect and no-replay behavior; report native limits |
| F16 | Cancellation, bounded cancelled drains, protocol fidelity | acp_test.go; lane_endpoint_test.go; review_seed_boundary_test.go; common bounds tests | Controlled race evidence is distinct from native observations |
| F17 | Normal/failed startup, config failure before start, close, native death and owned cleanup | grok_test.go; peer_test.go; process_linux_test.go; process_darwin_test.go | Owned rows/processes absent; hard-failure descendant containment remains qualified |
| F18 | Native history, persistence and independent auto-close policy | grok_test.go; common lane tests; retained acceptance notes | Close retains native history; no local policy scheduler, journal or durable output added |
| F19 | Independent module/archive/CI/release and platform builds | architecture_test.go; version_test.go; package_test.go; release tests | Exact extracted archive and source; Linux/macOS amd64/arm64 builds; publication held |
| F20 | Every original product/common runtime, asset, fixture and test preserved | PRESERVED-FILES.json and baseline | 89 protected files and 164 original test functions resolve locally or in exact peer-common |

Coverage paths without a prefix are under `wrappers/grok/`. Shared support is
pinned to peer-common `eb655f686e4456a4c1121054763318e3d27e89b0`; its tests run in
that repository. All Grok-specific tests remain here. The test count includes
`TestMain` in `grok_test.go`.

Native clients are expected to update routinely. No exact native version
allowlist is introduced. Record the actual native version/hash for each test;
check concrete capabilities and behavior rather than rejecting a new version.
The build dependency pin on peer-common is separate from native client support.

## Evidence rules and retained limitations

- Never relabel historical acceptance as acceptance of the extracted binary. The
  earlier wake evidence records source `a6b0735`; the 2026-09-10 matrix and the
  config-grant installation keep their own sources and native versions.
- Use the permanent real-home/config/PATH installation for both interactive and
  lane tests. Install/reinstall and test that same integration.
- Each fresh cell gets one send, no replay, and no post-inbound harness prompt,
  PTY input or lifecycle turn. Preserve first failures and actual receipts.
- Presence `running:false` alone is not native-idle evidence. `no_receipt` is
  uncertain and never permission to resend.
- Grok leader mode ignores per-process `--plugin-dir`, so the plugin is globally
  registered. Managed activation requires this launch's exact private leader
  socket and native `GROK_SESSION_ID`.
- The managed permission rule is a persistent user-config change made only by
  managed launches; help, version and native passthrough do not write it.
- Held lane skills under `docs/designs/grok-0.5.0` are retained for knowledge,
  not shipped or activated.
- Hard integration failure can leave native tool descendants. No actual native
  Grok macOS run is claimed; macOS builds do not imply one.

## Completion rule

Account for every protected file/test and dependency; review the concrete diff;
run retained tests/race/vet/lint/packaging checks; install the exact artifact;
verify interactive and lane behavior with retained native evidence; report any
remaining limitation before merge. No runtime redesign, version bump or release
publication is part of the extraction.
