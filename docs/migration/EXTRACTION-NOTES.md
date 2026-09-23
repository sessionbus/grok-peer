# Grok extraction record

Baseline: `710e5d33369cba4fb9468cd24fea0fe844a0219d` from the complete organisation
copy. The original repository history and sibling copies retain removed product
paths. Grok implementation, native plugin manifest/MCP entry, generic skill,
private `grok-peer-mcp` alias, managed permission rule, native argument and
permission handling, install layout and archive recipe remain.

The frozen inventory accounts for 89 protected files and 164 original Go test
functions: 52 Grok-owned files, the shared archive installer, 32 shared support
files now resolved in peer-common, and 4 held Grok lane reference files.
Sixty-eight files remain byte-identical. Eighteen Go files change only module
imports and gofmt import ordering. `scripts/install-grok.sh` and
`grok/README.md` change only the canonical repository URL. The shared archive
installer drops only its non-Grok ROLE arms. Product runtime contains no
behavior change.

Shared host/MCP/version/socket support and the legacy cleanup utility resolve to
`github.com/sessionbus/peer-common` at the reviewed immutable version
`v0.0.0-20260922143100-eb655f686e44` (commit
`eb655f686e4456a4c1121054763318e3d27e89b0`), with no replace or alternate
workspace. The linker stamps the Grok product release/revision into the common
version helper. The legacy cleanup documentation links to that pinned source.

Root boundary, packaging, version and download tests and workflows are scoped to
Grok. The Grok manifest command, native entry, private alias and generic skill
reachability checks remain. Other-product checks are removed with their
product paths. The version guard keeps RELEASE_VERSION format and stable-tag
agreement; it drops only the removed Claude and Codex manifest checks. The Grok
native plugin manifest keeps its independent `0.5.0` identity. The
`scripts/package-product grok DIR` interface and archive members are unchanged.
The archive installer, bootstrap checksum/role checks and Grok native plugin
inventory/uninstall/install sequence are unchanged. Third-party notices now
include the common module and BurntSushi/toml. The baseline Grok binary already
linked toml without a notice. The package has no Node/npm runtime or build
dependency.

All Grok design notes, acceptance records, held skills, product facts and
cross-product mandatory-wake and tool-grant designs remain. The held Grok lane
reference formerly under Claude's design notes is copied byte-for-byte to
`docs/designs/grok-0.5.0/legacy-lane-skill`. Three deletion-induced Pi/OMP
release-note links point to immutable original source in the family repository.
The v0.5.0 README install anchor is updated.

Local protected-file normalization passed for all 89 files and 164 tests. A
baseline/extracted archive comparison with the same revision found identical
members, symlink and plugin payload. Only the README URLs, notices, trimmed
installer and rebuilt binary differ. Both archive installers produced identical
trees and identical native plugin commands in a disposable home. Fresh
installation/acceptance and independent extraction review remain pending. A
historical pass does not validate this new artifact. Release publication remains
held and no version bump is made. Native client version updates are expected;
exact native versions in evidence are provenance, not a compatibility allowlist.

The historical wake acceptance source `a6b0735e202878bc80e6bb8eee0e1f2ba64708c7`
matches the baseline for every Grok, shared and packaging runtime path. Its
native display fix is merged as `8ab4cc8`. That evidence belongs to its own
binary and is not rebound to this extraction.

Unmerged delta not included: the diagnostic branch
`diagnosis/grok-reverse-request-capture-20260920` (tip
`b41738a51a614ad6854f552976aa859db9e75869`). It holds a never-executed
permission-capture diagnostic, superseded by the merged config grant, plus a
narrow lane fix that places the typed `-m` model after the native `agent`
subcommand, with two regression tests. The baseline still places typed model
and reasoning options before `agent`. Recorded native analysis attributes
reasoning and raw `--agent`/`--no-plan`/`--no-subagents` placement to separate
unresolved issues. Cherry-picking would change runtime behavior, so it needs a
separately reviewed product change and fresh installed evidence. The pre-split
`feature/release-install` branch is obsolete and remains archived on the
original Codex repository.

Local extraction checks: go test, race, vet, module verify/tidy, golangci-lint,
actionlint, diff check and protected source/test normalization pass. The pinned
common module's own tests also pass from the verified module cache. There is no
new model call, installation or UMKA mutation in this source preparation.
