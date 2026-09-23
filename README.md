# Sessionbus Grok peer

Connect native Grok sessions through [Sessionbus](https://github.com/sessionbus/sessionbus).
`grok-peer` provides interactive peers and managed lanes from one Go binary, a
permanent native plugin, native history and the native permission controls.

## Install

Install native Grok and Sessionbus first using your normal home, login and PATH.

```sh
curl -fsSL https://raw.githubusercontent.com/sessionbus/grok-peer/main/scripts/install-grok.sh | sh
```

This repository is being separated from the original peers tree. No independent
release is published yet; use a reviewed archive built from source until release.
The installer retains checksum verification, archive-role checks and latest
stable/development selection against this repository; until a release exists it
stops without installing and never fetches another product. Older published
installer links remain compatibility entrypoints in
[the original repository](https://github.com/sessionbus/codex-peer), pinned to the
final combined v0.5.3 assets.

See [the Grok guide](grok/README.md) for install/update instructions, the private
`grok-peer-mcp` alias, native flags, the managed permission rule, resume, identity
and lane lifecycle behavior. The archive contains the Go executable, its private
alias and the native plugin/skill; no Node.js/npm runtime is required.

## Updating older installations

`list` reports the bound originating caller in `self_info` alongside the visible
`sessions`. Compare its `session_id` with row IDs to recognize self; a filter or
remote host query does not change the caller identity. Older daemons may omit
this field, which must not be guessed from names or row order.

When updating an existing installation for `self_info`, update every product
peer first and restart managed sessions so their helpers load the updated SDK.
Then update the host daemon. Older SDKs reject the new response field; updated
SDKs also accept older daemon responses. Coordinate federated host upgrades as
well, since daemons validate forwarded responses with their embedded SDK.

## Build and test

```sh
git clone https://github.com/sessionbus/grok-peer.git
cd grok-peer
GOWORK=off go mod download
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
scripts/package-product grok ./dist
```

Builds require Go 1.24 or newer; target installations do not need Go. Packaging
supports Linux/macOS amd64/arm64. This extraction does not bump RELEASE_VERSION
or the Grok native plugin manifest. Publication remains held during validation.

Shared support uses the exact peer-common version/checksum in go.mod/go.sum.
Native Grok versions are not pinned: users routinely update native clients.
Observed native versions/hashes identify test evidence, not a runtime allowlist.

The [stable functionality checklist](docs/migration/FUNCTIONALITY-CHECKLIST.md)
and [preservation inventory](docs/migration/PRESERVED-FILES.json) track separation.
Historical behavior and limitations remain in [Grok facts](docs/products/grok.md)
and the [Grok design and acceptance records](docs/designs/grok-0.5.0/ACCEPTANCE.md).
Held lane skills stay documentation only and are not packaged or activated.
Fresh extracted-build validation remains pending.

## Version reporting

`grok-peer --version` and `grok-peer -v` report the peer release and exact source
revision without starting native Grok. Use exact `--native-version` to request
native Grok's own `--version` outside lane mode; lane workers reject that escape.
Development builds print `development` with their VCS revision. A stable build
fails before packaging unless its `vX.Y.Z` tag and [`RELEASE_VERSION`](RELEASE_VERSION)
agree.

## Interactive CLI aliases

`--yolo` passes as native `--always-approve`. `--resume VALUE` passes as native
`--resume VALUE` (ID or title); bare `--resume` keeps Grok's native most-recent
session behavior. Native `--continue` remains separate. Tokens after literal `--`,
attached native values and the first required value of recognized native options
are preserved. These aliases apply to interactive launches; they do not change the
typed lane `permission_mode` API.

## Delivery and presence

A delivery reported as `rejected` with reason `no_receipt` means that no usable
receipt was obtained. It does not prove the message was never submitted or
consumed, including when the recipient disconnects. Preserve the delivery ID,
reason and any run reference; report the uncertainty without automatically
resending. A later connected or idle-looking row does not make replay safe.

In a `list` row, `connected` describes the Sessionbus attachment and `running`
describes a daemon-managed Run. An interactive peer's `running:false` does not
prove its native model is idle. Do not use these flags to predict delivery
admission; follow the actual receipt.

The orchestrator's installed tool declaration governs the tool identifier and
`{action, arguments}` envelope. After selecting a lane product, that product's
`describe` response and product skill/README govern its open fields, native
permission values, delivery receipts and lifecycle. `describe` lists available
fields; product documentation supplies their meaning and allowed native values.
Do not apply the orchestrator product's native options to a different lane product.

### Parent tracing

The unified tool's `trace` action can enable `events` or `content` tracing for
one live direct child, and `spawn` accepts the same initial `trace` setting. The
default is `off`. The daemon sends at most one ordinary message copy to the
eligible parent after the original Sessionbus send settles; `content` includes
the body, while `events` contains message and delivery metadata. Run lifecycle,
native prompts, and native results are outside this initial scope.

Tracing adds no history, durable policy, replay, catch-up, or separate event
transport. It requires Sessionbus v0.5.4 or later on the daemon and every involved hub,
and updated peer tools. Upgrade all of them before requesting tracing across
hosts. See the [v0.5.1 notes](docs/releases/v0.5.1.md) and the
[Sessionbus communication-trace contract](https://github.com/sessionbus/sessionbus/blob/main/docs/designs/COMMUNICATION-TRACE.md)
for the complete authority and delivery rules.

The tracing relationship ends with the parent's live lifetime, even for a
persistent child. Reconnecting with an old parent ID does not recover that
relationship or replay copies. Older daemons reject the new `trace` action or
spawn field; a trace-aware daemon returns `unsupported_trace` when an involved
federation link cannot enforce the requested tracing controls.
