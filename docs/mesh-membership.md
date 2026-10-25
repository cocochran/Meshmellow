# Core mesh creation and joining

`llm_cluster::mesh::Node` owns device identity, mesh creation, invitations, signed
membership, trust decisions and live core control connections. The mobile library
in `apps/enrollment` is a C/JNI adapter to this implementation, not a second mesh.

A trusted member is not automatically a compute worker. Profiling, model artifacts,
resource reservations and inference readiness remain separate admission steps.

## Run a mesh node

```sh
cargo run --bin llm-cluster -- mesh --state-dir /path/to/private-device-state
```

Keep this process running and send one JSON object per line:

```json
{"op":"create","name":"My mesh","device_name":"Mac"}
{"op":"invite"}
{"op":"snapshot"}
```

The invitation includes `id`, `expires`, and `ticket`. Share the full
`layercake://join/...` ticket with another node. It carries the issuer's endpoint ID, invitation UUID, expiry, relay address
and signature. No lookup service is needed to paste this invitation and connect.

On the joining node:

```json
{"op":"join","invitation":"layercake://join/PASTE_THE_FULL_INVITATION","device_name":"Phone"}
```

The pending request appears on trusted members. Use the returned `issuer` and
request `id` to approve or decline:

```json
{"op":"decide","issuer":"ISSUER_ENDPOINT_ID","request":"REQUEST_UUID","approve":true}
```

Any trusted member can decide; the invitation issuer serializes decisions and
persists the signed grant before reporting success. The applicant polls for that
grant. `refresh`, `cancel_join`, and `snapshot` use the same command interface in
the CLI and mobile apps. EOF closes the CLI endpoint. Use one private state
directory per running node; do not copy private identities to another device.

## Live core membership

One pairwise control connection is initiated by the lexically lower endpoint ID.
Both sides check the authenticated endpoint against the signed membership ledger
before completing the existing core hello. The mesh's genesis hash is the hello's
cluster ID; knowing that ID alone does not authorize a control connection.

The same endpoint serves enrollment and the core control ALPN. Core heartbeat
helpers send every two seconds and close a silent connection after seven seconds.
The mesh retries disconnected links. Snapshots expose `connected_peers` and each
member's `online` flag from the core peer table; saved trust and live connectivity
are distinct. Closing or restarting a node withdraws its connection while retaining
its identity and signed membership. This does not implement leader election.

## No Layercake lookup server

Invitations carry all bootstrap information and are shared directly between users.
There is no Layercake invitation registry, directory service, or server to host.
Each invitation expires after ten minutes, accepts one applicant, and still requires
explicit approval from a trusted member. Keep the inviting device online until
approval completes; replacing an unused invitation invalidates the previous one.

Iroh establishes authenticated peer connections, using direct paths when possible
and its public relay infrastructure when needed. Existing members reconnect by
endpoint identity using Iroh discovery. This removes the Layercake lookup service,
not the underlying Iroh networking infrastructure. Private keys and membership
remain on the devices.

## Validation

```sh
cargo test --all-targets
cargo test --lib mesh:: -- --include-ignored
```

The second command also runs Internet-dependent tests with all IP transports
disabled. They cover full invitations without a lookup service, denial of
unapproved core connections, remote approval, bidirectional core heartbeats,
connection cleanup, and rejoining with persisted identity after restart.

For physical Android validation, build and install the app and androidTest APK,
then run `python3 tests/mobile/android_mesh.py ADB_SERIAL`. This requires a physical
phone, uses an isolated temporary identity, and admits only the expected companion
core endpoint. Existing app membership is not modified. It tests Android JNI/Iroh,
creation, a signed invitation, approval, a live core connection and saved membership.
