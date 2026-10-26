# gorums-brb

Bracha Byzantine reliable broadcast in Go using Gorums, developed for a UiS
capstone project. The report is maintained separately in Overleaf.

## Build and check

Go 1.27.3 or newer is selected through `go.mod`, as required by Gorums v0.12.0.

```sh
make build
make fix fmt test vet
make lint
go test -race ./...
make bench-linux
```

The executable is `bin/brb`. The Linux build is `bin/brb-linux-amd64`.
See [command usage](cmd/brb/README.md) for the terminal demo and sweep examples.
Run `make generate` after changing the proto file. It builds both generators
from the Gorums and protobuf versions in `go.mod`. Only `protoc` is required
on PATH.

## Protocol and limits

Membership is fixed, with IDs 1 through n and n > 3f. The default fault bound is
`(n-1)/3`, with an explicit `-f` override. A broadcast is identified by its origin
and sequence. PROPOSE, ECHO and READY are one-way multicasts, including self.
Delivery is once per instance and need not follow sequence order.
See the [protocol and system model](docs/Protocol.md) for the threshold reasoning,
properties and implementation limits.

The optional `-optimistic` variant follows OptiBRB's four-phase optimistic path
with rotating committees. It has no recovery or reputation system. It is intended
for fault-free performance comparisons and does not provide BRB liveness or
totality under arbitrary Byzantine faults. See [the variant description](internal/brb/OPTIMISTIC.md).

Votes are counted under the peer ID that Gorums verifies against each replica's
TLS certificate, so a replica cannot vote under another ID. Benchmarks require
`-tls-dir` with that replica's credentials, or an explicit `-plaintext` for a
labelled comparison. `bin/brb -mode=certs` generates a project CA and per-replica
credentials; see [command usage](cmd/brb/README.md#authentication). The security
argument assumes that private keys are not compromised. Failed sends to correct
peers are not repaired.

Each origin has a 1024-sequence receive window and a contiguous delivered prefix.
Old messages are ignored. Messages beyond the window are rejected without storing
them. Local submissions wait when their window is full. Gaps or a replica falling
more than one window behind can stop progress. This is an explicit bound on the
prototype's workload, not an unbounded asynchronous BRB implementation. Values
are limited to 1 MiB. Restart the entire group between runs.

## Layout

- `internal/brb/`: deterministic state machine and Gorums replica.
- `internal/bench/`: measurement, startup coordination and controlled faults.
- `internal/cluster/`: shared membership validation.
- `cmd/brb/`: terminal demo and benchmark modes.
- `proto/brb/v1/`: service definitions and generated bindings.

## License

Original code is licensed under [MIT](LICENSE). Dependencies retain their
respective licenses.
