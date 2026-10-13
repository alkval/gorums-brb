# gorums-brb

Capstone project exploring a Byzantine reliable broadcast prototype in Go using
[Gorums](https://github.com/relab/gorums).

The prototype implements a Bracha-style protocol. The final system model,
properties, thresholds, and evaluation requirements remain subject to supervisor
confirmation.

## Status

- Local project scaffold created.
- A provisional Bracha-style state machine implements the `SEND`, `ECHO`, and
  `READY` transitions and the textbook thresholds for `n > 3f`.
- Generated Gorums multicast calls send the three phases without application
  acknowledgements or custom retries. The first prototype assumes established,
  working connections throughout a run and trusts logical sender IDs.
- State-machine tests cover thresholds, duplicate votes, reordering, equivocation,
  and silent faulty processes. Network tests cover delivery on four connected
  Gorums nodes and concurrent broadcasts inside one Go process. These are not
  tests of recovery from broken connections.
- The current Gorums revision is pinned as a Go dependency.
- A separate [diagnostic benchmark](cmd/brbbench/README.md) writes benchkit
  reports for upstream sweep, measuring each origin's local delivery latency.
- A minimal [node executable](cmd/brbnode/README.md) starts one Gorums server,
  prints deliveries, and shuts down on Ctrl+C. Its broadcast flag supports a
  four-process local demonstration, with `-count` for repeated broadcasts using
  distinct sequences. `-interval` spaces submissions, and `-payload-size` creates
  a fixed-size test value. Any node can initiate broadcasts. Separate-process
  smoke tests build the executable, check delivery of each instance at all four
  nodes, and verify clean shutdown.
- Thesis writing is maintained separately in Overleaf and intentionally excluded
  from this source-code repository.
- The source repository is public; thesis writing remains private in Overleaf.

Each origin has a 1024-sequence receive window and a contiguous delivered prefix.
Old messages are ignored. Messages beyond the window are rejected without storing
them. Local submissions wait when their window is full. Gaps or a replica falling
more than one window behind can stop progress. Values are limited to 1 MiB.
Restart the entire group between runs. Failed ECHO and READY sends are logged
and are not repaired.

## Development

The module requires Go 1.27.2 for builds and generation. Gorums is
pinned to `00c1b1d48897c233c8bc68de9acccc0c482e38e6` from upstream
`master`, including the merged sweep support. This revision provides the runtime API used by current
benchkit. Sweep integration uses `brbbench`, not the node demonstration executable.

```sh
make generate
make fmt
make test
make vet
```

Run `make generate` when the `.proto` definitions change. It is not needed
before every test run. The target downloads the pinned Gorums dependency before
building its generator into `.cache/tools/`, rather than relying on a globally
installed Gorums generator. It still requires `protoc` and `protoc-gen-go` on PATH.

For the first prototype demonstration, follow the
[four-process instructions](cmd/brbnode/README.md#four-process-demonstration).
To check the same setup automatically with a fresh run:

```sh
go test -v -count=1 ./cmd/brbnode -run '^TestFourProcessesDeliver'
```

## Repository map

- `proto/brb/v1/`: Protocol Buffer and Gorums service definitions.
- `internal/brb/`: handwritten protocol state and transition logic.
- `cmd/brbnode/`: node executable and startup instructions.
- `cmd/brbbench/`: diagnostic benchmark and benchkit reports.
- `cmd/brbctl/`: placeholder for a control/demo command.
- `experiments/`: experiment configuration and orchestration.
- `results/`: policy and small reproducibility artifacts, not bulk raw data.

## License

Original project code is licensed under the [MIT License](LICENSE).
Dependencies remain under their respective upstream licenses; see
`THIRD_PARTY_NOTICES.md`.
