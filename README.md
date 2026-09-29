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
- A minimal [node executable](cmd/brbnode/README.md) starts one Gorums system,
  prints deliveries, and shuts down on Ctrl+C. Its one-shot broadcast flag
  supports a four-process local demonstration. A separate-process smoke test
  builds the executable, checks all four deliveries, and verifies clean shutdown.
- Thesis writing is maintained separately in Overleaf and intentionally excluded
  from this source-code repository.
- The source repository is public; thesis writing remains private in Overleaf.

## Development

The project targets Go 1.26.7 because the tested Gorums generator currently
passes on Go 1.26 but fails under Go 1.27.

```sh
make generate
make fmt
make test
make vet
```

For the first prototype demonstration, follow the
[four-process instructions](cmd/brbnode/README.md#four-process-demonstration).
To check the same setup automatically with a fresh run:

```sh
GOTOOLCHAIN=go1.26.7 go test -v -count=1 ./cmd/brbnode -run '^TestFourProcessesDeliver$'
```

## Repository map

- `proto/brb/v1/`: Protocol Buffer and Gorums service definitions.
- `internal/brb/`: handwritten protocol state and transition logic.
- `cmd/brbnode/`: node executable and startup instructions.
- `cmd/brbctl/`: placeholder for a control/demo command.
- `experiments/`: experiment configuration and orchestration.
- `results/`: policy and small reproducibility artifacts, not bulk raw data.

## License

Original project code is licensed under the [MIT License](LICENSE).
Dependencies remain under their respective upstream licenses; see
`THIRD_PARTY_NOTICES.md`.
