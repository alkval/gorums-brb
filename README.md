# gorums-brb

Capstone project exploring a Byzantine reliable broadcast prototype in Go using
[Gorums](https://github.com/relab/gorums).

The intended starting point is a Bracha-style protocol, but the exact system
model, properties, thresholds, and evaluation requirements remain subject to
supervisor confirmation. The repository therefore begins with research,
reproducibility, and build scaffolding rather than a prematurely fixed protocol
implementation.

## Status

- Local project scaffold created.
- Current Gorums `master` will be pinned as a Go dependency.
- Thesis writing is maintained separately in Overleaf and intentionally excluded
  from this source-code repository.
- Literature screening and supervisor questions are in `docs/`.
- The source repository is public; thesis writing remains private in Overleaf.

## Development

The project targets Go 1.26.7 because the tested Gorums generator currently
passes on Go 1.26 but fails under Go 1.27.

```sh
make test
make vet
```

See [docs/setup.md](docs/setup.md) for environment details.

## Repository map

- `proto/brb/v1/`: future Protocol Buffer and Gorums service definitions.
- `internal/brb/`: handwritten protocol state and transition logic.
- `cmd/`: future node and control/demo commands.
- `experiments/`: experiment configuration and orchestration.
- `results/`: policy and small reproducibility artifacts, not bulk raw data.
- `docs/`: architecture, literature, setup, decisions, and AI-use notes.

## License

Original project code is licensed under the [MIT License](LICENSE).
Dependencies remain under their respective upstream licenses; see
`THIRD_PARTY_NOTICES.md`.
