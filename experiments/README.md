# Experiments

This directory will contain reproducible experiment definitions and orchestration
scripts. It must record at least:

- Gorums and thesis commit identifiers.
- Go and Protobuf versions.
- host and network metadata.
- replica count, fault bound, payload size, and concurrency.
- repetitions, warm-up policy, duration, and random seeds.
- injected Byzantine behavior.
- collection and cleanup procedures.

Large generated output belongs in `experiments/out/` or `results/raw/`, both of
which are ignored. Small manifests and summarized results should be committed.
