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

## First multi-machine check

The next deployment check uses the existing `brbnode` executable on approved
UiS compute hosts. The SSH login gateway is not an experiment host. Confirm the
compute hostnames, ports, storage location, and cleanup procedure before starting
processes. Keep the real host inventory and SSH configuration outside Git.

On a compute host with the desired checkout and Go available:

```sh
git rev-parse HEAD
go version
make test
make vet
GOTOOLCHAIN=go1.26.7 go build -o bin/brbnode ./cmd/brbnode
```

For the first check, use four hosts and `f=1`. On each host, set the same ordered
`peers` variable to the four reachable, approved `host:port` addresses. Do not use
localhost addresses for a multi-machine deployment. Start one foreground process
per host, assigning IDs 1 through 4 according to the peer-list order:

```sh
# On the host assigned ID 1:
./bin/brbnode -id 1 -f 1 -peers "$peers" -broadcast hello -count 3 -interval 1s

# On the other hosts, replace ID with 2, 3, or 4:
./bin/brbnode -id ID -f 1 -peers "$peers"
```

Start all hosts within the 30-second startup timeout. Verify exactly one matching
delivery for each sequence at every node, then stop the whole group with Ctrl+C.
Restart everyone before the next run. After this passes, use the periodic
multi-origin example in [the node instructions](../cmd/brbnode/README.md#periodic-broadcasts).
Check the expected instance counts, not delivery order. Stop the run and
investigate any send errors, missing deliveries, or failed shutdowns. Record the
project and Gorums revisions, toolchain, host set, commands, and log locations.
This is a correctness check over working connections, not a benchmark or a test
of Byzantine behavior.

## Sweep integration

Use the upstream Gorums `cmd/sweep` work for automated cluster experiments once
its revision and use are approved. Run its preflight/check mode before a sweep.
This repository does not contain a competing SSH orchestrator.

`brbnode` remains a demonstration executable, not a sweep benchmark. Use
[`brbbench`](../cmd/brbbench/README.md) for the first diagnostic sweep. It writes
benchkit reports and measures invocation-to-origin-local-delivery latency.
It does not measure delivery at every replica or network bytes.

For an approved four-host setup, run upstream sweep from its benchkit module
directory with a private SSH configuration. Replace the host aliases and paths
below with your approved setup. Run the check first:

```sh
sweep -check -hosts 'compute[1-4]' -config /path/to/private-ssh-config -binary /path/to/brbbench-linux-amd64 -port 9000
sweep -hosts 'compute[1-4]' -config /path/to/private-ssh-config -binary /path/to/brbbench-linux-amd64 -benchmarks BrachaLocalDelivery -n 4 -workers 1 -payload 256 -duration 10s -reps 1 -extra-args '-f=1' -port 9000 -fd-limit 0 -outdir /path/to/ignored-results -sweep first-brb-diagnostic
```

This uses a Mac-side driver and strict host verification from the SSH config.
It runs one closed-loop worker per node, with no warm-up or injected faults.
Sweep deploys the Linux binary, collects reports, and removes its temporary
remote binary and report files. Keep raw results and machine inventories in
ignored storage. Record the BRB source revision separately from sweep's own
revision. For a dirty-source diagnostic, retain the patch, new source files,
and binary checksum alongside the reports. Do not present a single diagnostic
run as the final evaluation.
