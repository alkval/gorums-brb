# Running BRB

Build from the repository root with `make build`. Use `bin/brb -h` for demo
flags and `bin/brb -mode=bench -h` for benchmark flags.

## Terminal demo

Set this in each of four terminals:

```sh
peers=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004
```

Start receivers with `bin/brb -id 2 -peers "$peers"`, then IDs 3 and 4.
Start the origin with:

```sh
bin/brb -id 1 -peers "$peers" -broadcast hello -count 3
```

Every replica should deliver sequences 1, 2 and 3 once. Their order may differ.
Stop all replicas with Ctrl+C before repeating. The demo uses `-broadcast` and
`-count`. Payload sizes and paced workloads belong to benchmark mode.

## Benchmark and sweep

Each worker originates broadcasts and waits for local delivery. Latency ends at
that callback, not when the multicast returns. Throughput counts completed
origin broadcasts. State this measurement endpoint in figure captions.

```sh
make bench-linux
```

Build sweep from the Gorums revision in `go.mod`, and run it from that checkout's
`benchkit` directory. For an approved four-host setup:

```sh
sweep -check -hosts 'compute[1-4]' -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 -port 9000
sweep -hosts 'compute[1-4]' -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 -benchmarks BrachaLocalDelivery -n 4 -workers 1 -payload 256 -duration 10s -reps 1 -extra-args '-mode=bench' -port 9000 -fd-limit 0 -outdir /path/to/results -sweep brb-diagnostic
```

Use sweep's `-trim` for warm-up removal and repeated runs for evaluation.
Use `-stats-mode hdr` for long runs. No separate warm-up phase is implemented.
Capture `make provenance BINARY=bin/brb-linux-amd64` beside each sweep manifest.
It prints the checkout state, binary build information and SHA-256.

## Controlled faults

All members must complete startup before fault timing begins. Use the same
ordered membership and fault settings on all nodes. Fault modes enable
`-fail-fast`, which reports unavailable peers promptly. Correct-peer links must
remain healthy. Choose one faulty replica with `-fault-node` (default: last ID).

- `-fault-kill-after=2s` abruptly exits that replica two seconds after startup.
- `-fault=silent` receives messages but sends no protocol votes.
- `-fault=wrong-echo` echoes a different value under its own ID.
- `-fault=equivocate` proposes A and B to different replicas for one instance.

Pass these in sweep's `-extra-args`, alongside `-mode=bench`. Keep the injected
count within f. Faulty replicas produce no measurement file. Inspect survivor
reports and missing files together. A missing completion signal is recorded,
but does not itself prove a crash. Survivor success only establishes local
completion. Benchkit profiling, rate ramps, per-call timeouts, buffer-size sweeps
and stream deduplication are not supported by this command.

Startup round-trip probes and a barrier precede fail-fast measurement. Startup
allows 30 seconds plus one second per replica. Completion allows 10 seconds plus
100 ms per replica, followed by a one-second drain. These are outside measurement.

If startup fails, check addresses, ports and that every member is running.
Any replica that observes a sequence-window violation rejects its benchmark
result and writes no report. Investigate gaps or lag and discard that run,
including reports collected from other replicas.
Logical identities are trusted, and failed BRB sends to correct peers are not
repaired. See the root README for the receive-window assumptions.
