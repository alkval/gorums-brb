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

For the experimental optimistic variant, add `-optimistic` to **every** node's
command. Keep the same membership and f on all nodes and restart the whole group
when changing variants. It uses rotating committees and has no recovery.
Selected faulty members can stall delivery. See [protocol details](../../internal/brb/OPTIMISTIC.md).

## Benchmark and sweep

Each worker originates broadcasts and waits for local delivery. Latency ends at
that callback, not when the multicast returns. Throughput counts completed
origin broadcasts. State this measurement endpoint in figure captions.

```sh
make bench-linux
```

Build sweep in a separate Gorums checkout at the revision pinned in `go.mod`
(currently `00c1b1d48897c233c8bc68de9acccc0c482e38e6`):

```sh
git clone --no-checkout https://github.com/relab/gorums.git /path/to/gorums-sweep
cd /path/to/gorums-sweep
git checkout --detach 00c1b1d48897c233c8bc68de9acccc0c482e38e6
make sweep
cd benchkit
```

The executable is `benchkit/cmd/sweep/sweep`. Run it from the `benchkit` module
root, using absolute paths for the BRB binary, SSH configuration and output.
Replace `/path/to/...` and the example `compute` SSH aliases with your setup.
Run on the approved driver machine, with approved hosts, ports and workload
settings. For a four-host diagnostic, run preflight, the workload, then postflight:

```sh
./cmd/sweep/sweep -check -hosts 'compute[1-4]' \
  -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 -port 9000
./cmd/sweep/sweep -hosts 'compute[1-4]' \
  -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 \
  -benchmarks BrachaLocalDelivery -n 4 -workers 1 -payload 256 \
  -duration 10s -reps 1 -rate 0 -extra-args '-mode=bench' \
  -port 9000 -remote-dir /tmp -fd-limit 0 \
  -outdir /path/to/gorums-brb/experiments/out -sweep brb-diagnostic
./cmd/sweep/sweep -check -hosts 'compute[1-4]' \
  -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 -port 9000
```

To vary membership, use a host set large enough for the largest group. For an
approved pool of twelve machines, preflight that pool and run:

```sh
./cmd/sweep/sweep -check -hosts 'compute[1-12]' \
  -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 -port 9000
./cmd/sweep/sweep -hosts 'compute[1-12]' \
  -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 \
  -benchmarks BrachaLocalDelivery -n 4,7,10 -workers 1,2 -payload 256 \
  -duration 10s -reps 3 -rate 0 -extra-args '-mode=bench' -verbose -trim 1s \
  -port 9000 -remote-dir /tmp -fd-limit 0 \
  -outdir /path/to/gorums-brb/experiments/out -sweep brb-membership
./cmd/sweep/sweep -check -hosts 'compute[1-12]' \
  -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 -port 9000
```

Omitting `-f` selects `(n-1)/3`: the groups above use f=1, 2 and 3. A fixed
`-f=1` in extra arguments would override that for every group. The review's
4/7/10/13-node matrix additionally needs thirteen approved machines. With that
inventory, expand the host range to `[1-13]` and use `-n 4,7,10,13`, giving
f=1, 2, 3 and 4. The pool from `pitter1` through `pitter12` alone cannot provide
thirteen separate hosts.
These are run recipes, not claims that the larger matrix has been executed.

Use sweep's `-trim` for warm-up removal and repeated runs for evaluation.

To compare the optimistic variant, select `-benchmarks OptimisticLocalDelivery`
and use `-extra-args '-mode=bench -optimistic'`. Keep all other workload settings
identical. Fault injection flags are rejected for optimistic benchmarks.
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

Every benchmark run completes startup round-trip probes and a group barrier
before measurement, including ordinary Bracha and optimistic runs. This prevents
other origins from running ahead while a connected replica is still starting.
The `-fail-fast` flag only changes transport error handling. Startup
allows 30 seconds plus one second per replica. Completion allows 10 seconds plus
100 ms per replica, followed by a one-second drain. These are outside measurement.

If startup fails, check addresses, ports and that every member is running.
Any replica that observes a sequence-window violation rejects its benchmark
result and writes no report. Investigate gaps or lag and discard that run,
including reports collected from other replicas.
Logical identities are trusted, and failed BRB sends to correct peers are not
repaired. See the root README for the receive-window assumptions.

Each replica logs the first ECHO and READY multicast failure with error details.
Further failures are counted, with totals printed at normal shutdown. Each count
is one failed multicast call, even if several recipients were unavailable.
These totals are separate from failed measured operations: a broadcast can still
deliver despite a failed send to a faulty replica. Other errors, including
receive-window violations, are still logged normally. Abruptly killed processes
cannot print a shutdown summary.
