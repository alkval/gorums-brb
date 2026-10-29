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

Two optional variants change the protocol. Select at most one, give it to
**every** node, keep the same membership and f on all nodes, and restart the
whole group when changing variants.

- `-optimistic` runs the experimental optimistic path. It uses rotating
  committees and has no recovery, so selected faulty members can stall
  delivery. See [the variant description](../../internal/brb/OPTIMISTIC.md).
- `-ready-digest` keeps Bracha's thresholds and full values in PROPOSE and ECHO,
  but sends only the value's SHA-256 digest in READY. A replica that never
  receives a matching PROPOSE or ECHO cannot deliver. See
  [READY digests](../../docs/Protocol.md#ready-digests).

Without `-tls-dir` the demo runs plaintext and logs that sender IDs are not
verified. To authenticate it, give each node its own credential directory as
described below, for example `-tls-dir creds/node-2` for ID 2.

## Authentication

Votes are counted under the peer ID that Gorums reports for each stream. With
`-tls-dir`, peers carry TLS identities (`spiffe://gorums-brb/node/<id>`) and
Gorums checks each peer stream against the replica's certificate in both
directions before any handler runs. Clients without a certificate are refused,
and benchmark control calls from non-members are rejected. Errors in the
credential files stop the process before it listens. There is no fallback to
plaintext.

Generate a project CA and one directory per replica on the provisioning
machine, outside the repository. List the addresses in node-ID order, exactly
as the replicas receive them in `-peers` or `-remotes`. Certificates name each
address's IP, so the hosts must be reachable at those IPs:

```sh
bin/brb -mode=certs -out /path/to/brb-creds -peers "$addresses" -validity 720h
```

This writes `ca.pem` and `ca-key.pem` to the output root and `node-<id>/` with
`ca.pem`, `node.pem` and `node-key.pem` for each replica.

- Keep `ca-key.pem` on the provisioning machine. Never copy it to a replica.
- Copy only `node-<id>/` to replica `<id>`, for example with `scp` or `rsync`
  over SSH, into a private directory on a local, non-shared disk, for example
  `/path/to/brb-node` holding that replica's three files. Use the same absolute
  path on every host, with mode 0700 for the directory and 0600 for the key. A
  replica refuses a key that group or others can read.
- Pass only that path, `-tls-dir=/path/to/brb-node`. Never put keys in
  arguments, logs, results or Git.
- Remove the directories from every host after the experiments.

These measures assume that private keys stay with their replicas. TLS does not
protect against a stolen key or a compromised account that can read one.

Benchmarks require exactly one of `-tls-dir` and `-plaintext`. Plaintext runs
are only for a labelled comparison of transport cost. Report labels record
`auth=tls` or `auth=none`.

## Benchmark and sweep

Each worker originates broadcasts and waits for local delivery. Latency ends at
that callback, not when the multicast returns. The worker must also wait for
`Broadcast` to return before starting again. If the initial multicast finishes
after local delivery, this extra wait reduces throughput without increasing the
recorded delivery latency. Throughput counts completed origin broadcasts, not
phase messages or deliveries at every replica. State this endpoint in captions.

```sh
make bench-linux
```

Build sweep in a separate Gorums checkout at the release pinned in `go.mod`
(currently `v0.12.0`):

```sh
git clone --no-checkout https://github.com/relab/gorums.git /path/to/gorums-sweep
cd /path/to/gorums-sweep
git checkout --detach v0.12.0
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
  -duration 10s -reps 1 -rate 0 -extra-args '-mode=bench -tls-dir=/path/to/brb-node' \
  -port 9000 -remote-dir /tmp -fd-limit 0 \
  -outdir /path/to/gorums-brb/experiments/out -sweep brb-diagnostic
./cmd/sweep/sweep -check -hosts 'compute[1-4]' \
  -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 -port 9000
```

To vary membership, use a host set large enough for the largest group. For an
approved pool of thirteen machines, preflight that pool and run:

```sh
./cmd/sweep/sweep -check -hosts 'compute[1-13]' \
  -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 -port 9000
./cmd/sweep/sweep -hosts 'compute[1-13]' \
  -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 \
  -benchmarks BrachaLocalDelivery -n 4,7,10,13 -workers 1,2 -payload 256 \
  -duration 10s -reps 3 -rate 0 -extra-args '-mode=bench -tls-dir=/path/to/brb-node' -verbose -trim 1s \
  -port 9000 -remote-dir /tmp -fd-limit 0 \
  -outdir /path/to/gorums-brb/experiments/out -sweep brb-membership
./cmd/sweep/sweep -check -hosts 'compute[1-13]' \
  -config /path/to/private-ssh-config -binary /path/to/brb-linux-amd64 -port 9000
```

Omitting `-f` selects `(n-1)/3`: these groups use f=1, 2, 3 and 4. A fixed
`-f=1` in extra arguments would override that for every group. This baseline
matrix completed on 15 October 2026 using source `b2a85c3`, before peer
authentication, with plaintext and trusted sender IDs. It had 24 trials and
all 204 expected node reports. One trial recorded a host-wide TCP counter change.
This checks the tested configurations, not arbitrary faults or all-replica
delivery for every broadcast. The `compute` aliases above are placeholders,
not a public host inventory. Use only hosts available and permitted for your run.

Use repeated runs and describe the actual trim in the analysis. At the pinned
revision, `-trim 1s` removes throughput intervals whose end offsets are below
one second and the matching prefix of latency samples. In the retained
four/seven- and ten/thirteen-node comparisons it removed one roughly 500-ms
interval, not a full second. Inspect event offsets rather than treating this
flag as an exact per-operation warm-up cutoff.

An operation error fails the affected node's benchmark run, and that node writes
no report. It does not automatically cancel the other workers. The benchmark
does not call benchkit's `RecordError`, so `FailedOps=0` in a written report is
not an independent failure-rate measurement. Check expected reports, manifests
and logs, including receive-window errors and send-failure counters.

To compare the optimistic variant, select `-benchmarks OptimisticLocalDelivery`
and add `-optimistic` to the extra arguments. For READY digests, select
`-benchmarks BrachaDigestLocalDelivery` and add `-ready-digest`. Keep all other
workload settings identical. Fault injection flags are rejected for optimistic
benchmarks but allowed with READY digests. The traffic lines show the smaller
READY messages directly.
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
A faulty replica acts under its own authenticated identity. Failed BRB sends to
correct peers are not repaired. See the root README for the receive-window
assumptions.

Each replica logs the first ECHO and READY multicast failure with error details.
Further failures are counted, with totals printed at normal shutdown. Each count
is one failed multicast call, even if several recipients were unavailable.
These totals are separate from origin completions: a broadcast can still
deliver despite a failed send to a faulty replica. Other errors, including
receive-window violations, are still logged normally. Abruptly killed processes
cannot print a shutdown summary.

## Protocol traffic

Before writing its report, each valid benchmark replica prints one line per
phase, which sweep keeps in the per-run node log:

```text
brb bench: node 3 traffic phase=echo calls=1203 remote_msgs=7218 remote_bytes=... local_msgs=1203 local_bytes=...
```

`calls` counts multicast calls. Messages and bytes count each recipient
separately. Remote messages go to other replicas, and local messages are sends
to the replica itself, which the pinned Gorums runtime serves in-process.
These are attempted protobuf-message bytes, not measured wire bytes. Gorums
envelopes, gRPC and HTTP/2 framing, TCP/IP headers and retransmissions are
excluded. Failed attempts are counted even if nothing was transmitted.

Counting starts when the replica is created and stops at a cutoff taken after
the run context is cancelled and the server has stopped. A handler that was
still running at shutdown may attempt a send after that, but it is not counted,
so the logged totals are final. They cover the whole run, including the final
drain.

The comparison metric is attempted protocol-message bytes per origin
completion: `sum(remote_bytes) / sum(TotalOps)` over all replicas of one trial.
Use the untrimmed `TotalOps` from each report, not completions from sweep's
trimmed analysis, because the traffic totals are untrimmed. Use only complete
fault-free trials in which every expected node report and all its traffic lines
are present. Traffic lines are printed before the report is written, so their
presence alone does not show that a replica succeeded. The metric does
not measure NIC utilisation or actual wire traffic, and it alone cannot show
that bandwidth caused a performance difference.

## Replica deliveries

Each valid benchmark replica also prints how many broadcasts it delivered, from
every origin:

```text
brb bench: node 3 deliveries window_ns=10000000000 in_window=41231 total=41874
```

The replica's delivery throughput in deliveries per second is
`in_window * 1e9 / window_ns`. Each replica counts over its own local window,
which opens when its measurement starts, lasts the configured duration and is
not trimmed, so it is not the same interval as the origin reports' duration.

Both counts are a snapshot taken when the line is printed, after the server
stops. A delivery still in a handler at that point may be missing from either
count, so they are lower bounds. In a complete fault-free trial `total` is at
most the sum of all reports' untrimmed `TotalOps`, and a smaller `total` is not
proof that a broadcast was lost.

`go test -run '^$' -bench Transition ./internal/brb` measures one PROPOSE and
one ECHO transition of the state machine for Bracha and the READY-digest
variant, without networking. The difference is the incremental cost of those
two transitions in these implementations, including the digest variant's
extra allocations, rather than the cost of SHA-256 alone. It does not give the
cost of a whole broadcast or the effect on network latency.

## Diagnostics

Each benchmark replica also prints diagnostic lines when it shuts down,
including after an invalid run, which writes no report. They measure only; the
protocol, thresholds, window and sends are unchanged.

```text
brb bench: node 3 diag origin started=N returned=N failed=0 outstanding=0 before_delivery=N after_delivery=N undelivered=0 after_p50_ns=N after_p99_ns=N after_max_ns=N after_over10ms=N after_over100ms=N after_over1s=N
brb bench: node 3 diag send phase=echo started=N succeeded=N failed=0 cancelled=N outstanding=0 total_ns=N max_ns=N over10ms=N over100ms=N over1s=N oldest_outstanding_ns=0
brb bench: node 3 diag window violations=N
brb bench: node 3 diag window first origin=N phase=echo sender=N sequence=N prefix=N open=N next_allocated=false later_delivered=N highest_delivered=N lowest=N lowest_allocated=false started=false echo_sent=false ready_sent=false proposed=not-retained echoes=none readies=none payloads=not-retained
```

- `diag origin` compares, for each origin broadcast, when `Broadcast` returned
  with when the broadcast was delivered locally. `after_delivery` counts
  returns after delivery, with the distribution of that delay; time spent there
  lowers throughput without raising the recorded latency. `undelivered` counts
  broadcasts the worker saw no delivery for; `outstanding` calls had not
  returned at shutdown.
- `diag send` covers every Gorums `Send` of one phase: successful, failed during
  the run, and cancelled (a failure after the run context ended or reporting
  cancellation). Durations cover completed calls; `outstanding` calls were
  still waiting at shutdown, so their durations are censored and
  `oldest_outstanding_ns` is the age of the longest one.
- `diag window` counts messages rejected for being outside the receive window.
  The `first` line is a snapshot taken when the first one was rejected: the
  origin's delivered prefix, its open instances, whether `prefix+1` exists, how
  many later instances were already delivered, and the state of the lowest
  undelivered instance. Votes are listed as `fingerprint:count`, where the
  fingerprint is the first 8 bytes of the value's SHA-256 digest, so ECHO and
  READY counts for the same value match in both variants. `not-retained` marks
  facts the state does not keep (Bracha keeps values only as vote keys).
- The timing adds two clock reads per send and per origin broadcast; compare
  runs with the same source when judging its overhead. These lines show where
  time is spent, not why: they do not identify a peer, queue or network cause.

### Per-interval samples and the resource guard

Both are off by default. `-diag-interval=1s` prints one sample per interval
from the start of the measurement window, plus a final one at shutdown:

```text
brb bench: node 3 diag sample t_ms=N propose_out=N echo_out=N ready_out=N propose_oldest_ms=N echo_oldest_ms=N ready_oldest_ms=N completions=N deliveries=N goroutines=N heap_objects=N heap_stacks=N
```

- `t_ms` is the actual time since the window start, so late or missed samples
  show as gaps. Each line's fields are read one after another, not as one
  atomic snapshot.
- `*_out` and `*_oldest_ms` are the outstanding `Send` calls per phase and the
  age of the oldest one, as in `diag send`.
- `completions` counts operations at the point the benchmark records a
  success, so the final sample equals the report's total operations on a
  successful run. `deliveries` counts this replica's local deliveries from
  every origin.
- `goroutines`, `heap_objects` and `heap_stacks` are process-wide values
  from `runtime/metrics` (`/sched/goroutines`, `/memory/classes/heap/objects`,
  `/memory/classes/heap/stacks`), read without forcing a GC. Heap object bytes
  include objects not yet reclaimed.

`-guard-heap-objects=BYTES` and `-guard-goroutines=N` (both need
`-diag-interval`) are an intentional benchmark-abort policy, not a
measurement. If a sample exceeds a limit, or the metrics cannot be read while
a limit is set (`reason=metrics_unavailable`, with zero values), the replica
cancels its run first, then prints the sample and one stop line, and writes no
report, even if the measurement had already finished:

```text
brb bench: node 3 diag resource-stop reason=heap_objects heap_objects=N heap_objects_limit=N goroutines=N goroutines_limit=N t_ms=N
```

The heap limit is checked on samples, so it is not a hard memory limit: it
leaves out stacks and other memory, and allocation can overshoot between
samples. Each replica has its own guard; the others carry on until their own
run ends. Record the limits with every run that uses them.

### Linux TCP diagnostic rounds

`-diag-net -diag-interval=1s` adds an opt-in Linux collector. It leaves the
protocol, transport, sampler and resource limits unchanged. Unsupported
platforms and a missing sampling interval are rejected. The collector writes
through a duplicate of stderr's existing descriptor, supporting pipes, SSH
socketpairs and regular files. The duplicate shares the existing file offset
and flags. Raw writes avoid taking the Go stderr write mutex or closing the
original descriptor. Actual writes can still block on the underlying sink,
as can the benchmark's other logging.

Each line starts with `brb bench: node N diag net ` followed by one JSON
object, schema version 1. Common fields are `version`, `round` (zero-based),
`kind`, `phase` (`initial`, `periodic`, `final`), `start_ms`, `end_ms`, and
`anchor_utc`. The offsets are actual monotonic read start/end times since the
measurement window opened. The anchor is that window start in UTC. Files,
replicas and the two ends of a connection are read at different times.

- `kind=socket` has a `socket` object. Only sockets between configured member
  IPs, with the configured benchmark listening port at one end, are included.
  IPv4, IPv6 and IPv4-mapped IPv6 entries are supported. LISTEN entries are
  excluded. `peer` and `role` (`dialed` or `accepted`) identify the other node
  and the local connection direction. The address-free pairing key is
  (`dialer`, `acceptor`, `client_port`): the last field is the dialer's
  ephemeral port, also visible at the acceptor. Opposite directions have
  different keys. This maps membership endpoints, not process ownership.
- `state` is the numeric Linux TCP state. For ESTABLISHED (`1`), `tx_queue`
  includes unsent and unacknowledged bytes, and `rx_queue` is unread bytes.
  `timer_active=1` also covers reorder and loss-probe timers, so it does not
  alone identify an RTO. `timer_ticks` is the raw exported countdown, with no
  assumed tick frequency. `unrecovered_rto` is the current retransmission
  count, not a lifetime total. `zero_window_probes` is the column whose procfs
  header says `timeout`. Other states, including TIME_WAIT, have
  `field_status=state_only` and null queue/timer fields.
- `generation` and `lifecycle` track observations locally, using an internal
  inode that is never logged. Replacement or reappearance of the same key
  sets `reconnect=true`; it is an observation, not proof of a reconnect.
  A different port for a previously seen node pair is marked as a possible
  reconnect and ambiguous, since parallel connections are also possible.
  Duplicate keys and a peer IP shared by several possible senders are marked
  `ambiguous=true`. An unresolved accepted sender has `peer=0` and
  `dialer=0`. Unknown lifecycle has generation zero. Generations need not
  match across replicas. History is bounded and absence is inferred only
  when both TCP files were parsed completely.
- `kind=round` is the footer. Its `summary` contains source statuses and
  byte/entry counts for `tcp`, `tcp6`, `snmp` and `netstat` (TCP rows scanned,
  or requested counters extracted), the socket count,
  problems, and an `unobserved` list of peer/role combinations with no socket
  in this round. Absence is not a zero queue. It can also reflect closing
  connections, read failures, or the timing of the reads. Every matching
  state, including non-ESTABLISHED states, counts as observed. This list does
  not identify missing ESTABLISHED connections.
- The footer's counters are host-wide cumulative `Tcp.RetransSegs`,
  `Tcp.OutSegs` and `TcpExt.TCPTimeouts`, not BRB-only counts. Each has a
  source `status`, nullable `value`, `delta_status` and nullable `delta`.
  A delta requires consecutive valid reads of that counter. Decreases are
  marked `reset`; a missing read breaks the baseline. `initial`, `reset` and
  `unavailable` deltas are null, never zero placeholders.
- `kind=missed` records a range of periodic cadence slots that were not
  collected. Round numbers count actual rounds; slots count intervals since
  the window start. The collector catches up with one current round instead
  of creating workers for missed slots. The slot represented by the current
  round, including a final round, is excluded from the missed range.
- `kind=terminal` has a `terminal` object with a `status` of
  `output_unavailable`, `output_limit` or `shutdown_timeout`. It is a
  best-effort marker: a broken or blocked sink can prevent it from arriving.
  Its offsets give the marker attempt time. It is not a successful final round.

There is one persistent worker, independent of the resource sampler, with at
most one round in flight. Each source is capped at 2 MiB and 16,384 entries;
socket output and lifecycle history are each capped at 256 entries. A round
has at most 256 KiB of output and each line at most 4 KiB. Read, parse, byte,
entry and socket limits are explicit source statuses. Ambiguity and a full
history are explicit round problems. Partial rounds remain diagnostic data.
Raw addresses, hostnames, usernames, uids, inodes and kernel pointers are
never included in these records or collector errors.

Normal shutdown requests a final round and joins the worker. The wait is
capped at two seconds. A timeout cancels collection and stops new round
output. An in-flight read or write may finish later, followed only by a
best-effort terminal marker; the worker then closes its duplicate descriptor.
There is no replacement worker or separate status writer. The join budget
bounds the caller's wait, not the kernel's syscall completion.

A sink failure, output-size limit or shutdown timeout ends network collection
but does not suppress a valid benchmark report or change its exit status.
The validator marks the network capture incomplete using the shutdown status,
terminal record, missing final round or missing footer. An unfinished round
at the end of a node's capture is `truncated_without_terminal` when no marker
arrives, or `truncated_despite_ok_status` if the main line reported `ok`.
If later rounds follow an unfinished round, the capture is invalid.
All present lines still undergo schema, key and consistency checks. Resource stops
and protocol invalidity continue to suppress reports. The resource guard does
not depend on network reads. A fully blocked stderr can still delay ordinary
logging, so this is not a timeout guarantee for all benchmark I/O.

Once the `-diag-net` collector is constructed, the main goroutine also emits
exactly one ordinary shutdown line, after stopping the sampler (and after a
successful report write):

```text
brb bench: node 3 diag net-status status=shutdown_timeout
```

The status is `ok`, `not_started`, `shutdown_timeout`, `output_unavailable`,
`output_limit` or `unavailable_sink`. `not_started` means the collector never
began a round, for example after a startup failure. It takes precedence over
sink status before collection begins, and cannot accompany round/terminal data. Unlike the worker's terminal marker, this line does not
wait for a blocked collector to finish. It still uses ordinary stderr logging.
`ok` means no collector failure was detected at shutdown, not that all source
reads succeeded or the saved capture is complete. Missing shutdown lines make
the capture incomplete. Duplicate lines, node/host mismatches and contradictory
terminal/status pairs are invalid. `unavailable_sink` can accompany a worker
`output_unavailable` marker. A caller's `shutdown_timeout` may follow an earlier
worker output failure if its terminal write then blocked.

A timeout just after a complete final footer may produce a terminal marker
and therefore an incomplete capture. The validator keeps that conservative
classification even though all round data may have arrived. Shutdown metadata
can follow truncated round data; it does not count as a later round.

Validate one saved sweep log with the repository's read-only validator:

```sh
python3 tools/validate-diag-net.py RUN_LOG --manifest MANIFEST --nodes 13 \
  --duration-s 30 --interval-s 1
python3 -m unittest discover -s tools -p 'test_validate_diag_net.py'
go test ./internal/bench -run '^$' -bench BenchmarkNetDiagRound -benchmem
```

Run these from the repository root. The validator checks schema, source and
counter consistency, node/host binding, socket keys, initial/final rounds and
cadence. Exit 0 means complete capture, 1 invalid, and 4 incomplete. It lists
unmatched established socket keys across the capture without assuming that
both endpoints were sampled simultaneously. It supplements the sample,
resource-stop, report and trial checks. The benchmark measures fixture
parsing, mapping and formatting with a discarded sink; it excludes procfs I/O
and actual log transport. Record a Linux capability/overhead check before
using this option for retained distributed measurements. That check must record
stderr's type under the actual SSH exec setup and whether Gorums/gRPC keeps
multiple connections for one ordered dialer/acceptor pair. Parallel connections
could trigger the possible-reconnect ambiguity heuristic; resolve how they
will be interpreted before the two diagnostic trials.
