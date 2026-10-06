# BRB diagnostic benchmark

`brbbench` runs the existing Bracha implementation and writes Gorums benchkit
reports for upstream `cmd/sweep`. It is separate from the interactive `brbnode`
demo. Gorums and benchkit are pinned to the same revision in `go.mod`.

Each node originates broadcasts. A worker submits a fresh sequence, waits for
that origin's local delivery, and then starts another broadcast. The latency
sample starts immediately before `Broadcast` and ends in the origin's delivery
callback. Both timestamps use the same process clock. Returning from SEND alone
does not complete a sample. The initial SEND call must also return before the
worker starts its next operation.

The reported throughput counts locally completed origin broadcasts per second,
not phase messages or deliveries at every replica. Summing node throughput
counts the group's completed origin broadcasts. Memory statistics are not
network traffic measurements. This first benchmark has no warm-up period.

Build the binary locally for the compute hosts:

```sh
GOTOOLCHAIN=go1.26.7 GOOS=linux GOARCH=amd64 go build -o bin/brbbench-linux-amd64 ./cmd/brbbench
```

Sweep supplies `-self`, the same ordered `-remotes` list to every node,
`-benchmarks BrachaLocalDelivery`, `-time`, `-workers`, `-payload`, and `-output`.
Pass the fault bound explicitly with sweep's `-extra-args '-f=1'`. Addresses must
be numeric IP:port endpoints. IDs are their positions in the ordered list,
starting at 1. Payloads contain exactly the requested number of `x` bytes,
from zero to 1 MiB.

With rate zero, workers run closed-loop without an imposed rate limit. A fixed
positive rate uses benchkit's pacing. No new broadcasts start after the
measurement deadline. Broadcasts already started can finish, so the recorded
duration includes their completion time. Rate ramps, dedup stream mode, call
timeouts, profiling, and fault injection are not supported and are rejected.

Nodes wait up to 30 seconds for membership connections before measuring. After
measurement, they exchange benchkit completion signals and keep serving for a
one-second trailing-message grace period. This coordination is not part of
Bracha and does not prove that every replica delivered every instance. Startup,
completion, and grace time are outside the measured workload.

Use this only as a short diagnostic on healthy, established connections. Logical
sender IDs are trusted. The implementation does not yet authenticate links or
recover failed phase sends. Completed-instance state remains in memory until
shutdown. A successful report is not a Byzantine correctness proof or a final
performance evaluation.

Tests cover flag validation, missing-peer cancellation, empty and maximum-size
payloads, multiple workers, fixed-rate measurement deadlines, and four local
servers producing readable reports.
The protocol and separate-process demo have their own tests.
