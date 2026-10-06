# brbnode

A minimal process that listens for BRB messages through Gorums and prints
deliveries. It uses the existing state machine and transport adapter unchanged.

## Run

From the repository root, start a single node to check startup and shutdown:

```sh
GOTOOLCHAIN=go1.26.7 go run ./cmd/brbnode \
  -id 1 -f 0 -peers 127.0.0.1:7001
```

Press Ctrl+C to stop it. The `listening` message means the node has opened its
port, not that all peers are connected or that a broadcast has been delivered.
Without `-broadcast`, it only handles incoming messages.

## Four-process demonstration

Build the executable once:

```sh
GOTOOLCHAIN=go1.26.7 go build -o bin/brbnode ./cmd/brbnode
```

Open four terminals in the repository root. In each terminal, set the same list:

```sh
peers="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004"
```

Start nodes 2, 3, and 4 in their own terminals:

```sh
./bin/brbnode -id 2 -f 1 -peers "$peers"
```

```sh
./bin/brbnode -id 3 -f 1 -peers "$peers"
```

```sh
./bin/brbnode -id 4 -f 1 -peers "$peers"
```

Then start node 1 in the remaining terminal:

```sh
./bin/brbnode -id 1 -f 1 -peers "$peers" -broadcast hello
```

Node 1 waits up to 30 seconds for the full Gorums peer configuration, then
broadcasts with sequence 1. This is a connection check, not a BRB quorum or a
test of faulty-process tolerance. Each process should print its own delivery:

```text
node 1 delivered origin=1 sequence=1 value="hello"
node 2 delivered origin=1 sequence=1 value="hello"
node 3 delivered origin=1 sequence=1 value="hello"
node 4 delivered origin=1 sequence=1 value="hello"
```

The `submitted` line means node 1 completed the initial multicast call, not
that all nodes delivered. Check the `delivered` line in each terminal. These
logs may interleave, so local delivery can appear before `submitted`.

To send several broadcasts in the same run, use this command for node 1 instead:

```sh
./bin/brbnode -id 1 -f 1 -peers "$peers" -broadcast hello -count 3
```

Node 1 waits for peers once, then submits the same value with sequence numbers
1, 2, and 3. Every node should print one delivery for each sequence. Submissions
do not wait for all deliveries, so instances may overlap and delivery order is
not guaranteed. After submitting the requested broadcasts, the node stays
running to handle messages. This is a finite demo, not an interactive message prompt.

## Periodic broadcasts

Any node can initiate broadcasts, not just node 1. For example, restart the whole
group and run these commands in the four terminals, using the same `peers` list:

```sh
./bin/brbnode -id 1 -f 1 -peers "$peers" -payload-size 256 -count 10 -interval 100ms
./bin/brbnode -id 2 -f 1 -peers "$peers" -payload-size 256 -count 10 -interval 200ms
./bin/brbnode -id 3 -f 1 -peers "$peers" -payload-size 256 -count 10 -interval 300ms
./bin/brbnode -id 4 -f 1 -peers "$peers" -payload-size 256 -count 10 -interval 400ms
```

Each node submits ten broadcasts with its own origin ID and sequences 1 through
10. Each node should therefore deliver 40 distinct instances. The first
submission follows the connection check. Before each later submission, the node
waits for its interval after the previous multicast call returns. It does not
wait for delivery. A slow call increases the spacing, so this is not a fixed-rate
benchmark and the configured interval is not a measured throughput.

The generated value contains exactly the requested number of `x` bytes. It is
reused across that node's broadcasts. To study different sizes, repeat the run
with another size, keeping the other parameters fixed. Values longer than 64
bytes are logged as `bytes=256`, for example, instead of printing the payload.
These logs check size and delivery IDs, not payload contents or performance.

The count bounds the workload. After the last submission, nodes remain alive to
receive messages. Stop the whole group after checking all expected deliveries.
Ctrl+C or SIGTERM also interrupts an interval wait. Long runs retain more
completed-instance markers and produce more logs, so start with small counts.

Stop all four processes with Ctrl+C when finished. Restart the whole group
before repeating the demonstration. Sequences start at 1 on each launch, and
surviving processes retain completed-instance markers from a prior run.
Restarting only the origin could therefore suppress subsequent deliveries.

## Configuration

- `-id`: this node's ID, from 1 through the number of peers.
- `-f`: the fault bound. This flag is required, even when the bound is zero.
- `-peers`: comma-separated `host:port` addresses in node-ID order. The first
  address belongs to node 1, the second to node 2, and so on.
- `-broadcast`: optional nonempty value to broadcast. An empty value or
  an omitted flag means this process does not initiate a broadcast unless
  `-payload-size` is positive.
- `-count`: number of broadcasts to submit, default 1. Must be positive.
  Values greater than 1 require `-broadcast` or a positive `-payload-size`.
  Each broadcast uses the same value but a distinct sequence number, from 1
  through `count`.
- `-interval`: nonnegative delay between submissions, default `0` (no delay).
  Accepts Go durations such as `100ms` or `1s`. A positive interval requires
  `-broadcast` or a positive `-payload-size`.
- `-payload-size`: generate a value of this many bytes instead of `-broadcast`.
  Default `0` disables generation. Positive sizes are limited to 1 MiB for this
  workload, below the default gRPC receive limit. The two payload options cannot
  be used together.

Every process must use the same ordered peer list and fault bound. Its own
address is also its listening address. Hostnames are resolved at startup.
Ports must be fixed and nonzero. Duplicate endpoints, wildcard addresses,
invalid IDs, and configurations that violate `n > 3f` are rejected.

The command binds its listening port and creates one `gorums.Server`.
`WithPeers` assigns its identity and fixed membership and configures outbound
calls. The adapter uses `PeerConfig` and registers the BRB service directly on
that server before `Serve` starts. `WaitForPeers` checks the connected membership
before initiating broadcasts. Deliveries print the local node ID, origin,
sequence number, and value or byte length. Calls to the local member are served
in-process by this Gorums revision. It still takes part in the same protocol and
counts as one distinct sender, but self-delivery does not use a loopback stream.
The gRPC `WaitForReady` option lets initial streams wait while the other
processes start. `WithBackoff` sets an initial connection retry delay of 100 ms
and a maximum backoff of 1 second, keeping the default multiplier and jitter.
This reduces the extra wait when nodes start at different times. The settings
control connection retries, not BRB latency or a delivery deadline. They cause
more frequent connection attempts while a peer is unavailable and should be
reviewed before cluster experiments. Neither option adds protocol-level retries
or guarantees recovery of BRB messages after a connection failure.

Ctrl+C or SIGTERM cancels the protocol context, then closes the Gorums server,
connections, and listening port. A port already in use produces a startup error.

## If the demonstration fails

- If a port is already in use, stop the previous demo processes or choose four
  other ports. Update the peer list in every terminal.
- If node 1 times out waiting for peers, check that all four processes are
  running with the same ordered addresses and fault bound. Stop the group and
  restart it after correcting the configuration.
- If a second run does not deliver, restart all four processes. Restarting
  only node 1 reuses an identifier that the other nodes have already delivered.

For a meeting, show the four delivery lines and explain the path from `SEND`
to `ECHO` to `READY`. Each terminal is a separate process, including the
origin, which also takes part in the protocol and delivers. This demonstrates
the correct-process case over localhost. The state-machine tests separately
exercise duplicates, conflicting messages, thresholds, and reordering.

## Limits and tests

This is an unauthenticated prototype with no transport recovery. Use it only
on localhost or an approved test network. It does not verify that peers use
identical configurations. The demo uses four correct processes on one machine,
not a Byzantine fault scenario or a multi-machine deployment. This executable
is a demo, not a benchmark. The separate [brbbench command](../brbbench/README.md)
provides origin-local delivery measurements for upstream sweep. All peers must
be connected before the demo submits broadcasts, even when `f` is greater than zero.

Command tests cover argument validation, submission spacing, generated payload
sizes, cancellation during an interval wait, startup, cancellation, port release,
an occupied listening port, the one-shot trigger delivering to itself, and
cancellation while waiting for peers or about to initiate a broadcast.
The existing BRB tests cover protocol behavior. `TestFourProcessesDeliver`
builds a temporary executable and runs four separate processes with `n=4,f=1`
on temporary localhost ports. It starts the origin first to check startup while
peers are still missing. It checks that every process logs exactly one
delivery of `origin=1, sequence=1, value="hello"`, and rejects other deliveries.
`TestFourProcessesDeliverWithLateOrigin` repeats the check with the receivers
started first and the origin started two seconds later, exercising connection
retries while the origin is missing. Both tests log the time from the last
process starting to all four deliveries, without imposing a tight timing limit.
`TestFourProcessesDeliverRepeatedBroadcasts` starts the origin with `-count 3`
and checks exactly one matching delivery for each of sequences 1, 2, and 3 at
every process, without requiring a particular delivery order.
`TestFourProcessesDeliverPeriodicBroadcastsFromEveryNode` runs all four nodes
as senders with 256-byte values, three broadcasts each, and an interval of 50 ms.
It checks that every process logs exactly one delivery for each of the twelve
origin/sequence pairs, with the expected size, regardless of delivery order.
`TestFourProcessesDeliverDifferentWorkloads` repeats the multi-origin check with
different intervals, counts, and payload sizes across the four senders, including
1-byte and 1-MiB values and the 64/65-byte log-format boundary.
Each of these five smoke tests allows 20 seconds for delivery, sends SIGTERM to
all children during cleanup, and fails if a child exits early, exits with an error,
or needs a forced stop. Failed runs include the process logs in the test output.
The temporary binary and logs are removed by Go's test cleanup.

`TestRunGeneratedPayloadContents` runs the sender code and a receiving Gorums
node in the test process. It checks every delivered byte against `x`, across two
broadcasts for each of the sizes 1, 64, 65, 256, and 1048576. Unlike the large-value
log checks, it verifies actual payload contents after network delivery. Each
size has an eight-second delivery deadline. Cleanup cancels the node contexts
and stops the servers directly rather than sending signals to child processes.

Run all smoke tests without cached results:

```sh
GOTOOLCHAIN=go1.26.7 go test -v -count=1 ./cmd/brbnode -run '^TestFourProcessesDeliver'
```

`make test` includes all five smoke tests. `go test -short ./...` skips these smoke tests, but still
runs the other tests, including the in-process Gorums integration tests.
This is a local correctness check, not a benchmark or a cluster orchestrator.
Run all checks from the repository root:

```sh
make fmt
make test
make vet
```
