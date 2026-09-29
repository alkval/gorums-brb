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

Stop all four processes with Ctrl+C when finished. Restart the whole group
before repeating this demonstration. The one-shot trigger always uses sequence
1, and surviving processes retain the completed-instance marker from a prior
run. Restarting only the origin could therefore suppress the next delivery.

## Configuration

- `-id`: this node's ID, from 1 through the number of peers.
- `-f`: the fault bound. This flag is required, even when the bound is zero.
- `-peers`: comma-separated `host:port` addresses in node-ID order. The first
  address belongs to node 1, the second to node 2, and so on.
- `-broadcast`: optional nonempty value to broadcast once. An empty value or
  an omitted flag means this process does not initiate a broadcast.

Every process must use the same ordered peer list and fault bound. Its own
address is also its listening address. Hostnames are resolved at startup.
Ports must be fixed and nonzero. Duplicate endpoints, wildcard addresses,
invalid IDs, and configurations that violate `n > 3f` are rejected.

The command creates one `gorums.System`. `WithConfig` assigns its server identity
and membership, while `WithOutboundNodes` provides the configuration used by
the generated multicast calls. The BRB service is registered before `Serve`
starts. Deliveries print the local node ID, origin, sequence number, and value.
The gRPC `WaitForReady` option lets initial streams wait while the other
processes start. It does not add protocol-level retries or guarantee recovery
of BRB messages after a connection failure.

Ctrl+C or SIGTERM cancels the protocol context, then closes the Gorums system,
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
not a Byzantine fault scenario or a multi-machine deployment. There is no
cluster runner or performance instrumentation yet. All peers must be connected
before the one-shot trigger sends, even when `f` is greater than zero.

Command tests cover argument validation, startup, cancellation, port release,
an occupied listening port, the one-shot trigger delivering to itself, and
cancellation while waiting for peers or about to initiate a broadcast.
The existing BRB tests cover protocol behavior. `TestFourProcessesDeliver`
builds a temporary executable and runs four separate processes with `n=4,f=1`
on temporary localhost ports. It starts the origin first to check startup while
peers are still missing. It checks that every process logs exactly one
delivery of `origin=1, sequence=1, value="hello"`, and rejects other deliveries.
It allows 20 seconds for delivery, sends SIGTERM to all children during cleanup,
and fails if a child exits early, exits with an error, or needs a forced stop.
Failed runs include the process logs in the test output. The temporary binary and logs are
removed by Go's test cleanup.

Run just this smoke test without cached results:

```sh
GOTOOLCHAIN=go1.26.7 go test -v -count=1 ./cmd/brbnode -run '^TestFourProcessesDeliver$'
```

`make test` includes it. `go test -short ./...` skips this smoke test, but still
runs the other tests, including the in-process Gorums integration tests.
This is a local correctness check, not a benchmark or a cluster orchestrator.
Run all checks from the repository root:

```sh
make fmt
make test
make vet
```
