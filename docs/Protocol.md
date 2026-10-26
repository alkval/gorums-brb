# Protocol and system model

The baseline implements Bracha's double-echo reliable broadcast. Membership is
fixed, with IDs 1 through n, at most f faulty processes, and n > 3f. The model
is asynchronous: correct processes keep taking steps, and their messages
eventually arrive. Sender identities must be authentic for the protocol
guarantees to hold. Each vote is counted under the peer ID that Gorums reports
for its stream; messages carry no sender field. With `-tls-dir`, every replica
has its own certificate naming `spiffe://gorums-brb/node/<id>`, and Gorums
verifies each peer stream against it in both directions before any handler
runs. Replicas refuse clients without a certificate, and votes or benchmark
control calls from non-members are rejected. This assumes that each replica's
private key and the project CA's signing key are not compromised. Without
`-tls-dir` the transport is plaintext and peer IDs are not verified, which is
allowed only in the terminal demo and in benchmarks explicitly labelled
`auth=none`. The prototype does not repair failed sends to correct processes.

## Bracha message flow

Each instance has an origin and positive sequence number. Only that origin can
initiate PROPOSE. Every phase is sent to all members, including self.

1. A first valid PROPOSE causes one ECHO of its value.
2. More than (n+f)/2 distinct matching ECHOs cause one READY. Two such quorums
   intersect in more than f processes, so they share a correct voter.
3. More than f matching READY messages also cause READY. At least one of those
   senders is correct. The first correct READY must come from an ECHO quorum,
   so faulty votes alone cannot start support for a new value.
4. More than 2f matching READY messages cause delivery. At least f+1 are correct,
   enough to trigger READY amplification at every other correct process. Since
   n-f >= 2f+1, correct processes alone then provide enough votes for delivery.

Each correct process sends at most one ECHO and one READY per instance. Votes
are counted once per sender and phase. Different values never share a count.
State updates are protected by the state-machine mutex. Networking and delivery
callbacks run after the lock and Gorums dispatch ordering have been released.

The intended properties are validity (a correct origin's value is delivered by
all correct processes), no duplication, integrity (a delivered value attributed
to a correct origin was broadcast by that origin), consistency (correct
processes do not deliver different values for an instance), and totality (one
correct delivery implies eventual delivery at every correct process). These
properties rely on the model above. BRB provides no ordering between distinct
instances and is not consensus.

## Bounded prototype workload

The receive window is 1024 sequence numbers per origin. Correct origins use
consecutive sequences starting from 1 and never reuse an ID within a run.
The delivered prefix advances only over contiguous deliveries. Out-of-order
deliveries retain compact records until gaps close.
An old message cannot reopen an instance. A sequence beyond the window is
rejected with an explicit error, without allocating an instance. Values are
limited to 1 MiB. Local sequence reservations use the same bounded state, and
submissions wait for space or context cancellation.

This bounds retained protocol state by the membership and window, including
faulty origins sending arbitrary sequence numbers. It does not bound transport
queues or every process allocation. Missing sequences can exhaust an origin's
window. A correct replica that falls more than a window behind can reject a
later correct broadcast, so unrestricted asynchronous liveness is not claimed
for this resource-limited prototype. Local backpressure alone cannot guarantee
that every remote receive window has space. Experiments must stay within that
skew limit, and any window error invalidates the run. A replica remembers the
violation and refuses to write its benchmark report, even if later broadcasts
deliver. Discard the whole run, including other replicas' reports. Advancing
the prefix to the highest observed sequence would lose earlier deliveries.

Sequence counters and prefixes are not persisted, so individual process
recovery is outside the model. The whole group restarts between runs.

## Optimistic comparison

The optional variant adds Q-ECHO and sends intermediate phases through rotating
committees. It uses the same peer authentication and window restrictions. It omits
recovery and READY amplification, so selected faulty members can prevent
delivery and totality is not guaranteed. Its benchmarks are fault-free.
See the [optimistic variant](../internal/brb/OPTIMISTIC.md) for its rules and limits.

## Controlled faults and measurements

Network tests cover an unavailable peer, a silent peer, equivocation by the
origin and wrong-value ECHOs. A faulty replica acts under its own identity, so
it contributes at most one vote per phase. Simulation
tests reorder and duplicate messages and check the properties above under fair
delivery. These scenarios are evidence, not an exhaustive correctness proof.

The benchmark measures invocation to delivery at the origin, on one clock.
Reports count completed origin broadcasts and cannot establish all-replica
delivery. Startup, completion coordination and the final drain are excluded.
Per-replica delivery rates cannot be inferred from origin throughput.
