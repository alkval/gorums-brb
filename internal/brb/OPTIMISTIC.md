# Optimistic variant

This experiment follows Algorithm 1 in Dan-Cristian Ciacu's *OptiBRB:
Optimistic and Adaptive Byzantine Reliable Broadcast* (TU Delft, 2026).
It keeps full payloads in every message. Hashing is a separate experiment.

For each instance, Q = floor((n+f)/2)+1. PROPOSE goes to all replicas, as in
Algorithm 1. Only Q selected replicas ECHO, each to f+1 selected replicas.
After Q matching ECHOs, those replicas send Q-ECHO to 2f+1 selected replicas.
After f+1 matching Q-ECHOs, those replicas send READY to everyone. Delivery
requires 2f+1 matching READYs. Votes count distinct senders, and a replica sends
at most one vote per phase and instance. There is no READY amplification.

The paper supplies committees with the payload and adapts them using reputation.
Here all nodes derive the same committees from the fixed membership and the
broadcast ID. The cyclic order starts at `(origin-1 + sequence-1) mod n`.
Its first Q, f+1 and 2f+1 members form the three committees. Committees overlap.
Each role rotates once per sequence, balancing assignments over a full cycle.
This deterministic policy is not a reputation mechanism or a claim about
resistance to targeted faults. All nodes must use identical membership and f.

The state machine filters votes by committee membership before allocating state.
The adapter uses subsets of the existing Gorums peer configuration, reusing the
connections. Messages mark their variant, so Bracha and optimistic replicas
reject each other's messages. Restart the entire group when switching variants.
The existing trusted-identity, reliable-channel and sequence-window restrictions
still apply. No new identity protection or retransmission is introduced.

## Guarantees and limits

With an honest origin and responsive correct committees, every correct replica
eventually delivers, subject to the existing transport and window assumptions.
Consistency follows from intersecting Q-sized ECHO quorums and at least one
correct sender among f+1 matching Q-ECHOs. Correct replicas vote only once per
phase, and 2f+1 matching READYs include correct senders.

Without the paper's recovery path, a silent selected member can prevent delivery.
A faulty READY sender can also help one replica reach 2f+1 votes and withhold its
vote from another. The second replica cannot amplify READY and may never deliver.
Thus neither Byzantine liveness nor totality is promised. This is an optimistic
path experiment, not the full OptiBRB protocol. Fault injection in benchmark mode
is rejected for this variant to avoid reporting it as a fault-tolerant comparison.

## Cost and testing

Counting logical sends including self, fault-free Bracha sends n+2n² messages.
This variant sends n+Q(f+1)+(f+1)(2f+1)+n(2f+1), with one extra sequential phase.
For n=4 and f=1 this is 28 instead of 36. These counts are not TCP byte counts,
and lower message counts do not guarantee lower latency or higher throughput.

Tests cover threshold boundaries, noncommittee votes, duplicates, conflicting
votes, rotation, reordered concurrent instances, real Gorums delivery and
benchmark reports. A negative test demonstrates the missing totality guarantee.
Performance reports use `OptimisticLocalDelivery`, with the same origin-local
measurement endpoint as `BrachaLocalDelivery`. Compare identical workloads and
record the source revision. No distributed performance result is claimed yet.
