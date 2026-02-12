# Distribution Crate — Design Notes

Design decisions behind non-obvious mechanisms in the distribution crate.

---

## Transmit Budget (dissemination.rs)

The transmit budget controls how many times a membership update gets piggybacked onto
protocol messages before being evicted from the dissemination queue.

It is computed as **`Λ * ceil(log₂(n))`** where `Λ` (lambda) is a configurable multiplier
and `n` is the cluster size. The logarithmic scaling ensures that in a 10-node cluster
each update is sent ~4Λ times, while in a 1000-node cluster it gets ~10Λ sends — enough
redundancy for epidemic-style convergence without flooding the network.

Each time an update is piggybacked onto a Ping or Ack message, its remaining budget
decrements by 1. When the budget reaches zero the update is evicted from the queue.
Higher-priority updates (e.g. deaths) are piggybacked first, so critical state changes
propagate faster than routine alive announcements.

## Re-Replication (kademlia/repair.rs — RepairQueue)

In the Kademlia directory, each actor's location entry is STOREd on the `r` closest nodes
(by XOR distance to the actor address). When one of those replica holders dies, the
replication factor drops below `r`.

**Re-replication** restores the target replication factor: surviving nodes that detect the
death extract all directory entries the dead node held and re-STORE them on the
next-closest node that didn't already have a copy.

In practice: `RepairQueue::on_node_death()` pulls all entries authored by the dead node
from the local `DirectoryShard` and queues them. The node's tick loop drains the queue
and issues STORE RPCs to the new r-closest nodes, restoring the replication invariant.

## Periodic Republish (kademlia/repair.rs — RepublishTracker)

Topology churn — nodes joining and leaving — gradually shifts which nodes are "r-closest"
to a given actor address in XOR space. Without periodic republishing:

- A new node that joins *closer* to an actor than existing replicas would never learn
  about that actor's entry.
- Entries could become stranded on nodes that are no longer among the closest, making
  lookups slower or requiring more hops.

`RepublishTracker` has each node periodically re-STORE the directory entries for its own
locally-spawned actors at a configurable interval. This ensures entries migrate to the
current r-closest nodes as the topology evolves, without waiting for a failure event to
trigger repair.
