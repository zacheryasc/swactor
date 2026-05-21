# The simulator

We deploy distributed algorithms — SWIM today, others in the future — to
environments we don't fully control: WANs across NAT'd hosts, LANs, cloud
GPU rentals, whatever the next thing requires. Iterating on those
algorithms by deploying them is slow and expensive. We pay for cloud time,
we wait for runs to finish, we get one bundle of data per attempt, and the
environment is non-reproducible — two runs of the same code can produce
different outcomes.

The simulator exists to remove that bottleneck. It models the environments
swactor runs in, faithfully enough that algorithms developed against it
behave the same way when deployed. Once faithful enough, the development
loop becomes write, run in sim, iterate, deploy — instead of write, deploy,
wait, repeat.

## What it models, and what it doesn't

The simulator models the network and the transports that run over it. The
network covers arbitrary topology, per-link properties, NAT and
reachability, partitions, anything a real deployment might present.
Transports are modeled separately; iroh is the one we use today, others
will be added as needed.

The algorithms running on top — swactor and whatever it hosts — are not
simulated. They execute as code, against the simulated network and
transport, the same way they execute against the real ones in production.
Running the same code path in sim and in prod is the point.

## Properties it must have

The simulator must be deterministic. Same input, same output, byte for
byte. Tuning algorithms means searching a space of configurations and
comparing outcomes; if outcomes drift from run to run, the search has
nothing solid to compare against.

It must express any environment swactor could be deployed into. A closed
set of named topologies is not enough; real deployments aren't shaped that
way, and a sim limited to such shapes gives answers that don't generalize
past it.

It must be observably equivalent to production. Whatever data we collect
from a real deployment, the simulator must produce the same surface. If
production ever emits something the sim can't, the sim is incomplete —
that direction of asymmetry is always a bug. The form this data takes
today is a diag bundle, but the form will evolve as data collection
matures; the constraint is on the observable surface, not on whatever
artifact happens to carry it.

## The parity bar

Observable equivalence is stronger than matching the schema of whatever
artifact production emits. Every internal observation the process makes
— retry counts, peer-state distributions, timer firings, transport
stats, queue depths, the tails as well as the means — must be
statistically indistinguishable from the corresponding observation in a
real deployment under matched conditions, within the noise floor of the
real measurement. A schema-level match that hides a tail-behavior
mismatch is a parity failure. The process running on top of the sim
must have no means, statistical or otherwise, of detecting that it is
in a sim.

The model floor is whatever today's deployments can record. The model
grows as recording grows. If production ever observes something the
recording cannot capture or the sim cannot reproduce, both are
incomplete — extending recording and extending the sim are the same
project, pursued together.

## Two modes, one engine

The sim runs in two modes against one engine. In the generative mode
the environment is parameterized — topology, per-link policies, peer
populations — and the engine produces runs from that specification. In
the record-replay mode the engine ingests a captured deployment trace
and plays its environment back: the network conditions and the peer
behavior that surrounded the recorded run, rerun against whatever
algorithm code is under test now.

Replay is not a tape of the node-under-test; it is a restoration of
the world that node was in, so that a new version of the node can be
run against the same world. Peers in a replay are not stubs driven by
recorded outputs; they are the peer code, re-executed inside the sim
against the recorded network. The recording must carry enough to make
that re-execution faithful.

The recording schema is both the sim's input format and the sim's
output format. A sim run consumes the same shape it produces. That
shape is the bridge between deployment and sim.

## The hosted world

The sim hosts everything the node-under-test can touch on the wire.
Peers running our code are sim-native — same binary, same runtime
facade, different runtime implementation underneath. Infrastructure we
own — relays, signaling, whatever else lands on a node's path — is
also sim-native. Things we don't own, and that we can only get
faithful behavior from by running the real artifact — a peer on a
different version, a third-party node, a kernel that handles a packet
a particular way — are accommodated by an escape hatch: the sim drives
an opaque binary's I/O through a virtualized boundary, so the binary
still sees a real socket, real time, real interrupts, while the sim
controls what crosses the wire. "Anything it might interact with"
admits no exceptions in the long run; the engine is built to let any
of those interaction surfaces be plugged in.

## Calibration is ongoing

Every real deployment is evidence about how well the sim resembles
reality. The calibration loop is concrete: deploy, collect the
recording, feed it into the sim as the environment, run the same
algorithm version against it, diff the distributions of internal
observations. Where they diverge beyond noise, the sim has a gap to
fix. The sim's quality is measured against that loop, not against
itself.

This is not a one-time validation step. As transports change, as
algorithms evolve, as production collection matures, calibration
continues. A real run that the sim cannot reproduce is a sim bug, not
a curiosity.
