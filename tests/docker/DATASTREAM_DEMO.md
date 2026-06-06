# Datastream demo cluster

Spins up a small cluster of real `swactor` nodes in Docker, all shipping their
per-node telemetry **datastream** to a single collector that prints the raw
stream — live, frame-by-frame — to stdout. The standalone relay ships its own
stream too, so the whole fleet (relay included) is visible on one wire.

This is the "see the new metrics scheme working" demo. No dashboard; just the
raw stream.

## One-liner

```sh
./tests/docker/datastream-demo.sh
```

Builds everything, brings the cluster up in the foreground, and streams the raw
datastream to your terminal. Ctrl-C tears it down. (Everything below is the
manual breakdown of what that script does.)

## What runs

| Service     | Role                                                                  |
|-------------|-----------------------------------------------------------------------|
| `collector` | Binds UDP `:7700`, decodes each delivery, prints it on arrival        |
| `relay`     | `swactor-iroh-relay`; also ships its own datastream to the collector  |
| `seed`      | Coordinator node (fixed identity)                                     |
| `node-2/3`  | Worker nodes that join the seed                                       |

The emitter is default-on in every node; setting
`SWACTOR_DATASTREAM_COLLECTOR=<ip:port>` (as the compose file does) swaps the
default no-op sink for the UDP frame sink. Each node emits `identity` once,
then `host.resource` / `runtime.stats` / `transport.internals` every ~1s,
`membership` transitions as peers come and go, and any managed process's
stdout/stderr as `proc.<label>.{stdout,stderr}`.

## Run it

From the repo root (the image is multi-stage — Docker compiles the binaries
itself, so you need nothing on the host but Docker):

```sh
# 1. Build the images (first build compiles the workspace; later builds cache)
#    and bring the cluster up in the foreground so the collector's stream is
#    visible in the aggregated log.
docker compose -f tests/docker/docker-compose.datastream.yml build
docker compose -f tests/docker/docker-compose.datastream.yml up

# 2. Ctrl-C to stop, then clean up:
docker compose -f tests/docker/docker-compose.datastream.yml down
```

Watch the `collector-1 | ...` lines. Each is one frame:

```
collector-1  | e8d77206#0 #0     [identity] {"node":"e8d7...","life":0}
collector-1  | e8d77206#0 #1     [host.resource] {"cpu_pct":3.5,"mem_total_mb":31741,"mem_used_mb":4019,...}
collector-1  | e8d77206#0 #2     [runtime.stats] {"actors_live":7,"mailbox_depth":0,"scheduled_tasks":7}
collector-1  | e8d77206#0 #3     [transport.internals] {"direct_peers":2,"relay_connected":true,...}
collector-1  | e98b5ff5#0 #7     [membership] {"from":"unknown","peer":"...","to":"alive"}
```

The prefix is `<node-id-prefix>#<lifetime>`; `#N` is the position within that
node's stream (monotonic, gap-free at the source — gaps in the printed sequence
mean datagrams were lost in transit, which is expected for a best-effort UDP
carrier).

## Local (non-Docker) version

You don't need Docker to see the stream. Run a collector and a couple of nodes
on localhost; they ship over loopback UDP:

```sh
cargo build -p node --bin swactor --bin swactor-datastream-collector

./target/debug/swactor-datastream-collector --bind 127.0.0.1:7700 &

SWACTOR_DATASTREAM_COLLECTOR=127.0.0.1:7700 \
    ./target/debug/swactor --identity-dir /tmp/dsA --dashboard-port 9101 --no-relay --actors 2 &
SWACTOR_DATASTREAM_COLLECTOR=127.0.0.1:7700 \
    ./target/debug/swactor --identity-dir /tmp/dsB --dashboard-port 9102 --no-relay --actors 3 &
```

(Two standalone nodes with `--no-relay` and no seed won't discover each other,
so `membership` stays quiet — that's expected. Every other channel streams.)

## Configuration

| Env var                          | Meaning                                                       |
|----------------------------------|---------------------------------------------------------------|
| `SWACTOR_DATASTREAM_COLLECTOR`   | UDP collector address (`ip:port`); unset = no-op sink         |
| `SWACTOR_LIFETIME`               | Lifetime discriminator (default `0`)                          |

Telemetry emission itself is default-on — the env var only chooses where the
ordered frames ship. Bump `SWACTOR_LIFETIME` across restarts so a re-incarnated
node starts a fresh stream instead of colliding with its prior life.
