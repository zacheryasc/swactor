# Datastream demo cluster

Spins up a small cluster of real `swactor` nodes in Docker, each driving a dummy
child process, all shipping their per-node telemetry **datastream** to a single
collector that prints the raw stream — live, frame-by-frame — to stdout.

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

| Service     | Role                                                              |
|-------------|-------------------------------------------------------------------|
| `collector` | Binds UDP `:7700`, decodes each delivery, prints it on arrival    |
| `relay`     | `swactor-iroh-relay` so nodes can find each other over the bridge |
| `seed`      | Coordinator node (fixed identity), drives a dummy workload        |
| `node-2/3`  | Worker nodes that join the seed, each drives a dummy workload      |

Each node emits `identity` once, then `host.resource` / `runtime.stats` /
`transport.internals` every ~1s, `membership` transitions as peers come and go,
and its dummy child's stdout/stderr as `proc.workload.{stdout,stderr}`.

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
collector-1  | e8d77206#1780337699 #0     [identity] {"node":"e8d7...","region":"seed","role":"coordinator",...}
collector-1  | e8d77206#1780337699 #1     [proc.workload.stdout] tick 1 — workload working
collector-1  | e8d77206#1780337699 #2     [host.resource] {"cpu_pct":3.5,"mem_total_mb":31741,"mem_used_mb":4019,...}
collector-1  | e8d77206#1780337699 #3     [runtime.stats] {"actors_live":7,"mailbox_depth":0,"scheduled_tasks":7}
collector-1  | e8d77206#1780337699 #4     [transport.internals] {"direct_peers":2,"relay_connected":true,...}
collector-1  | e98b5ff5#1780337699 #7     [membership] {"from":"unknown","peer":"...","to":"alive"}
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

./target/debug/swactor --identity-dir /tmp/dsA --dashboard-port 9101 --no-relay \
    --datastream --datastream-collector 127.0.0.1:7700 --datastream-region A --actors 2 &
./target/debug/swactor --identity-dir /tmp/dsB --dashboard-port 9102 --no-relay \
    --datastream --datastream-collector 127.0.0.1:7700 --datastream-region B --actors 3 &
```

(Two standalone nodes with `--no-relay` and no seed won't discover each other,
so `membership` stays quiet — that's expected. Every other channel streams.)

## Flags

| Flag                          | Meaning                                                  |
|-------------------------------|----------------------------------------------------------|
| `--datastream`                | Enable telemetry emission (off by default)               |
| `--datastream-collector H:P`  | UDP collector address; implies `--datastream`            |
| `--datastream-region R`       | Region label in the `identity` record                    |
| `--no-datastream-child`       | Don't spawn the dummy child workload                     |
| `--datastream-child-label L`  | Name for the `proc.<L>.*` channels (default `workload`)  |

The lifetime discriminator is taken from `$SWACTOR_LIFETIME` if set, else the
current UNIX seconds — bump it across restarts so a re-incarnated node starts a
fresh stream.
