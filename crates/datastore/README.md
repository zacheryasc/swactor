# swactor-datastore **CURRENTLY OBSELETE**


Distributed content-addressed datastore built on [swactor](../../README.md). Objects are split into fixed-size chunks, identified by their blake3 hash, and replicated across a peer-to-peer network via epidemic gossip.

## Building

Node binary (HTTP server + actor runtime):

```sh
cargo build -p swactor-datastore --features node
```

CLI client:

```sh
cargo build -p swactor-datastore --features cli
```

Both at once:

```sh
cargo build -p swactor-datastore --features node,cli
```

## Node

Start a datastore node:

```sh
swactor-store-node
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `9091` | HTTP API port |
| `--storage-path` | *(in-memory)* | Directory for persistent storage |
| `--dashboard-port` | *(disabled)* | Runtime dashboard port |
| `--chunk-size` | `1048576` | Chunk size in bytes (1 MB) |
| `--gc-interval` | `1000` | GC interval in ticks (~100s) |
| `--disseminate-interval` | `50` | Gossip interval in ticks (~5s) |
| `--config` | *(none)* | Path to a TOML config file |

Example with persistent storage and dashboard:

```sh
swactor-store-node --storage-path ./data --dashboard-port 9090
```

### Config file

Create a `store.toml` and pass it with `--config`:

```toml
port = 9091
storage_path = "./my-data"
dashboard_port = 9090
chunk_size = 1048576
gc_interval = 1000
disseminate_interval = 50
```

CLI flags override config file values. Omitted fields use built-in defaults.

```sh
swactor-store-node --config store.toml --port 8080
```

## CLI

The `swactor-store` command talks to a running node over HTTP.

### Status

```sh
swactor-store status
```

### Put

```sh
swactor-store put photo.jpg --name "vacation"
```

### Get (metadata)

```sh
swactor-store get <hash>
```

### Get (download)

```sh
swactor-store get <hash> --output photo.jpg
```

### Delete

```sh
swactor-store delete <hash>
```

### List (local)

```sh
swactor-store list
```

### List (swarm-wide)

```sh
swactor-store list --all
```

### Filter by name

```sh
swactor-store list --name vacation
```

Use `--url` to point at a different node:

```sh
swactor-store --url http://192.168.1.50:9091 list
```

## Web UI

Visit `http://<host>:<port>/` in a browser. The UI supports uploading, listing, downloading, inspecting, and deleting objects — works on desktop and mobile.

## HTTP API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/status` | Node identity |
| `POST` | `/api/put?name=...` | Upload (body = raw bytes) |
| `GET` | `/api/list` | List objects (`?all=true` for swarm) |
| `GET` | `/api/get?hash=...` | Object metadata + manifest |
| `GET` | `/api/data?hash=...` | Download reassembled binary |
| `POST` | `/api/delete?hash=...` | Delete object |
