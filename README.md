# Distributed Hash Table (Chord)

A Distributed Hash Table (DHT) is a distributed system that provides a lookup service similar to a hash table: `(key, value)` pairs are stored across many nodes, and any participating node can efficiently retrieve the value associated with a given key. The goal is to store and retrieve data in a scalable, efficient and reliable manner.

This repository implements the **Chord** protocol in Go. A node maintains a finger table and a successor list, replicates data to its successors, and keeps the ring consistent through periodic stabilization. Communication between nodes is done exclusively over the network using Go's `net/rpc`.

## Project Layout

```
.
├── main.go                  CLI entry point: interactive REPL + optional TCP command server
├── go.mod / go.sum          module `dht`, Go 1.18 (logrus, fatih/color)
├── node/                    Chord core
│   ├── interface.go         the DhtNode interface
│   ├── node.go              Chord protocol (hashing, finger table, stabilize, replication, RPC)
│   ├── factory.go           NewNode(port)
│   ├── addr.go              local-address / port→addr helpers
│   ├── basic_test.go        TestBasic (in-process)
│   └── advance_test.go      TestForceQuit, TestQuitAndStabilize (in-process)
├── network/
│   └── pool.go              RPC connection pool over net/rpc
├── testutil/
│   └── helpers.go           shared test constants, colored output, pass/fail metrics
├── test/
│   └── integration/         Docker Compose based integration tests
│       ├── cluster.go       cluster harness, fault injection, workload generation
│       └── cluster_test.go  TestMain bootstrap + TestReadWrite
├── deploy/
│   ├── Dockerfile           multi-stage build (golang:1.18-alpine → alpine:3.17 + iproute2)
│   └── docker-compose.yml   3-node cluster with NET_ADMIN for fault injection
└── doc/                     English documentation (setup, tutorial, Go reference)
```

## Build and Run

Build the binary from the project root:

```bash
go build -o dht .
```

Run a single node and start the ring with `Create`:

```bash
./dht -port 20000
```

Run another node that joins an existing one:

```bash
./dht -port 20001 -join 127.0.0.1:20000
```

### CLI flags

| Flag | Default | Description |
|---|---|---|
| `-port` | `20000` | Port the node listens on for RPC. |
| `-addr` | `127.0.0.1` | Address advertised to other nodes (use the container/host name in a cluster). |
| `-join` | _(empty)_ | Address of an existing node to join. If empty, the node creates a new ring. |
| `-cmd-port` | `0` | Port for a line-based TCP command server (`0` disables it). Used by the integration tests. |

### Interactive commands

Once running, a node reads commands from standard input (and, if enabled, from
the `-cmd-port` TCP server). One command per line:

```
put <key> <value>     # store a pair, prints "true" / "false"
get <key>             # look up a key, prints the value or "false"
delete <key>          # remove a key, prints "true" / "false"
quit                  # gracefully leave the ring and exit
```

## Testing

The project has **two independent test layers**.

### 1. In-process Go tests (`node/`)

These spawn many nodes inside a single process on `127.0.0.1` and drive the
`DhtNode` API directly. They require no external dependencies.

```bash
go test ./node/...
```

| Test | What it does |
|---|---|
| `TestBasic` | 5 rounds: nodes join, then `put`/`get`/`delete`, then nodes quit, repeated. |
| `TestForceQuit` | Nodes join and load data, then repeatedly **force-quit** without graceful handoff. |
| `TestQuitAndStabilize` | Nodes quit one by one while data is queried, exercising stabilization. |

Sizing constants and the maximum allowed failure rates live in
[`testutil/helpers.go`](testutil/helpers.go). These tests open a large number of
sockets; if you hit `Too many open files`, see
[Environment Setup](doc/env-setup.md).

> Each node writes its runtime log to `dht-test.log` in the working directory.

### 2. Docker Compose integration tests (`test/integration/`)

These exercise a **real, containerized cluster** and inject network faults. They
require Docker with the Compose plugin (`docker compose`).

```bash
go test ./test/integration/...
```

`TestMain` builds the `dht-cluster` image from [`deploy/Dockerfile`](deploy/Dockerfile),
starts the 3-node cluster defined in
[`deploy/docker-compose.yml`](deploy/docker-compose.yml), runs the tests, and
tears the cluster down. Commands are sent to each node over its `-cmd-port`
(host ports `21001`–`21003`).

`TestReadWrite` runs the following subtests:

| Subtest | Fault injected | Expectation |
|---|---|---|
| `healthy` | none | values put on `node1` are readable from `node2`. |
| `delay_200ms` | `tc netem` 200 ms delay on `node2` | reads still succeed (with measurable latency). |
| `loss_50pct` | 50% packet loss on `node2` | reads may retry but should not corrupt data. |
| `kill_recovery` | `docker kill node2` | data is still readable from `node3` via replication. |

The cluster harness (`cluster.go`) also exposes pause/unpause, network
partition (`ip link`), log inspection and a workload generator for writing
additional scenarios. Fault injection relies on `iproute2` (installed in the
image) and the `NET_ADMIN` capability (granted in the compose file).

## Documentation

- [Environment Setup](doc/env-setup.md) — install Go, configure the toolchain, and raise resource limits.
- [Tutorial](doc/tutorial.md) — learning resources for Go, DHT protocols, and debugging tips.
- [Go Language Reference](doc/Go.md) — a condensed reference of Go syntax and semantics.
