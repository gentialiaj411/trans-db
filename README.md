# trans-db

A distributed transactional database built from scratch in C++20. It exposes PostgreSQL wire protocol v3, shards storage across multiple shard servers, uses MVCC and WAL for transaction correctness, and uses Raft-backed replication within each shard. Clients can connect with `psql`, but the SQL surface is intentionally limited.

## Features
- Serializable isolation via MVCC + strict 2PL + read-set validation
- Coordinator-driven 2PC across hash-sharded keys
- Per-shard Raft replication with restart persistence for log and metadata
- PostgreSQL wire protocol v3 with simple and extended query support
- SQL support: `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `BEGIN`, `COMMIT`, `ROLLBACK`
- Query features: `JOIN`, `ORDER BY`, `LIMIT`, `COUNT`, `SUM`, `MIN`, `MAX`
- Crash and restart coverage for WAL, coordinator log, Raft persistence, and multi-shard fault scenarios
- TPC-C-style benchmark harness with all 5 transaction types

## Architecture
```text
                    +-----------------------------+
                    |        PostgreSQL Client    |
                    |      (psql / pgbench)       |
                    +--------------+--------------+
                                   |
                            PgWire v3 (TCP)
                                   |
                  +----------------v----------------+
                  |           PgWireServer          |
                  +----------------+----------------+
                                   |
                        +----------v----------+
                        |      Coordinator    |
                        | SQL Parser/Executor |
                        | 2PC + Recovery Log  |
                        +----+-----------+----+
                             |           |
                    gRPC/Raft Txn RPCs   |
                             |           |
      +----------------------+--+     +--+----------------------+
      |       Shard 0           | ... |        Shard N-1        |
      | (3 Raft replicas total) |     | (3 Raft replicas total) |
      +-----------+-------------+     +------------+-------------+
                  |                                |
      +-----------v-------------+      +-----------v-------------+
      | TxnManager + LockMgr    |      | TxnManager + LockMgr    |
      | WAL + RaftStateMachine  |      | WAL + RaftStateMachine  |
      +-----------+-------------+      +-----------+-------------+
                  |                                |
              +---v---+                        +---v---+
              | RocksDB|                        | RocksDB|
              |  MVCC  |                        |  MVCC  |
              +--------+                        +--------+
```

## Quick Start
```bash
git clone <repo>
cd trans-db
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=C:/tools/vcpkg/scripts/buildsystems/vcpkg.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

Run server:
```bash
./build/Release/trans_db_server --shards 3 --port 5432 --data-dir ./data
```

Connect:
```bash
psql -h localhost -p 5432
```

Docker:
```bash
docker-compose -f docker/docker-compose.yml up --build
psql -h localhost -p 5432
```

## TPC-C-Style Benchmark
`bench_tpcc` implements all 5 TPC-C-style transaction types:
- New-Order
- Payment
- Order-Status
- Delivery
- Stock-Level

Current artifact:
- `bench/results/tpcc_style_full_mix_7ab86ffe4ca1.json`

Artifact snapshot:
- commit: `7ab86ffe4ca1`
- date: `2026-05-23T23:11:24Z`
- topology: `3_shards_local`
- mix: `45/43/4/4/4`
- overall throughput (canonical gRPC 9-process, GC off): `115.954 txn/sec` — see `bench/results/tpcc_grpc.md` (superseded in-process / older headlines in that file’s historical section)
- overall p99 latency: `106947 us`
- Caveat: benchmark transaction outcomes use bounded retries per logical transaction (max 8 attempts with exponential backoff+jitter), so success/abort counts in the artifact are eventual outcomes after retry policy, not single-attempt outcomes.

## CLI Flags & Defaults
Server (`src/main.cpp`):

| Flag | Default | Notes |
|---|---|---|
| `--shards` | `3` | Number of shard servers; shard `i` listens on `0.0.0.0:(50051+i)`. |
| `--port` | `5432` | PgWire listen port for client connections. |
| `--data-dir` | `./data` | Base data directory for shard state. |
| `--help` / `-h` | n/a | Prints usage and exits. |

Benchmark (`bench/main_bench.cpp`):

| Flag | Default | Notes |
|---|---|---|
| `--shards` | `3` | Number of local shard servers; shard `i` listens on `127.0.0.1:(57051+i)`. |
| `--threads` | `4` | Worker threads issuing benchmark transactions. |
| `--duration` | `30` | Measured run duration in seconds. |
| `--warmup` | `5` | Warmup duration in seconds. |
| `--warehouses` | `2` | TPC-C-style dataset scale input. |
| `--districts` | `10` | Districts per warehouse. |
| `--customers` | `100` | Customers per district. |
| `--data-dir` | `./bench_data` | Benchmark-local storage root; coordinator log path is `data_dir/coordinator.log`. |
| `--mix` | `standard` | One of `standard`, `new-order-only`, `payment-only`, `order-status-only`, `delivery-only`, `stock-level-only`. |
| `--help` / `-h` | n/a | Prints usage and exits. |

## Tests
`ctest --test-dir build -C Release --output-on-failure`

Source-defined test count: 106 total.

| Suite | Tests |
|---|---:|
| coordinator_log | 3 |
| e2e | 16 |
| fault | 14 |
| mvcc_store | 8 |
| parser | 16 |
| pgwire | 8 |
| pgwire_phase4 | 3 |
| raft | 18 |
| txn | 20 |

## Project Structure
```text
src/
  storage/      MVCC store + iterators + key encoding
  txn/          lock manager, WAL, transaction manager
  raft/         raft log/node/transport/state machine
  shard/        shard gRPC service + 3-replica raft stack
  coordinator/  catalog, parser, executor, coordinator, coordinator_log
  pgwire/       PostgreSQL wire protocol server
bench/
  tpcc.*        schema/data loader
  benchmark.*   workload runner + metrics
  results/      benchmark artifacts
proto/
  shard.proto
test/
  test_*.cpp

docker/
  Dockerfile
  docker-compose.yml
```
