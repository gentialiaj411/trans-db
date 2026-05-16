# trans-db

A distributed transactional database built from scratch in C++20. It provides serializable transactions across sharded data using MVCC, strict 2PL, two-phase commit, and Raft replication. Clients connect through PostgreSQL wire protocol v3, so `psql` works directly.

## Features
- Serializable isolation via MVCC + strict 2PL + read-set validation
- Distributed transactions with coordinator-driven 2PC across hash-sharded keys
- Raft replication (3 replicas per shard) for fault tolerance
- PostgreSQL wire protocol v3 (simple + extended query support)
- SQL support: `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `BEGIN/COMMIT/ROLLBACK`
- Crash/fault test coverage including restart scenarios, contention, and large payloads
- TPC-C-inspired benchmark harness with throughput and latency metrics

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
                        |  SQL Parser/Exec    |
                        |  2PC + Routing      |
                        +----+-----------+----+
                             |           |
                    gRPC/Raft Txn RPCs   |
                             |           |
      +----------------------+--+     +--+----------------------+
      |       Shard 0           | ... |        Shard N-1         |
      | (3 Raft replicas total) |     | (3 Raft replicas total)  |
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
./build/Release/trans_db_server --port 5433 --shards 3 --data-dir ./data
```

Connect:
```bash
psql -h localhost -p 5433
```

Docker:
```bash
docker-compose -f docker/docker-compose.yml up --build
psql -h localhost -p 5432
```

## TPC-C Benchmark (Simplified)
Run (multi-thread):
```bash
./build/Release/bench_tpcc --shards 3 --threads 4 --duration 30 --warmup 5 --warehouses 2
```

Run (single-thread baseline):
```bash
./build/Release/bench_tpcc --shards 3 --threads 1 --duration 30 --warmup 5 --warehouses 2
```

Benchmarked on **Intel Core Ultra 9 275HX**, Windows 11, Release build.

4-thread result (`--shards 3 --threads 4 --duration 30 --warmup 5 --warehouses 2`):

| Metric | Value |
|---|---:|
| Throughput | 0.93 txn/sec |
| Latency p50 | 63,969.90 us |
| Latency p99 | 5,226,064.60 us |
| Abort rate | 30.95% |
| Committed | 29 |
| Aborted | 13 |
| New-Order | 16 |
| Payment | 13 |

1-thread result (`--shards 3 --threads 1 --duration 30 --warmup 5 --warehouses 2`):

| Metric | Value |
|---|---:|
| Throughput | 37.20 txn/sec |
| Latency p50 | 31,253.80 us |
| Latency p99 | 32,746.50 us |
| Abort rate | 0.00% |
| Committed | 1116 |
| Aborted | 0 |
| New-Order | 681 |
| Payment | 435 |

## Tests
`ctest --test-dir build -C Release --output-on-failure`

Current status: **75 tests total** (67 existing + 8 fault tests), all passing in Release CI run.

| Suite | Tests |
|---|---:|
| mvcc_store | 8 |
| txn | 18 |
| raft | 10 |
| e2e | 11 |
| parser | 12 |
| pgwire | 8 |
| fault | 8 |

## Project Structure
```text
src/
  storage/      MVCC store + iterators + key encoding
  txn/          lock manager, WAL, transaction manager
  raft/         raft log/node/transport/state machine
  shard/        shard gRPC service + 3-replica raft stack
  coordinator/  catalog, parser, executor, coordinator (2PC)
  pgwire/       PostgreSQL wire protocol server
bench/
  tpcc.*        schema/data loader
  benchmark.*   workload runner + metrics
  main_bench.cpp
proto/
  shard.proto
test/
  test_*.cpp

docker/
  Dockerfile
  docker-compose.yml
```
