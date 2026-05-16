# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

`trans-db` is a distributed transactional database built from scratch in C++20. It speaks the PostgreSQL wire protocol so any `psql` client connects directly. Internally it uses MVCC + strict 2PL for serializable isolation, Raft for per-shard replication, and two-phase commit (2PC) for cross-shard transactions. Storage is RocksDB.

## Build

Requires CMake 3.20+, vcpkg, and the vcpkg toolchain at `C:/tools/vcpkg`.

```bash
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=C:/tools/vcpkg/scripts/buildsystems/vcpkg.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

## Run

```bash
./build/Release/trans_db_server --port 5433 --shards 3 --data-dir ./data
psql -h localhost -p 5433   # connect with any psql client
```

## Tests

```bash
ctest --test-dir build -C Release --output-on-failure
```

Seven suites (75 tests total): `mvcc_store`, `txn`, `raft`, `e2e`, `parser`, `pgwire`, `fault`. Tests create temporary directories and clean up after themselves.

## Benchmark (TPC-C)

```bash
./build/Release/bench_tpcc --shards 3 --threads 4 --duration 30 --warmup 5 --warehouses 2
```

## Docker

```bash
docker-compose -f docker/docker-compose.yml up --build
```

## Architecture

The stack has six layers, bottom to top:

### 1. Storage (`src/storage/`)
`MVCCStore` wraps RocksDB. Every write is versioned by timestamp so reads see a consistent snapshot. `KeyEncoding` serializes `(table_id, primary_key_value)` into RocksDB keys. `MVCCIterator` does range scans filtered to a snapshot timestamp.

### 2. Transaction (`src/txn/`)
`TxnManager` owns the transaction lifecycle: begin → read/write → prepare → commit/abort. `LockManager` enforces strict 2PL (locks held until commit). `WAL` logs mutations before they reach RocksDB. A `Transaction` object carries the read-set, write-set, and held locks.

### 3. Raft (`src/raft/`)
Each shard runs three Raft replicas. `RaftNode` handles leader election and log replication. `RaftLog` persists entries. `RaftTransport` is in-process gRPC. `RaftStateMachine` applies committed entries by calling `TxnManager`.

### 4. Shard (`src/shard/`)
`ShardServer` exposes a gRPC `ShardService` (defined in `proto/shard.proto`) with four RPCs: `Execute`, `Prepare`, `Commit`, `Abort`. Internally it owns a `ReplicaStack[3]` — each replica is its own `Store + WAL + LockMgr + TxnMgr + Raft` instance.

### 5. Coordinator (`src/coordinator/`)
`Coordinator` routes transactions to shards via FNV hash on the primary key and drives 2PC. `Parser` is a hand-written recursive-descent SQL parser (CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, BEGIN/COMMIT/ROLLBACK). `Executor` translates parsed statements into shard calls and serializes/deserializes rows. `Catalog` holds in-memory table metadata. `TimestampOracle` issues monotonically increasing snapshot timestamps.

### 6. PostgreSQL Wire Protocol (`src/pgwire/`)
`PgWireServer` listens on TCP and speaks the PostgreSQL v3 wire protocol. `PgSession` handles one client connection, supporting both simple and extended query protocols. This is what makes `psql` and any Postgres-compatible driver work without modification.

### Data Flow

```
psql client
  → PgWireServer / PgSession (Postgres wire protocol)
  → Executor + Coordinator
  → ShardService gRPC (Execute → Prepare → Commit/Abort)
  → ReplicaStack leader → Raft log replication
  → RaftStateMachine → TxnManager → MVCCStore (RocksDB)
```

### Key Design Decisions
- **Serializable isolation**: MVCC snapshots for reads + strict 2PL for writes + read-set validation at prepare time.
- **2PC is coordinator-driven**: The coordinator is not replicated; a coordinator crash mid-2PC requires manual recovery or timeout-based abort.
- **Raft transport is in-process**: `RaftTransport` uses in-process calls, so all replicas run in the same process. True network-separated replicas would require a real gRPC transport.
- **SQL parser is hand-written**: No parser generator (ANTLR, Bison) — the recursive-descent parser in `parser.cpp` covers the supported subset.
- **Windows-specific**: Uses `NOMINMAX` and `WIN32_LEAN_AND_MEAN`; path conventions are Windows-style in CMake config.

## LLM Efficiency Rules
- Start with PROJECT_STATE.md, CLAIMS_MATRIX.md, and NEXT_TASK.md before reading source files.
- Avoid full-repo scans unless explicitly requested; use targeted reads only.
- Treat README/CLAUDE claims as unverified until tied to code/tests/bench evidence.
- Use small diffs and keep documentation concise; mark uncertainty as TODO/VERIFY.
- After meaningful work, append AUDIT_LOG.md and refresh NEXT_TASK.md.

