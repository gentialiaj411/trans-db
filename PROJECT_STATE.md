# PROJECT_STATE.md

## Summary
`trans-db` appears to be a C++20 distributed transactional database that exposes a PostgreSQL-compatible wire interface and claims serializable transactions across shards using MVCC, strict 2PL, Raft-style replication, and coordinator-driven 2PC. This summary is based on `README.md` and `CLAUDE.md` and is not a full code audit.

## Architecture Layers (Claimed)
- Client protocol layer: PostgreSQL wire protocol server/session (`src/pgwire/`).
- SQL + coordination layer: parser/executor/catalog/coordinator (`src/coordinator/`).
- Shard service layer: gRPC shard RPC boundary (`src/shard/`, `proto/shard.proto`).
- Replication layer: Raft node/log/state machine (`src/raft/`).
- Transaction layer: transaction manager, lock manager, WAL (`src/txn/`).
- Storage layer: MVCC store over RocksDB (`src/storage/`).

## Credible Current Claims
- Repository structure for the above layers exists (top-level directories present).
- Build/test/benchmark commands are documented in `README.md` and `CLAUDE.md`.
- Test suites and benchmark harness are documented, but results are not independently verified in this scaffold pass.

## Caveats / Limitations
- No full source/test audit was performed in this pass.
- Feature completeness and correctness remain `TODO/VERIFY`.
- Performance numbers in README are `TODO/VERIFY` against reproducible benchmark runs.
- Fault tolerance semantics (recovery behavior, coordinator failure handling) are `TODO/VERIFY`.

## Build / Test / Benchmark Commands (From Docs)
```bash
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=C:/tools/vcpkg/scripts/buildsystems/vcpkg.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
./build/Release/bench_tpcc --shards 3 --threads 4 --duration 30 --warmup 5 --warehouses 2
```

## Verify Before Trusting Claims
- Confirm parser grammar and executor coverage for claimed SQL subset.
- Confirm isolation behavior with txn/e2e/fault tests tied to serializability cases.
- Confirm WAL durability/restart behavior via targeted fault/recovery tests.
- Confirm Raft replication behavior and failure handling via raft/fault tests.
- Confirm cross-shard 2PC correctness under abort/timeouts.
- Confirm benchmark methodology and reproducibility of published metrics.
