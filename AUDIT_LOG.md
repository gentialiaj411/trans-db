# AUDIT_LOG.md

## 2026-05-16 — Scaffold
- LLM scaffold created (`AGENTS.md`, `PROJECT_STATE.md`, `CLAIMS_MATRIX.md`, `AUDIT_LOG.md`, `NEXT_TASK.md`; refreshed `.claudeignore`).
- Initial evidence source limited to top-level structure plus `README.md` and `CLAUDE.md`.
- Next priorities: verify README/resume-style claims against targeted tests and benchmark harness evidence.

## 2026-05-16 — Narrow Evidence Audit

**Method:** Targeted reads of all layer headers + coordinator.cpp, raft_transport.cpp, txn_manager.cpp (Recover), bench/benchmark.cpp; grep over test files for test case names and specific anomaly coverage. No full-repo scan. No build executed.

### Verified Claims

1. **PostgreSQL wire protocol** — `PgWireServer`/`PgSession` implement startup, simple query, and full extended query protocol (Parse/Bind/Describe/Execute/Sync/Close). `test_pgwire.cpp` has 7 tests. No SSL; auth is trivial (AuthOk only). Claim is real but modest.

2. **SQL parser/executor** — Real recursive-descent parser. Covers CREATE TABLE, INSERT, SELECT, UPDATE (with arithmetic SET), DELETE, BEGIN/COMMIT/ROLLBACK, AND-only WHERE. No JOINs, no aggregates, no ORDER BY/LIMIT, no OR predicates. Claim accurate if described as "limited SQL subset."

3. **MVCC over RocksDB** — `mvcc_store.h` directly wraps `rocksdb::DB`. Versioned Get/Put/Delete/Scan by snapshot timestamp confirmed at header level. `ReplayPut` path for WAL recovery confirmed in `txn_manager.cpp`. Claim fully substantiated.

4. **WAL durability + crash recovery** — `WAL` has Append/Sync/AppendSync/Replay. `TxnManager::Recover()` replays WAL and re-applies committed data. Unit tests cover append/replay/truncation/serialization. Integration-level `FaultTest.CommittedDataSurvivesRestart` and `FaultTest.AbortedDataNotVisibleAfterRestart` exist. **Caveat:** `WAL` uses `std::ofstream`; whether `Sync()` calls OS-level fsync is unverified from header alone.

5. **Cross-shard 2PC** — `Coordinator::TwoPhaseCommit` does real parallel Prepare→Commit/Abort fan-out. Single-shard path optimized. `FaultTest.MultiShardCommitAtomicity` tests this. `Scan` fans out to all shards in parallel and merges sorted. Claim is substantiated.

6. **Strict 2PL locking** — `LockManager` with FIFO wait queue, `Acquire`/`Release`/`ReleaseAll`. `Transaction` holds read_set + write_set + locked_keys. Prepare calls `ValidateReadSet` (OCC-style read validation layered on top). Tests cover basic lock conflict and read-set validation, but no named serializable anomaly tests (write-skew, phantom reads).

### Risky / Weakened Claims

7. **"Raft-style replication"** — **Critical gap found:** `RaftLog` stores entries in `std::vector<RaftLogEntry>` — purely in-memory. `current_term_` and `voted_for_` in `RaftNode` are also in-memory (no disk persistence). **Raft state does not survive process restart.** Transport is `InProcessTransport` (same-process function calls, not real network). Raft tests exercise correctness of the algorithm but not durability. This claim must be weakened in README/resume: "in-process Raft replication; log and term state are volatile."

8. **"TPC-C benchmark"** — Only `RunNewOrder` and `RunPayment` implemented. Missing: OrderStatus, Delivery, StockLevel (3 of 5 transaction types). Cannot claim TPC-C conformance or compare to industry TPC-C numbers. Should be described as "TPC-C-inspired micro-benchmark."

9. **"Serializable isolation"** — Code implements strict 2PL writes + OCC read-set validation at Prepare. Tests cover basic conflicts but not write-skew or phantom reads. Formal correctness not proven by tests.

10. **`FaultTest.SingleReplicaFailureNoDataLoss` is explicitly `GTEST_SKIP()`ped** — `ShardServer` does not expose a replica-level disconnect API.

### Top 5 Missing Tests / Benchmarks

1. **Write-skew / serializable anomaly test** — Demonstrate that concurrent transactions reading overlapping key sets and writing disjoint keys are correctly aborted (or prevented). Most important gap for the serializability claim.
2. **Phantom read test** — Show range scans are protected under concurrent inserts.
3. **WAL fsync verification** — Confirm `WAL::Sync()` calls OS-level flush, not just `std::ofstream::flush()`. A power-loss simulation test would complete the durability story.
4. **Coordinator crash recovery test** — Show what happens mid-2PC: currently unrecoverable; document or implement.
5. **Full TPC-C transaction mix** — Add OrderStatus, Delivery, StockLevel to benchmark for a conformant workload.

### Single Best Next Implementation Task

**Persist Raft log and term/votedFor to disk.** Currently the entire Raft state is in-memory, meaning the replication layer provides no durability across restarts and the "Raft replication" claim is misleading for fault tolerance purposes. Replacing `std::vector<RaftLogEntry>` with an append-only log file (or a RocksDB column family) and persisting `current_term_`/`voted_for_` before responding to RPCs would make the Raft claim real, complete the WAL+Raft durability story, and enable the currently-skipped `SingleReplicaFailureNoDataLoss` test.

## 2026-05-16 - Raft Persistence Patch

- Implemented on-disk Raft persistence in:
  - `src/raft/raft_log.h/.cpp`
  - `src/raft/raft_node.h/.cpp`
  - `test/test_raft.cpp` (3 new persistence tests)
- `RaftLog` now supports path-backed mode with:
  - Constructor overload `RaftLog(std::string path)`
  - Sequential load from `raft_log` on startup
  - Binary append on `Append` and follower `AppendEntries`
  - File rewrite on `TruncateFrom`
- `RaftNode` now supports optional persistence directory overload:
  - Constructor overload with `std::string raft_dir`
  - Metadata file `raft_meta` load at construction
  - `PersistMeta()` called at Raft safety mutation sites:
    - `BecomeFollower`
    - `BecomeCandidateLocked`
    - vote grant path in `HandleRequestVote`
- Added targeted tests:
  - `RaftPersistence.LogSurvivesRestart`
  - `RaftPersistence.MetaSurvivesRestart`
  - `RaftPersistence.VotedForSurvivesRestart`
- Verified prerequisite: `src/txn/wal.cpp` `WAL::Sync()` currently calls `std::ofstream::flush()` only (no explicit OS-level `fsync` / `FlushFileBuffers`).
- Validation status:
  - `TODO/VERIFY`: Could not run build/tests in this environment due to CMake dependency resolution failure for `RocksDB` after path flattening and build-dir regeneration.
