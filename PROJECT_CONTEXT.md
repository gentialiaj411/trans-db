# PROJECT_CONTEXT

## 1. Purpose and Project Identity
- `trans-db` is a C++20 distributed transactional database.
- Public-facing intent: PostgreSQL wire protocol v3 compatibility, SQL subset support, sharded storage, serializable transactions, and Raft-based replication.
- Code evidence:
  - `src/main.cpp` starts one `ShardServer` per shard plus a `PgWireServer`.
  - `src/pgwire/pgwire_server.cpp` implements startup negotiation, simple query, and prepared-statement protocol handling.
  - `src/coordinator/coordinator.cpp` routes SQL work to shards and performs single-shard commit or 2PC.
  - `src/shard/shard_server.cpp` builds a 3-replica Raft stack per shard and exposes gRPC RPCs.
- Scope note:
  - The repo implements a SQL subset, not full PostgreSQL or full SQL. `TODO/VERIFY` any README wording that implies broader compatibility than the parser/executor actually supports.

## 2. High-Level Architecture and Data Flow
```text
psql / client
  -> PgWireServer (TCP, protocol v3)
    -> Parser + Executor
      -> Coordinator
        -> shard routing by FNV-1a(key) % num_shards
          -> ShardService gRPC
            -> leader RaftNode
              -> RaftStateMachine
                -> TxnManager
                  -> LockManager + WAL + MVCCStore (RocksDB)
```

- Main flow for a statement:
  1. `PgSession` receives a simple query or prepared-statement message.
  2. `Parser::Parse` converts SQL text into a `Statement` variant.
  3. `Executor::Execute` maps the statement to `Coordinator` calls.
  4. `Coordinator` opens/uses a transaction, routes keys to shards, and issues gRPC `Execute`, `Prepare`, `Commit`, `Abort`.
  5. Each `ShardServiceImpl` resolves the current leader Raft replica and applies the request through `TxnManager`.
  6. Raft replicates transaction log entries across the 3 replicas for the shard.
  7. `TxnManager` uses WAL + MVCC store to make committed data visible by timestamp.
- Data shape:
  - SQL row payloads are serialized by `Executor` into a custom row blob.
  - The blob is stored as the MVCC value for the primary key.
  - The key routed across shards is the row primary key string.
- Transaction shape:
  - Coordinator uses `raft_txn_id` as the distributed transaction identifier.
  - Shard-local `TxnManager` uses its own local `txn_id`.
  - `RaftStateMachine` maps `raft_txn_id -> local_txn_id` per shard.

## 3. Core Data Model
- Catalog model:
  - `Catalog` owns `TableDef` objects.
  - `TableDef` = `table_id`, `name`, `columns`, `primary_key`.
  - `ColumnDef` uses `ColumnType::{INT,BIGINT,FLOAT,VARCHAR}`.
- SQL AST / execution model:
  - `Statement` is a `std::variant` of `CreateTableStmt`, `InsertStmt`, `SelectStmt`, `UpdateStmt`, `DeleteStmt`, `BeginStmt`, `CommitStmt`, `RollbackStmt`.
  - `WhereClause` is an AND-only list of `CompareExpr`.
  - `UpdateStmt` supports literal assignment and `col = col +/- literal`.
- Row storage model:
  - `Executor::SerializeRow` stores rows as:
    - `uint16_t column_count`
    - for each column: `uint32_t len` + raw bytes
  - `Executor::DeserializeRow` reads the same format.
  - Primary key is stored separately as the MVCC key.
- MVCC key model:
  - `EncodeKey(table_id, primary_key, write_ts)` produces:
    - big-endian table id
    - raw primary key bytes
    - inverted timestamp (`UINT64_MAX - write_ts`)
  - This makes newer versions sort earlier in RocksDB key order.
- Transaction model:
  - `Transaction` tracks:
    - `txn_id`, `snapshot_ts`, `prepare_commit_ts`, `state`
    - `write_set` of `BufferedWrite`
    - `read_set` of `ReadEntry`
    - `locked_keys`
- WAL / Raft payload model:
  - WAL records are typed (`BEGIN`, `WRITE`, `DELETE`, `PREPARE`, `COMMIT`, `ABORT`) with a binary payload and CRC.
  - Raft log entries wrap WAL payloads plus `raft_txn_id`, or carry `TXN_PREPARE_BATCH` with full prepare batch data.

## 4. Repository Layout and Module Map
- `src/storage/`
  - `key_encoding.*`: MVCC key encoding/decoding helpers.
  - `mvcc_store.*`: RocksDB-backed versioned store and scan iterator.
  - `status.h`: lightweight status/error codes.
- `src/txn/`
  - `lock_manager.*`: mutex/condition-variable lock table.
  - `wal.*`: binary WAL append/replay/truncate.
  - `transaction.h`: in-memory transaction state.
  - `txn_manager.*`: read/write/prepare/commit/recover logic.
- `src/raft/`
  - `raft_log.*`: persistent log file abstraction.
  - `raft_node.*`: leader election, append entries, commit advancement, apply loop.
  - `raft_transport.*`: in-process RPC transport for tests/dev.
  - `raft_state_machine.*`: bridges Raft log entries to `TxnManager`.
- `src/shard/`
  - `shard_server.*`: gRPC service for shard execution plus a `ShardServer` wrapper.
- `src/coordinator/`
  - `timestamp.h`: atomic timestamp oracle.
  - `catalog.*`: table metadata.
  - `parser.*`: recursive-descent SQL parser.
  - `executor.*`: statement-to-API translation, row serialization, predicate shaping.
  - `coordinator.*`: shard routing, transaction lifecycle, 2PC.
- `src/pgwire/`
  - `pgwire_server.*`: TCP server and PostgreSQL wire protocol session state machine.
- `bench/`
  - `tpcc.*`: TPC-C schema/data loader and primary-key formatting.
  - `benchmark.*`: TPC-C-inspired workload runner and metrics.
- `proto/`
  - `shard.proto`: gRPC schema used between coordinator and shards.
- `test/`
  - `test_mvcc_store.cpp`, `test_txn.cpp`, `test_raft.cpp`, `test_e2e.cpp`, `test_parser.cpp`, `test_pgwire.cpp`, `test_fault.cpp`.

## 5. Module-by-Module API Map
- Storage
  - `MVCCStore::Open(path, out)`
  - `Put(table_id, key, write_ts, value)`
  - `Delete(table_id, key, write_ts)`
  - `Get(table_id, key, snapshot_ts, value*, value_ts*)`
  - `Scan(table_id, start, end_exclusive, snapshot_ts, open_end_exclusive=false)`
  - `AcquireLock` / `ReleaseLock`: currently stubs returning OK. `TODO/VERIFY` whether any caller depends on them; actual locking is in `LockManager`.
  - `ReplayPut`: used during WAL recovery.
  - `MVCCIterator::Valid/PrimaryKey/Value/ValueTimestamp/Next`
- Transaction layer
  - `LockManager::Acquire/Release/ReleaseAll`
  - `WAL::Open/Append/Sync/AppendSync/Replay/Truncate`
  - `WAL::Serialize*` and `Deserialize*` payload helpers
  - `TxnManager::Begin/Read/Scan/Write/Delete/Prepare/Commit/Abort/CommitSingleShard/Recover`
  - `TxnManager::ImportPreparedTxn`: used by Raft prepare-batch replay
  - `TxnManager::ValidateReadSet`: serializable validation hook
- Raft layer
  - `RaftLog::Append/Get/GetRange/LastIndex/TermAt/TruncateFrom/AppendEntries`
  - `RaftNode::Start/Stop/Propose/HandleAppendEntries/HandleRequestVote/WaitForCommit`
  - `RaftStateMachine::Apply/WrapApply`
  - `InProcessTransport::RegisterNode/DisconnectNode/ReconnectNode`
- Coordinator / SQL
  - `Catalog::CreateTable/GetTable/GetTableById/ListTables`
  - `Parser::Parse`
  - `Executor::Execute`
  - `Coordinator::Begin/Read/Write/Delete/Scan/Commit/Abort`
  - internal coordinator helpers: `RouteShard`, `EnsureShardParticipant`, `SingleShardCommit`, `TwoPhaseCommit`
- gRPC / wire
  - `ShardServiceImpl::Execute/Prepare/Commit/Abort`
  - `ShardServer::Start/Stop/Wait`
  - `PgWireServer::Start/Stop/ListenPort`
  - `PgSession::Run` plus protocol handlers: simple query, Parse/Bind/Describe/Execute/Sync/Close
- Benchmark / loader
  - `tpcc::LoadTPCCData`
  - `tpcc::FormatPK` / `ParsePK`
  - `tpcc::Benchmark::Run`

## 6. Implementation Details by Subsystem
### Storage
- `MVCCStore` wraps `rocksdb::DB` and stores each version under the MVCC-encoded key.
- `Get`:
  - seeks newest-first at the primary-key prefix
  - walks versions until it finds the newest write timestamp `<= snapshot_ts`
  - treats a one-byte `0x00` value as tombstone
- `Scan`:
  - `MVCCIterator` iterates visible versions for a key range at one snapshot.
  - It filters out hidden versions and tombstones.
- `AcquireLock` / `ReleaseLock` are placeholders. Actual write exclusion is enforced by `LockManager`.

### Transaction / WAL
- `LockManager`
  - keyed by `(table_id, key)` string
  - supports reentrant acquisition by the same txn
  - uses FIFO wait queues and condition variables
  - `ReleaseAll` frees all locks held by a txn
- `WAL`
  - binary record format: length prefix, LSN, txn id, type byte, payload length, payload, CRC32
  - replay ignores truncated/corrupt tail records
  - `AppendSync` = append + flush (`FlushFileBuffers` on Windows only)
  - `Truncate` rewinds file to empty and reopens append mode
- `TxnManager`
  - `Begin` creates ACTIVE txn and logs `BEGIN`
  - `Read`
    - returns from buffered write set first
    - otherwise reads snapshot from MVCC store
    - records observed version in `read_set`
  - `Scan`
    - snapshot-scans the store and overlays buffered writes
    - records observed versions for non-buffered rows
  - `Write` / `Delete`
    - acquire per-key lock
    - append WAL record
    - buffer write in memory
  - `Prepare`
    - validates the read set against the target commit timestamp
    - persists `PREPARE`
    - transitions txn to PREPARED
  - `Commit`
    - requires PREPARED
    - applies buffered writes to MVCC store at commit timestamp
    - logs `COMMIT`
    - releases locks
  - `Abort`
    - logs `ABORT`
    - releases locks
  - `Recover`
    - replays WAL
    - restores committed writes
    - reconstructs PREPARED txns and reacquires locks
    - sets `next_txn_id_` above the maximum seen txn id

### Raft
- `RaftLog`
  - file-backed append-only log with in-memory mirror
  - `AppendEntries` handles append, conflict truncation, and file rewrite
  - `TruncateFrom` rewrites the file from current in-memory state
  - Windows path flushes file buffers after writes; non-Windows code flushes streams only.
- `RaftNode`
  - maintains term, vote, role, leader id, commit index, last applied index
  - election timeout is randomized per node
  - `Start` launches a tick loop thread
  - leader path:
    - appends `NOOP` on becoming leader
    - sends periodic heartbeats / append entries
    - advances commit index on majority replication
  - follower path:
    - handles AppendEntries and RequestVote
    - steps down on higher term
  - `LoadMeta` / `PersistMeta` store current term and voted-for node in `raft_meta`
- `RaftStateMachine`
  - converts Raft log entries into local `TxnManager` actions
  - `PackTxnPayload` appends `raft_txn_id` to a WAL payload
  - `TXN_BEGIN` -> local `Begin`
  - `TXN_WRITE` / `TXN_DELETE` -> local mutation
  - `TXN_PREPARE` -> local `Prepare`
  - `TXN_COMMIT` -> local `Commit`
  - `TXN_ABORT` -> local `Abort`
  - `TXN_PREPARE_BATCH` -> import a prepared txn with buffered writes
- `InProcessTransport`
  - maps node ids to `RaftNode*`
  - simulates partitions by disconnecting source or destination ids

### Shard Service
- Each shard owns `num_replicas` independent `ReplicaStack`s:
  - `MVCCStore`
  - `WAL`
  - `LockManager`
  - `TxnManager`
  - `RaftStateMachine`
  - `RaftNode`
- `ShardServiceImpl` always executes through the current leader replica.
- `Execute`
  - `OP_BEGIN`: start local txn, register `raft_txn_id -> local_txn_id`
  - `OP_READ`: read through local txn manager
  - `OP_WRITE` / `OP_DELETE`: mutate buffered local txn
  - `OP_SCAN`: scan through local txn manager
- `Prepare`
  - local `TxnManager::Prepare`
  - extracts buffered writes
  - replicates a `TXN_PREPARE_BATCH` entry through Raft
  - waits for apply and prepare result
- `Commit`
  - replicates `TXN_COMMIT`
- `Abort`
  - if prepare was not yet replicated, abort locally
  - otherwise replicate `TXN_ABORT`

### Coordinator / SQL
- `Coordinator`
  - uses `TimestampOracle` for transaction begin and commit timestamps
  - routes shard by FNV-1a of the key
  - maintains distributed txn records in `txns_`
  - opens a shard participant with `OP_BEGIN` on first access
  - single-shard commit path:
    - `Prepare` then `Commit`
  - multi-shard commit path:
    - parallel `Prepare` fan-out
    - all-vote-commit check
    - parallel `Commit` fan-out
    - if any prepare fails, parallel `Abort`
  - `Scan`
    - fans out to all shards in parallel
    - merges and sorts rows by key
- `Parser`
  - recursive descent
  - supports:
    - `CREATE TABLE`
    - `INSERT INTO ... VALUES ...`
    - `SELECT ... FROM ... WHERE ...`
    - `UPDATE ... SET ... WHERE ...`
    - `DELETE FROM ... WHERE ...`
    - `BEGIN`
    - `COMMIT`
    - `ROLLBACK` / `ABORT`
  - WHERE grammar is AND-only
  - no JOINs, aggregates, ORDER BY, LIMIT, GROUP BY, subqueries, OR
- `Executor`
  - serializes rows into the storage blob
  - deserializes rows back into column strings
  - computes primary-key routing and supported key ranges
  - enforces table/column existence and primitive type comparisons
  - auto-commits DML when not already inside a transaction
  - `SELECT`:
    - point lookups for point predicates
    - range scans for bounded primary-key ranges
    - projections over explicit column lists or `*`
  - `UPDATE` / `DELETE`:
    - require a primary-key point predicate
    - evaluate non-PK WHERE filters against the fetched row
  - `CREATE TABLE` mutates only the catalog
- `PgWireServer`
  - handles startup negotiation and responds with `AuthenticationOk`, parameter status, and `ReadyForQuery`
  - supports simple query and prepared statement flows:
    - Parse, Bind, Describe, Execute, Sync, Close
  - command completion tags are PostgreSQL-like but this is not a full server implementation

### Benchmark / Loader
- `LoadTPCCData`
  - creates `warehouse`, `district`, `customer`
  - seeds them through coordinator writes
  - row formats are custom blobs, not relational wire-format rows
- `Benchmark`
  - runs two transaction types only:
    - `RunNewOrder`
    - `RunPayment`
  - uses multiple worker threads and latency sampling
  - workload mix defaults to 60% new-order / 40% payment
  - this is TPC-C-inspired, not a full TPC-C implementation

## 7. Concurrency / Performance Primitives
- `LockManager`
  - `std::mutex` + `std::condition_variable`
  - per-key FIFO wait queue
  - reentrant lock count per owner
- `TxnManager`
  - `std::mutex` protects txn map and txn state
  - `std::atomic<uint64_t>` allocates txn ids
- `WAL`
  - `std::mutex` serializes append/sync/replay/truncate
- `RaftNode`
  - background tick thread
  - `std::condition_variable` wakes on commits and stop events
  - leader append replication uses detached async send path
- `Coordinator`
  - parallel prepare/commit/abort/scan fan-out via `std::async`
- `PgWireServer`
  - accept loop plus one worker thread per client connection
- `Benchmark`
  - worker threads run until a wall-clock stop condition
  - latency vector protected by mutex

## 8. Determinism / Evidence Mechanics
- Deterministic core ordering:
  - MVCC keys sort by inverted timestamp, so latest version is found first.
  - FNV-1a routes a key to a stable shard id.
  - Raft log indices increase monotonically per leader.
  - `TimestampOracle` is atomic and monotonic within a process.
- Recovery / replay mechanics:
  - WAL replay ignores truncated tail records and replays only valid checksum-passing records.
  - Raft log and meta are file-backed and can be reloaded on startup.
  - `TxnManager::Recover` reconstructs state from WAL records.
- Evidence status from code/tests:
  - Verified by source inspection and unit/integration tests in `test/`.
  - Not executed in this turn. Any runtime claim should be treated as code-backed but not session-run evidence.
- Important limitations:
  - WAL durability is flush-based; explicit OS-level durable sync is only partial and platform-specific. `TODO/VERIFY`.
  - Coordinator crash recovery is not persisted. `TODO/VERIFY`.
  - `MVCCStore::AcquireLock` / `ReleaseLock` are no-op stubs. The real concurrency control is `LockManager`, not the store.

## 9. Test-by-Test Meaning Map
### `test_mvcc_store.cpp`
- `KeyEncoding.DecodeRoundTrip`: key encoding/decoding preserves table id, primary key bytes, and timestamp.
- `KeyEncoding.NewerWriteTimestampSortsBeforeOlder`: inverted timestamp ordering makes newer versions sort earlier.
- `KeyEncoding.InvertIsSelfInverse`: timestamp inversion is reversible.
- `MVCCStore.PutGetLatestVisibleVersion`: newest visible version is returned for a snapshot.
- `MVCCStore.SnapshotIsolationBetweenVersions`: snapshot timestamps isolate older reads from later writes.
- `MVCCStore.DeleteHidesOlderValues`: tombstones hide versions for snapshots after delete.
- `MVCCStore.ScanExclusiveEndAndMvccVisibility`: scan respects end bound and snapshot visibility.
- `MVCCStore.ScanOpenEndedUpperBounds`: open-ended scans traverse to the end of the keyspace.

### `test_txn.cpp`
- `LockManager.BasicLockUnlock`: simple acquire/release works.
- `LockManager.ConflictingLockTimesOut`: conflicting acquisition returns timeout.
- `LockManager.LockReleasedWakesWaiter`: releasing a lock wakes a blocked waiter.
- `LockManager.ReleaseAllFreesEverything`: `ReleaseAll` clears all locks for the txn.
- `LockManager.ReentrantLock`: same txn can reacquire the same lock.
- `WAL.AppendAndReplay`: WAL records append and replay in order.
- `WAL.SyncDurability`: appended record survives reopen in the normal file path.
- `WAL.TruncatedRecordIgnored`: tail truncation is ignored on replay.
- `WAL.PayloadSerialization`: payload serializers/deserializers round-trip.
- `TxnEnv.SimpleReadWrite`: a txn can write, read its own write, and commit.
- `TxnEnv.SnapshotIsolation`: later committed data does not affect an earlier snapshot.
- `TxnEnv.ReadSetValidationDetectsConflict`: `Prepare` aborts when a read version changed.
- `TxnEnv.CommitSingleShardHappyPath`: one-txn single-shard commit path works.
- `TxnEnv.AbortReleasesLocks`: abort releases locks.
- `TxnManager.WriteConflictAborts`: conflicting writes on same key fail.
- `TxnManager.RecoverCommittedTxn`: committed WAL state is recovered into MVCC data.
- `TxnManager.RecoverAbortedTxn`: aborted txn does not reappear after recovery.
- `TxnManager.RecoverPreparedTxn`: prepared txn is reconstructed and can later commit.
- `TxnEnv.SerializabilityWriteSkew`: read-set validation blocks a write-skew pattern here.
- `TxnEnv.PhantomReadSnapshotBehavior`: snapshot reads do not see later inserted key in this key-granular setup.

### `test_raft.cpp`
- `RaftCluster.LeaderElection`: exactly one leader emerges in a 3-node cluster.
- `RaftCluster.LeaderElectionAfterDisconnect`: leader loss triggers a new leader/term.
- `RaftCluster.ProposeAndCommit`: a BEGIN/WRITE/PREPARE/COMMIT sequence replicates and applies on all replicas.
- `RaftCluster.ProposeOnFollowerFails`: non-leader proposal is rejected.
- `RaftCluster.LogReplicationToFollowers`: follower logs match leader logs across multiple proposals.
- `RaftCluster.LeaderFailoverPreservesCommitted`: committed entries survive leader failover.
- `RaftCluster.DisconnectedNodeCatchesUp`: a partitioned follower catches up after reconnection.
- `RaftCluster.ConcurrentProposals`: concurrent leader proposals get distinct consecutive log indices.
- `RaftCluster.ElectionSafetyNoTwoLeaders`: no two leaders in the same term are observed during polling.
- `RaftCluster.FullTransactionViaRaft`: full transaction lifecycle applies through Raft to all replicas.
- `RaftPersistence.LogSurvivesRestart`: `RaftLog` reloads after restart.
- `RaftPersistence.MetaSurvivesRestart`: term persistence survives restart.
- `RaftPersistence.VotedForSurvivesRestart`: vote persistence survives restart.
- `RaftPersistence.TruncateFromSurvivesRestart`: truncation survives restart.
- `RaftPersistence.ConflictTruncationSurvivesRestart`: conflicting append entries rewrite survives restart.

### `test_e2e.cpp`
- `E2ETest.SingleShardWriteRead`: coordinator can write/read a key on one shard.
- `E2ETest.MultiShardTransaction`: one transaction can update keys on different shards.
- `E2ETest.SingleShardOptimization`: single-key transaction lands only on its owner shard.
- `E2ETest.MultiShardAtomicity`: failed 2PC leaves no partial commit visible.
- `E2ETest.AbortCleansUp`: abort removes in-flight writes.
- `E2ETest.ConcurrentTransactions`: concurrent txns can commit independently.
- `E2ETest.ReadYourOwnWrites`: read-your-own-writes holds inside a txn.
- `E2ETest.ConflictDetection`: conflicting writes cause commit failure.
- `E2ETest.MultipleShardReadsAndWrites`: a txn can mix reads and writes across shards.
- `E2ETest.TimestampOrdering`: later committed version becomes visible to later snapshots.
- `E2ETest.CoordinatorRangeScanFansOutShards`: coordinator scan fans out and merges ordered rows.

### `test_parser.cpp`
- `ParseCreateTable`: CREATE TABLE syntax and primary-key capture.
- `ParseInsert`: INSERT columns and literal parsing.
- `ParseSelectStar`: SELECT * with WHERE clause.
- `ParseSelectColumns`: explicit projection list.
- `ParseUpdate`: literal assignment parsing.
- `ParseUpdateArith`: arithmetic update parsing.
- `ParseDelete`: DELETE parsing.
- `ParseBeginCommitRollback`: transaction control statements parse.
- `ParseCaseInsensitive`: keywords are case-insensitive.
- `ParseStringEscape`: SQL string escaping with doubled quotes works.
- `ParseError`: malformed SQL reports parse error.
- `ParseMultiConditionWhere`: WHERE conditions are AND-combined.

### `test_pgwire.cpp`
- `ConnectAndStartup`: TCP connect and startup handshake succeed.
- `CreateTableAndInsert`: simple query path can create a table and insert a row.
- `SelectAfterInsert`: SELECT returns row description, data row, and command tag.
- `TransactionBlock`: BEGIN/COMMIT transaction block works through wire protocol.
- `Rollback`: rollback clears uncommitted writes.
- `UpdateAndRead`: UPDATE changes visible data.
- `DeleteAndRead`: DELETE hides the row.
- `ErrorHandling`: parse/runtime errors return error responses without breaking the session.

### `test_fault.cpp`
- `CommittedDataSurvivesRestart`: committed writes survive cluster restart.
- `AbortedDataNotVisibleAfterRestart`: aborted writes do not reappear after restart.
- `SingleReplicaFailureNoDataLoss`: skipped because replica-level kill/disconnect API is not exposed.
- `MultiShardCommitAtomicity`: multi-shard commit produces visible rows on all routing owners.
- `HighContentionWorkload`: system continues under write contention.
- `RapidTransactions`: many quick txns mostly persist and remain readable.
- `LargeValueStorage`: large payloads are storable and readable.
- `ReadOnlyTransactionNoLocks`: read-only txn does not block writers in this test shape.

## 10. Validation Commands and Scripts
- Build:
  - `cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=C:/tools/vcpkg/scripts/buildsystems/vcpkg.cmake -DCMAKE_BUILD_TYPE=Release`
  - `cmake --build build --config Release`
- Test:
  - `ctest --test-dir build -C Release --output-on-failure`
- Runtime:
  - `./build/Release/trans_db_server --port 5433 --shards 3 --data-dir ./data`
  - `psql -h localhost -p 5433`
- Benchmark:
  - `./build/Release/bench_tpcc --shards 3 --threads 4 --duration 30 --warmup 5 --warehouses 2`
- Docker:
  - `docker-compose -f docker/docker-compose.yml up --build`
- Evidence note:
  - README publishes benchmark numbers and a 75-test total, but this file should treat those as code-backed claims until a fresh run artifacts them in the current environment.

## 11. Current Evidence Status
- Strongly supported by code and tests:
  - MVCC key encoding and versioned reads.
  - Parser grammar shown in `src/coordinator/parser.cpp` and covered by parser tests.
  - Transaction manager read/write/prepare/commit/recover logic.
  - Raft leader election, replication, failover, and persistence tests in source.
  - PG wire protocol startup, simple query, prepared statements, and command completion logic.
  - Coordinator routing and 2PC logic.
- Partially supported / needs caution:
  - Serializable isolation is supported by strict locking plus read-set validation, but not formally proven. Write-skew coverage exists in tests; phantom coverage is only key-granularity in current tests. `TODO/VERIFY`.
  - WAL durability is flush-based, not full power-loss durability. `TODO/VERIFY`.
  - Raft persistence uses file rewrite/flush semantics; explicit cross-platform durable fsync semantics should be checked. `TODO/VERIFY`.
  - Coordinator crash recovery and in-doubt 2PC state are not persisted. `TODO/VERIFY`.
  - Benchmark claim is TPC-C-inspired, not a full TPC-C implementation.
- Not verified in this session:
  - No build or test command was run here.
  - README performance numbers are unverified in this turn.

## 12. Open Risks / Limitations
- SQL coverage is narrow:
  - no JOINs, aggregates, ORDER BY, LIMIT, GROUP BY, subqueries, OR predicates, RETURNING.
- Security/authentication:
  - PgWire startup uses `AuthenticationOk`; no real auth or TLS is implemented.
- Storage locking:
  - `MVCCStore::AcquireLock`/`ReleaseLock` are no-ops; correctness depends on `LockManager` and caller discipline.
- Durability:
  - flush-based file sync is weaker than explicit OS durability on all paths. `TODO/VERIFY`.
- Recovery:
  - coordinator state is ephemeral; mid-2PC crash recovery is not durable.
- Benchmark validity:
  - workload only implements New-Order and Payment, so external comparisons to canonical TPC-C are misleading.
- Fault coverage:
  - one replica-failure test is skipped because there is no replica-level kill/disconnect API on `ShardServer`.

## 13. Fast Orientation for a New LLM
1. Start at `src/main.cpp`: it constructs shard servers, the coordinator, and the PgWire server.
2. Read `src/coordinator/parser.cpp` and `src/coordinator/executor.cpp` to see supported SQL and how it maps to storage calls.
3. Read `src/coordinator/coordinator.cpp` to understand routing and 2PC.
4. Read `src/shard/shard_server.cpp`, `src/raft/raft_state_machine.cpp`, and `src/raft/raft_node.cpp` to see shard-side replication and transaction application.
5. Read `src/txn/txn_manager.cpp`, `src/txn/lock_manager.cpp`, `src/txn/wal.cpp`, and `src/storage/mvcc_store.cpp` for the correctness path.
6. Use the test files as the truth table for implemented behavior:
   - parser and PG wire for protocol surface
   - txn/raft/e2e/fault for correctness and recovery
   - benchmark for workload shape only

## 14. Practical Summary
- What the system is:
  - a sharded transactional DB with MVCC storage, lock-based writes, WAL recovery, Raft replication per shard, coordinator-driven 2PC, and PostgreSQL wire access.
- What is verified:
  - the code and tests show working transaction flow, Raft replication/persistence, protocol handling, and a limited SQL subset.
- What is only partially verified:
  - crash-durability semantics, full serializability proof, coordinator failure recovery, and benchmark claims.
- What to remember:
  - routing is key-based, storage is versioned, Raft is intra-shard, coordinator handles cross-shard coordination, and the SQL surface is intentionally small.
