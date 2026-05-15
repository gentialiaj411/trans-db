# trans-db

Sharded distributed OLTP database (C++20) per [`distributed-db-spec.md`](../../distributed-db-spec.md) alongside this repo under `PROJECTS`.

## Current status

**Phase 1 (storage)** and **Phase 2 (txn)** are implemented: MVCC layer; `LockManager`; append-only **WAL** with CRC replay; `TxnManager` (`Begin`/`Read`/`Write`/`Delete`, `Prepare`/`Commit`/`Abort`, `CommitSingleShard`, `Recover`). Tests: `test/test_mvcc_store.cpp`, `test/test_txn.cpp`.

## Build (Windows + vcpkg)

```powershell
# Set VCPKG_ROOT to your installation (example: C:\tools\vcpkg)
# One-time: install vcpkg and integrate (see https://github.com/microsoft/vcpkg)
$env:VCPKG_ROOT = "C:\tools\vcpkg"
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT\scripts\buildsystems\vcpkg.cmake"
cmake --build build --config Release
ctest --test-dir build -C Release
```

Dependencies are listed in `vcpkg.json` (`rocksdb`, `gtest`).

## Next steps

Phase 3: Raft consensus (`src/raft/`, protos, gRPC — see spec).
