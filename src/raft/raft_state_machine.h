#pragma once

#include "raft/raft_log.h"
#include "txn/txn_manager.h"

#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace txndb {

/// Result of applying a TXN_PREPARE entry (consume with TakePrepareResult).
struct PrepareResult {
  bool success{false};
  std::string error;
};

class RaftStateMachine {
public:
  explicit RaftStateMachine(TxnManager* txn_mgr);

  /// WAL serialization + raft_txn_id (8-byte LE suffix), except abort (use PackAbortPayload).
  static std::string PackTxnPayload(std::string_view wal_payload, uint64_t raft_txn_id);

  /// TXN_ABORT full payload — 8-byte LE raft_txn_id only.
  static std::string PackAbortPayload(uint64_t raft_txn_id);

  void Apply(const RaftLogEntry& entry);

  std::function<void(const RaftLogEntry& entry)> WrapApply();

  uint64_t GetLocalTxnId(uint64_t raft_txn_id) const;

  /// Returned once per raft_txn_id after prepare commits; erased when taken.
  std::optional<PrepareResult> TakePrepareResult(uint64_t raft_txn_id);

private:
  static bool UnpackPayload(std::string_view full_payload, std::string* wal_payload,
                           uint64_t* raft_txn_id);

  TxnManager* txn_mgr_{nullptr};
  mutable std::mutex mu_;
  std::unordered_map<uint64_t, uint64_t> txn_id_map_;
  std::unordered_map<uint64_t, PrepareResult> prepare_results_;
};

}  // namespace txndb
