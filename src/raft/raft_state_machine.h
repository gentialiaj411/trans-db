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
#include <vector>

namespace txndb {

struct PrepareResult {
  bool success{false};
  std::string error;
};

class RaftStateMachine {
public:
  struct PrepareBatchWrite {
    bool is_delete{false};
    uint32_t table_id{0};
    std::string key;
    std::string value;
  };

  explicit RaftStateMachine(TxnManager* txn_mgr);

  static std::string PackTxnPayload(std::string_view wal_payload, uint64_t raft_txn_id);
  static std::string PackAbortPayload(uint64_t raft_txn_id);
  static std::string PackPrepareBatchPayload(uint64_t raft_txn_id, uint64_t snapshot_ts,
                                             uint64_t commit_ts,
                                             const std::vector<PrepareBatchWrite>& writes);

  void Apply(const RaftLogEntry& entry);
  std::function<void(const RaftLogEntry& entry)> WrapApply();

  uint64_t GetLocalTxnId(uint64_t raft_txn_id) const;
  std::optional<PrepareResult> TakePrepareResult(uint64_t raft_txn_id);
  void RegisterLocalTxn(uint64_t raft_txn_id, uint64_t local_txn_id);
  void ForgetTxn(uint64_t raft_txn_id);

private:
  static bool UnpackPayload(std::string_view full_payload, std::string* wal_payload,
                            uint64_t* raft_txn_id);
  static bool UnpackPrepareBatchPayload(std::string_view payload, uint64_t* raft_txn_id,
                                        uint64_t* snapshot_ts, uint64_t* commit_ts,
                                        std::vector<PrepareBatchWrite>* writes);

  TxnManager* txn_mgr_{nullptr};
  mutable std::mutex mu_;
  std::unordered_map<uint64_t, uint64_t> txn_id_map_;
  std::unordered_map<uint64_t, PrepareResult> prepare_results_;
};

}  // namespace txndb
