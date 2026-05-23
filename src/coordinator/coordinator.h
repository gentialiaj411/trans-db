#pragma once

#include "coordinator/coordinator_log.h"
#include "coordinator/timestamp.h"
#include "storage/status.h"

#include "shard.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace txndb {

struct CoordinatorTxn {
  uint64_t raft_txn_id{0};
  uint64_t snapshot_ts{0};
  std::unordered_set<uint32_t> participant_shards;
  bool aborted{false};
};

class Coordinator {
public:
  Coordinator(const std::unordered_map<uint32_t, std::string>& shard_addresses,
              uint32_t num_shards,
              std::string coordinator_log_path = {});
  ~Coordinator() = default;

  uint64_t Begin();

  Status Read(uint64_t txn_id, uint32_t table_id, std::string_view key, std::string* value);

  Status Write(uint64_t txn_id, uint32_t table_id, std::string_view key,
               std::string_view value);

  Status Delete(uint64_t txn_id, uint32_t table_id, std::string_view key);

  Status Scan(uint64_t txn_id, uint32_t table_id, std::string_view range_start_pk,
              std::string_view range_end_exclusive, bool range_end_open,
              std::vector<std::pair<std::string, std::string>>* rows_out);

  Status Commit(uint64_t txn_id);

  Status Abort(uint64_t txn_id);
  Status Recover();

private:
  static constexpr auto kGrpcTimeout = std::chrono::seconds(5);

  uint32_t RouteShard(std::string_view key) const;

  Status EnsureShardParticipant(CoordinatorTxn& txn, uint32_t shard_id);

  ShardService::Stub* GetStub(uint32_t shard_id);

  Status TwoPhaseCommit(uint64_t raft_txn_id, std::vector<uint32_t> participant_shards,
                        uint64_t commit_ts);

  Status SingleShardCommit(uint64_t raft_txn_id, uint32_t shard_id, uint64_t commit_ts);

  static void ApplyDeadline(grpc::ClientContext* ctx);
  Status AppendCoordinatorRecord(CoordinatorLogRecordType type, uint64_t txn_id,
                                 const std::vector<uint32_t>& shards = {});
  Status DriveCommit(uint64_t txn_id, const std::vector<uint32_t>& shards);
  Status DriveAbort(uint64_t txn_id, const std::vector<uint32_t>& shards);
  TxnStateCode QueryShardTxnState(uint32_t shard_id, uint64_t txn_id);

  uint32_t num_shards_{0};
  TimestampOracle ts_oracle_;

  std::mutex mu_;
  uint64_t next_txn_id_{1};
  std::unordered_map<uint64_t, std::shared_ptr<CoordinatorTxn>> txns_;

  std::unordered_map<uint32_t, std::shared_ptr<grpc::Channel>> channels_;
  std::unordered_map<uint32_t, std::unique_ptr<ShardService::Stub>> stubs_;
  std::unique_ptr<CoordinatorLog> coordinator_log_;
};

}  // namespace txndb
