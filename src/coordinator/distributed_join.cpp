#include "coordinator/distributed_join.h"

#include "coordinator/row_codec.h"

#include <algorithm>
#include <cstdlib>
#include <future>
#include <unordered_map>

namespace txndb {

namespace {

constexpr size_t kBroadcastRowThreshold = 128;

using RowMap = std::unordered_map<std::string, std::string>;

}  // namespace

DistributedJoinPlanner::PhysicalOp ParseJoinPhysicalOpFromEnv() {
  const char* v = std::getenv("TRANS_DB_JOIN_PHYSICAL");
  if (v == nullptr) {
    return DistributedJoinPlanner::PhysicalOp::Auto;
  }
  if (std::string_view(v) == "broadcast") {
    return DistributedJoinPlanner::PhysicalOp::Broadcast;
  }
  if (std::string_view(v) == "shuffle") {
    return DistributedJoinPlanner::PhysicalOp::Shuffle;
  }
  return DistributedJoinPlanner::PhysicalOp::Auto;
}

DistributedJoinPlanner::DistributedJoinPlanner(Coordinator* coordinator, Catalog* catalog,
                                               uint32_t num_shards)
    : coordinator_(coordinator), catalog_(catalog), num_shards_(num_shards) {}

DistributedJoinPlanner::PhysicalOp DistributedJoinPlanner::ResolveOp(PhysicalOp requested,
                                                                     size_t left_rows,
                                                                     size_t right_rows) {
  if (requested == PhysicalOp::Broadcast) {
    return PhysicalOp::Broadcast;
  }
  if (requested == PhysicalOp::Shuffle) {
    return PhysicalOp::Shuffle;
  }
  const size_t smaller = std::min(left_rows, right_rows);
  return smaller <= kBroadcastRowThreshold ? PhysicalOp::Broadcast : PhysicalOp::Shuffle;
}

Status DistributedJoinPlanner::ScanTable(
    uint64_t txn_id, const TableDef& table,
    std::vector<std::pair<std::string, std::string>>* rows_out) {
  return coordinator_->Scan(txn_id, table.table_id, "", "", true, rows_out);
}

Status DistributedJoinPlanner::ExecuteInnerJoin(
    const SelectStmt& stmt, const TableDef& left_table, const TableDef& right_table,
    uint64_t txn_id, PhysicalOp op, std::vector<RowMap>* rows_out) {
  if (!stmt.join.has_value()) {
    return Status::Error(StatusCode::InvalidArgument, "missing join clause");
  }

  std::vector<std::pair<std::string, std::string>> left_rows;
  std::vector<std::pair<std::string, std::string>> right_rows;
  Status ls = ScanTable(txn_id, left_table, &left_rows);
  if (!ls.ok()) {
    return ls;
  }
  Status rs = ScanTable(txn_id, right_table, &right_rows);
  if (!rs.ok()) {
    return rs;
  }

  const PhysicalOp chosen = ResolveOp(op, left_rows.size(), right_rows.size());
  if (chosen == PhysicalOp::Broadcast) {
    if (left_rows.size() <= right_rows.size()) {
      return RunBroadcastJoin(txn_id, left_table, right_table, stmt.join->left_column,
                              stmt.join->right_column, left_rows, right_rows, rows_out);
    }
    return RunBroadcastJoin(txn_id, right_table, left_table, stmt.join->right_column,
                            stmt.join->left_column, right_rows, left_rows, rows_out);
  }
  return RunShuffleJoin(txn_id, left_table, right_table, stmt.join->left_column,
                        stmt.join->right_column, left_rows, right_rows, rows_out);
}

Status DistributedJoinPlanner::RunBroadcastJoin(
    uint64_t txn_id, const TableDef& build_table, const TableDef& probe_table,
    std::string_view build_join_col, std::string_view probe_join_col,
    const std::vector<std::pair<std::string, std::string>>& build_rows,
    const std::vector<std::pair<std::string, std::string>>& probe_rows, std::vector<RowMap>* rows_out) {
  (void)probe_rows;
  rows_out->clear();

  std::vector<DistributedJoinResponse> responses;
  responses.resize(num_shards_);
  std::vector<std::future<Status>> futs;
  futs.reserve(num_shards_);

  for (uint32_t sid = 0; sid < num_shards_; ++sid) {
    futs.push_back(std::async(std::launch::async, [&, sid]() {
      DistributedJoinRequest req;
      req.set_raft_txn_id(txn_id);
      req.set_op(JOIN_OP_BROADCAST);
      req.set_build_table_id(build_table.table_id);
      req.set_probe_table_id(probe_table.table_id);
      req.set_build_join_col(std::string(build_join_col));
      req.set_probe_join_col(std::string(probe_join_col));
      req.set_build_table_name(build_table.name);
      req.set_probe_table_name(probe_table.name);
      for (const auto& col : build_table.columns) {
        req.add_build_column_names(col.name);
      }
      for (const auto& col : probe_table.columns) {
        req.add_probe_column_names(col.name);
      }
      for (const auto& kv : build_rows) {
        auto* payload = req.add_broadcast_build_rows();
        payload->set_row_bytes(kv.second);
        payload->set_join_key(
            CellValueForJoinColumn(build_table, DeserializeStoredRow(build_table, kv.second),
                                   build_join_col));
      }
      return coordinator_->DistributedJoinOnShard(sid, req, &responses[sid]);
    }));
  }

  for (auto& fu : futs) {
    const Status st = fu.get();
    if (!st.ok()) {
      return st;
    }
  }
  return MergeShardResponses(build_table, probe_table, responses, rows_out);
}

Status DistributedJoinPlanner::RunShuffleJoin(
    uint64_t txn_id, const TableDef& left_table, const TableDef& right_table,
    std::string_view left_join_col, std::string_view right_join_col,
    const std::vector<std::pair<std::string, std::string>>& left_rows,
    const std::vector<std::pair<std::string, std::string>>& right_rows, std::vector<RowMap>* rows_out) {
  rows_out->clear();

  std::vector<std::vector<JoinRowPayload>> left_by_shard(num_shards_);
  std::vector<std::vector<JoinRowPayload>> right_by_shard(num_shards_);

  for (const auto& kv : left_rows) {
    const auto cells = DeserializeStoredRow(left_table, kv.second);
    JoinRowPayload payload;
    payload.set_row_bytes(kv.second);
    payload.set_join_key(CellValueForJoinColumn(left_table, cells, left_join_col));
    left_by_shard[HashJoinKey(payload.join_key(), num_shards_)].push_back(std::move(payload));
  }
  for (const auto& kv : right_rows) {
    const auto cells = DeserializeStoredRow(right_table, kv.second);
    JoinRowPayload payload;
    payload.set_row_bytes(kv.second);
    payload.set_join_key(CellValueForJoinColumn(right_table, cells, right_join_col));
    right_by_shard[HashJoinKey(payload.join_key(), num_shards_)].push_back(std::move(payload));
  }

  std::vector<DistributedJoinResponse> responses(num_shards_);
  std::vector<std::future<Status>> futs;
  futs.reserve(num_shards_);

  for (uint32_t sid = 0; sid < num_shards_; ++sid) {
    futs.push_back(std::async(std::launch::async, [&, sid]() {
      DistributedJoinRequest req;
      req.set_raft_txn_id(txn_id);
      req.set_op(JOIN_OP_SHUFFLE);
      req.set_build_table_id(left_table.table_id);
      req.set_probe_table_id(right_table.table_id);
      req.set_build_join_col(std::string(left_join_col));
      req.set_probe_join_col(std::string(right_join_col));
      req.set_build_table_name(left_table.name);
      req.set_probe_table_name(right_table.name);
      for (const auto& col : left_table.columns) {
        req.add_build_column_names(col.name);
      }
      for (const auto& col : right_table.columns) {
        req.add_probe_column_names(col.name);
      }
      for (auto& row : left_by_shard[sid]) {
        *req.add_shuffle_build_rows() = std::move(row);
      }
      for (auto& row : right_by_shard[sid]) {
        *req.add_shuffle_probe_rows() = std::move(row);
      }
      return coordinator_->DistributedJoinOnShard(sid, req, &responses[sid]);
    }));
  }

  for (auto& fu : futs) {
    const Status st = fu.get();
    if (!st.ok()) {
      return st;
    }
  }
  return MergeShardResponses(left_table, right_table, responses, rows_out);
}

Status DistributedJoinPlanner::MergeShardResponses(
    const TableDef& left_table, const TableDef& right_table,
    const std::vector<DistributedJoinResponse>& responses, std::vector<RowMap>* rows_out) {
  rows_out->clear();
  for (const auto& resp : responses) {
    if (!resp.ok()) {
      return Status::Error(static_cast<StatusCode>(resp.error_code()), resp.error_message());
    }
    for (const JoinOutputRow& out_row : resp.rows()) {
      RowMap row;
      for (const JoinOutputColumn& col : out_row.columns()) {
        row[col.name()] = col.value();
      }
      rows_out->push_back(std::move(row));
    }
  }
  (void)left_table;
  (void)right_table;
  return Status::OK();
}

}  // namespace txndb
