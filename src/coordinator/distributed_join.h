#pragma once

#include "coordinator/catalog.h"
#include "coordinator/coordinator.h"
#include "coordinator/parser.h"
#include "shard.grpc.pb.h"
#include "storage/status.h"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace txndb {

class DistributedJoinPlanner {
public:
  enum class PhysicalOp { Auto, Broadcast, Shuffle };

  DistributedJoinPlanner(Coordinator* coordinator, Catalog* catalog, uint32_t num_shards);

  Status ExecuteInnerJoin(const SelectStmt& stmt, const TableDef& left_table,
                          const TableDef& right_table, uint64_t txn_id, PhysicalOp op,
                          std::vector<std::unordered_map<std::string, std::string>>* rows_out);

private:
  using RowMap = std::unordered_map<std::string, std::string>;

  Status ScanTable(uint64_t txn_id, const TableDef& table,
                 std::vector<std::pair<std::string, std::string>>* rows_out);

  Status RunBroadcastJoin(uint64_t txn_id, const TableDef& build_table, const TableDef& probe_table,
                          std::string_view build_join_col, std::string_view probe_join_col,
                          const std::vector<std::pair<std::string, std::string>>& build_rows,
                          const std::vector<std::pair<std::string, std::string>>& probe_rows,
                          std::vector<RowMap>* rows_out);

  Status RunShuffleJoin(uint64_t txn_id, const TableDef& left_table, const TableDef& right_table,
                        std::string_view left_join_col, std::string_view right_join_col,
                        const std::vector<std::pair<std::string, std::string>>& left_rows,
                        const std::vector<std::pair<std::string, std::string>>& right_rows,
                        std::vector<RowMap>* rows_out);

  Status MergeShardResponses(const TableDef& left_table, const TableDef& right_table,
                             const std::vector<DistributedJoinResponse>& responses,
                             std::vector<RowMap>* rows_out);

  static PhysicalOp ResolveOp(PhysicalOp requested, size_t left_rows, size_t right_rows);

  Coordinator* coordinator_{nullptr};
  Catalog* catalog_{nullptr};
  uint32_t num_shards_{1};
};

DistributedJoinPlanner::PhysicalOp ParseJoinPhysicalOpFromEnv();

}  // namespace txndb
