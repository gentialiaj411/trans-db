#pragma once

#include "coordinator/catalog.h"

#include <string>
#include <string_view>
#include <vector>

namespace txndb {

std::vector<std::string> DeserializeStoredRow(const TableDef& table, std::string_view data);

std::string BaseColumnName(std::string_view qualified_name);

int ColumnIndexByName(const TableDef& table, std::string_view name);

std::string CellValueForJoinColumn(const TableDef& table, const std::vector<std::string>& cells,
                                   std::string_view join_column);

uint32_t HashJoinKey(std::string_view key, uint32_t num_shards);

}  // namespace txndb
