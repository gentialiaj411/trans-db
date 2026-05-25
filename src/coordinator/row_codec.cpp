#include "coordinator/row_codec.h"

namespace txndb {

namespace {

bool ReadU16LE(std::string_view* data, uint16_t* out) {
  if (data->size() < 2) {
    return false;
  }
  *out = static_cast<uint16_t>(static_cast<unsigned char>((*data)[0]) |
                               (static_cast<unsigned char>((*data)[1]) << 8));
  data->remove_prefix(2);
  return true;
}

bool ReadU32LE(std::string_view* data, uint32_t* out) {
  if (data->size() < 4) {
    return false;
  }
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) {
    v |= static_cast<uint32_t>(static_cast<unsigned char>((*data)[i])) << (8 * i);
  }
  *out = v;
  data->remove_prefix(4);
  return true;
}

}  // namespace

std::vector<std::string> DeserializeStoredRow(const TableDef& table, std::string_view data) {
  (void)table;
  std::vector<std::string> row;
  uint16_t n = 0;
  if (!ReadU16LE(&data, &n)) {
    return row;
  }
  row.reserve(n);
  for (uint16_t i = 0; i < n; ++i) {
    uint32_t len = 0;
    if (!ReadU32LE(&data, &len) || data.size() < len) {
      return {};
    }
    row.emplace_back(data.substr(0, len));
    data.remove_prefix(len);
  }
  return row;
}

std::string BaseColumnName(std::string_view qualified_name) {
  const size_t pos = qualified_name.find_last_of('.');
  if (pos == std::string_view::npos) {
    return std::string(qualified_name);
  }
  return std::string(qualified_name.substr(pos + 1));
}

int ColumnIndexByName(const TableDef& table, std::string_view name) {
  const std::string base = BaseColumnName(name);
  for (size_t i = 0; i < table.columns.size(); ++i) {
    if (table.columns[i].name == base) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

std::string CellValueForJoinColumn(const TableDef& table, const std::vector<std::string>& cells,
                                   std::string_view join_column) {
  const int idx = ColumnIndexByName(table, join_column);
  if (idx < 0 || static_cast<size_t>(idx) >= cells.size()) {
    return {};
  }
  return cells[static_cast<size_t>(idx)];
}

uint32_t HashJoinKey(std::string_view key, uint32_t num_shards) {
  constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
  constexpr uint64_t kFnvPrime = 1099511628211ULL;
  uint64_t hash = kFnvOffset;
  for (unsigned char c : key) {
    hash ^= static_cast<uint64_t>(c);
    hash *= kFnvPrime;
  }
  return static_cast<uint32_t>(hash % std::max<uint32_t>(1u, num_shards));
}

}  // namespace txndb
