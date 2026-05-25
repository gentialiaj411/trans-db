#include "shard/distributed_join_handler.h"

#include "storage/status.h"
#include "txn/txn_manager.h"

#include <unordered_map>
#include <vector>

namespace txndb {
namespace {

using RowCells = std::vector<std::string>;

RowCells DecodeRow(std::string_view bytes, size_t column_count) {
  RowCells cells;
  if (bytes.size() < 2) {
    return cells;
  }
  uint16_t n = static_cast<uint16_t>(static_cast<unsigned char>(bytes[0]) |
                                     (static_cast<unsigned char>(bytes[1]) << 8));
  if (n != column_count) {
    return {};
  }
  std::string_view data = bytes;
  data.remove_prefix(2);
  cells.reserve(n);
  for (uint16_t i = 0; i < n; ++i) {
    if (data.size() < 4) {
      return {};
    }
    uint32_t len = static_cast<uint32_t>(static_cast<unsigned char>(data[0]) |
                                         (static_cast<unsigned char>(data[1]) << 8) |
                                         (static_cast<unsigned char>(data[2]) << 16) |
                                         (static_cast<unsigned char>(data[3]) << 24));
    data.remove_prefix(4);
    if (data.size() < len) {
      return {};
    }
    cells.emplace_back(data.substr(0, len));
    data.remove_prefix(len);
  }
  return cells;
}

void EmitJoinedRow(DistributedJoinResponse* response, const std::vector<std::string>& build_names,
                   const std::string& build_table_name, const RowCells& build_cells,
                   const std::vector<std::string>& probe_names, const std::string& probe_table_name,
                   const RowCells& probe_cells) {
  JoinOutputRow* out = response->add_rows();
  for (size_t i = 0; i < build_names.size() && i < build_cells.size(); ++i) {
    JoinOutputColumn* col = out->add_columns();
    col->set_name(build_names[i]);
    col->set_value(build_cells[i]);
    JoinOutputColumn* qual = out->add_columns();
    qual->set_name(build_table_name + "." + build_names[i]);
    qual->set_value(build_cells[i]);
  }
  for (size_t i = 0; i < probe_names.size() && i < probe_cells.size(); ++i) {
    JoinOutputColumn* col = out->add_columns();
    col->set_name(probe_names[i]);
    col->set_value(probe_cells[i]);
    JoinOutputColumn* qual = out->add_columns();
    qual->set_name(probe_table_name + "." + probe_names[i]);
    qual->set_value(probe_cells[i]);
  }
}

void LocalHashJoin(const std::vector<JoinRowPayload>& build_rows,
                   const std::vector<JoinRowPayload>& probe_rows, size_t build_col_count,
                   size_t probe_col_count, const std::vector<std::string>& build_names,
                   const std::string& build_table_name,
                   const std::vector<std::string>& probe_names,
                   const std::string& probe_table_name, DistributedJoinResponse* response) {
  std::unordered_map<std::string, std::vector<std::string>> build_map;
  build_map.reserve(build_rows.size());
  for (const auto& row : build_rows) {
    build_map[row.join_key()].push_back(row.row_bytes());
  }

  for (const auto& probe : probe_rows) {
    auto it = build_map.find(probe.join_key());
    if (it == build_map.end()) {
      continue;
    }
    const RowCells probe_cells = DecodeRow(probe.row_bytes(), probe_col_count);
    if (probe_cells.empty()) {
      continue;
    }
    for (const auto& build_bytes : it->second) {
      const RowCells build_cells = DecodeRow(build_bytes, build_col_count);
      if (build_cells.empty()) {
        continue;
      }
      EmitJoinedRow(response, build_names, build_table_name, build_cells, probe_names,
                    probe_table_name, probe_cells);
    }
  }
}

}  // namespace

namespace {

uint32_t ErrorCode(StatusCode code) {
  return static_cast<uint32_t>(code);
}

}  // namespace

bool RunDistributedJoin(TxnManager* txn_mgr, uint64_t local_txn_id, const DistributedJoinRequest& request,
                        DistributedJoinResponse* response) {
  response->set_ok(false);
  if (!txn_mgr || local_txn_id == 0) {
    response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
    response->set_error_message("txn not active");
    return false;
  }

  std::vector<std::string> build_names;
  build_names.reserve(static_cast<size_t>(request.build_column_names_size()));
  for (const auto& n : request.build_column_names()) {
    build_names.push_back(n);
  }
  std::vector<std::string> probe_names;
  probe_names.reserve(static_cast<size_t>(request.probe_column_names_size()));
  for (const auto& n : request.probe_column_names()) {
    probe_names.push_back(n);
  }

  const size_t build_col_count = build_names.size();
  const size_t probe_col_count = probe_names.size();

  if (request.op() == JOIN_OP_BROADCAST) {
    std::vector<JoinRowPayload> build_rows;
    build_rows.reserve(static_cast<size_t>(request.broadcast_build_rows_size()));
    for (const auto& row : request.broadcast_build_rows()) {
      build_rows.push_back(row);
    }

    std::vector<std::pair<std::string, std::string>> probe_scan;
    const Status scan_st = txn_mgr->Scan(local_txn_id, request.probe_table_id(), "", "", true, &probe_scan);
    if (!scan_st.ok()) {
      response->set_error_code(ErrorCode(scan_st.code()));
      response->set_error_message(scan_st.message());
      return false;
    }

    std::vector<JoinRowPayload> probe_rows;
    probe_rows.reserve(probe_scan.size());
    for (auto& kv : probe_scan) {
      JoinRowPayload payload;
      payload.set_row_bytes(std::move(kv.second));
      const RowCells cells = DecodeRow(payload.row_bytes(), probe_col_count);
      if (cells.empty()) {
        continue;
      }
      const std::string base = request.probe_join_col().find('.') == std::string::npos
                                   ? request.probe_join_col()
                                   : request.probe_join_col().substr(
                                         request.probe_join_col().find_last_of('.') + 1);
      size_t idx = 0;
      for (; idx < probe_names.size(); ++idx) {
        if (probe_names[idx] == base) {
          break;
        }
      }
      if (idx >= cells.size()) {
        continue;
      }
      payload.set_join_key(cells[idx]);
      probe_rows.push_back(std::move(payload));
    }

    LocalHashJoin(build_rows, probe_rows, build_col_count, probe_col_count, build_names,
                  request.build_table_name(), probe_names, request.probe_table_name(), response);
    response->set_ok(true);
    return true;
  }

  if (request.op() == JOIN_OP_SHUFFLE) {
    std::vector<JoinRowPayload> build_rows;
    build_rows.reserve(static_cast<size_t>(request.shuffle_build_rows_size()));
    for (const auto& row : request.shuffle_build_rows()) {
      build_rows.push_back(row);
    }
    std::vector<JoinRowPayload> probe_rows;
    probe_rows.reserve(static_cast<size_t>(request.shuffle_probe_rows_size()));
    for (const auto& row : request.shuffle_probe_rows()) {
      probe_rows.push_back(row);
    }
    LocalHashJoin(build_rows, probe_rows, build_col_count, probe_col_count, build_names,
                  request.build_table_name(), probe_names, request.probe_table_name(), response);
    response->set_ok(true);
    return true;
  }

  response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
  response->set_error_message("unknown join physical op");
  return false;
}

}  // namespace txndb
