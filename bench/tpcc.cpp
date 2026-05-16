#include "bench/tpcc.h"

#include "storage/status.h"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace txndb {
namespace tpcc {

namespace {

void AppendU16LE(std::string* buf, uint16_t v) {
  buf->push_back(static_cast<char>(v & 0xFF));
  buf->push_back(static_cast<char>((v >> 8) & 0xFF));
}

void AppendU32LE(std::string* buf, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    buf->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
  }
}

std::string EncodeRow(const std::vector<std::string>& cells) {
  std::string out;
  AppendU16LE(&out, static_cast<uint16_t>(cells.size()));
  for (const auto& c : cells) {
    AppendU32LE(&out, static_cast<uint32_t>(c.size()));
    out.append(c);
  }
  return out;
}

Status RunWriteTx(Coordinator* coordinator, uint32_t table_id,
                  const std::vector<std::pair<std::string, std::string>>& kvs) {
  const uint64_t txn = coordinator->Begin();
  for (const auto& kv : kvs) {
    const Status s = coordinator->Write(txn, table_id, kv.first, kv.second);
    if (!s.ok()) {
      (void)coordinator->Abort(txn);
      return s;
    }
  }
  const Status c = coordinator->Commit(txn);
  if (!c.ok()) {
    (void)coordinator->Abort(txn);
  }
  return c;
}

}  // namespace

std::string FormatPK(int64_t id) {
  std::ostringstream oss;
  oss << std::setw(10) << std::setfill('0') << id;
  return oss.str();
}

int64_t ParsePK(const std::string& pk) {
  return std::stoll(pk);
}

uint64_t LoadTPCCData(Coordinator* coordinator, Catalog* catalog, const TPCCConfig& config) {
  const uint32_t warehouse_tid =
      catalog->CreateTable("warehouse", {{"w_id", ColumnType::INT}, {"w_name", ColumnType::VARCHAR},
                                         {"w_ytd", ColumnType::FLOAT}},
                           "w_id");
  const uint32_t district_tid =
      catalog->CreateTable("district", {{"d_id", ColumnType::INT}, {"d_w_id", ColumnType::INT},
                                        {"d_name", ColumnType::VARCHAR}, {"d_next_o_id", ColumnType::INT},
                                        {"d_ytd", ColumnType::FLOAT}},
                           "d_id");
  const uint32_t customer_tid =
      catalog->CreateTable("customer", {{"c_id", ColumnType::INT}, {"c_d_id", ColumnType::INT},
                                        {"c_w_id", ColumnType::INT}, {"c_name", ColumnType::VARCHAR},
                                        {"c_balance", ColumnType::FLOAT}},
                           "c_id");

  const TableDef* warehouse_tbl = catalog->GetTable("warehouse");
  const TableDef* district_tbl = catalog->GetTable("district");
  const TableDef* customer_tbl = catalog->GetTable("customer");

  const uint32_t wtid = warehouse_tid == 0 && warehouse_tbl ? warehouse_tbl->table_id : warehouse_tid;
  const uint32_t dtid = district_tid == 0 && district_tbl ? district_tbl->table_id : district_tid;
  const uint32_t ctid = customer_tid == 0 && customer_tbl ? customer_tbl->table_id : customer_tid;

  if (wtid == 0 || dtid == 0 || ctid == 0) {
    throw std::runtime_error("TPC-C table creation failed");
  }

  uint64_t warehouse_rows = 0;
  uint64_t district_rows = 0;
  uint64_t customer_rows = 0;

  for (uint32_t w = 1; w <= config.num_warehouses; ++w) {
    {
      std::vector<std::pair<std::string, std::string>> kvs;
      kvs.emplace_back(FormatPK(w),
                       EncodeRow({std::to_string(w), "WH-" + std::to_string(w), "300000"}));
      const Status s = RunWriteTx(coordinator, wtid, kvs);
      if (!s.ok()) {
        throw std::runtime_error("warehouse load failed: " + s.message());
      }
      ++warehouse_rows;
    }

    for (uint32_t d = 0; d < config.districts_per_wh; ++d) {
      const int64_t d_id = static_cast<int64_t>(w) * 10 + static_cast<int64_t>(d);

      {
        std::vector<std::pair<std::string, std::string>> kvs;
        kvs.emplace_back(FormatPK(d_id), EncodeRow({std::to_string(d_id), std::to_string(w),
                                                    "DIST-" + std::to_string(w) + "-" + std::to_string(d),
                                                    "1", "30000"}));
        const Status s = RunWriteTx(coordinator, dtid, kvs);
        if (!s.ok()) {
          throw std::runtime_error("district load failed: " + s.message());
        }
        ++district_rows;
      }

      {
        std::vector<std::pair<std::string, std::string>> kvs;
        kvs.reserve(config.customers_per_dist);
        for (uint32_t c = 1; c <= config.customers_per_dist; ++c) {
          const int64_t c_id = static_cast<int64_t>(w) * 100000 + static_cast<int64_t>(d) * 10000 +
                               static_cast<int64_t>(c);
          kvs.emplace_back(FormatPK(c_id), EncodeRow({std::to_string(c_id), std::to_string(d),
                                                      std::to_string(w), "CUST-" + std::to_string(c),
                                                      "-10"}));
        }
        const Status s = RunWriteTx(coordinator, ctid, kvs);
        if (!s.ok()) {
          throw std::runtime_error("customer batch load failed: " + s.message());
        }
        customer_rows += kvs.size();
      }
    }
  }

  const uint64_t total = warehouse_rows + district_rows + customer_rows;
  std::cout << "Loaded " << warehouse_rows << " warehouses, " << district_rows << " districts, "
            << customer_rows << " customers (" << total << " rows)" << std::endl;
  return total;
}

}  // namespace tpcc
}  // namespace txndb
