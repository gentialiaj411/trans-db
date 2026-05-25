#include "coordinator/executor.h"

#include "coordinator/distributed_join.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <sstream>
#include <unordered_map>

namespace txndb {

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

bool ReadU16LE(std::string_view* data, uint16_t* out) {
  if (data->size() < 2) {
    return false;
  }
  *out =
      static_cast<uint16_t>(static_cast<unsigned char>((*data)[0]) |
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

const ColumnDef* FindColumn(const TableDef& t, std::string_view name) {
  for (const auto& c : t.columns) {
    if (c.name == name) {
      return &c;
    }
  }
  return nullptr;
}

std::string BaseColName(std::string_view name) {
  const size_t pos = name.find_last_of('.');
  if (pos == std::string_view::npos) {
    return std::string(name);
  }
  return std::string(name.substr(pos + 1));
}

int ColumnIndex(const TableDef& t, std::string_view name) {
  for (size_t i = 0; i < t.columns.size(); ++i) {
    if (t.columns[i].name == name) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

}  // namespace

Executor::Executor(Coordinator* coordinator, Catalog* catalog)
    : coordinator_(coordinator), catalog_(catalog) {}

std::string Executor::LiteralToString(const LiteralExpr& lit) {
  return std::visit(
      [](auto&& v) -> std::string {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, int64_t>) {
          return std::to_string(v);
        } else if constexpr (std::is_same_v<T, double>) {
          std::ostringstream oss;
          oss << v;
          return oss.str();
        } else {
          return std::string(v);
        }
      },
      lit.value);
}

std::string Executor::LiteralToPkString(const LiteralExpr& lit, ColumnType ct) {
  // Use same string form as column serialization for PK routing key.
  (void)ct;
  return LiteralToString(lit);
}

bool Executor::CompareCell(const std::string& cell, CompareOp op, const LiteralExpr& lit,
                           ColumnType ct) {
  if (ct == ColumnType::FLOAT) {
    try {
      const double cv = std::stod(cell);
      const double lv = std::visit(
          [](auto&& v) -> double {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int64_t>) {
              return static_cast<double>(v);
            } else if constexpr (std::is_same_v<T, double>) {
              return v;
            } else {
              return std::stod(std::string(v));
            }
          },
          lit.value);
      const double eps = 1e-9;
      switch (op) {
        case CompareOp::EQ:
          return std::fabs(cv - lv) < eps;
        case CompareOp::NE:
          return std::fabs(cv - lv) >= eps;
        case CompareOp::LT:
          return cv < lv - eps;
        case CompareOp::LE:
          return cv <= lv + eps;
        case CompareOp::GT:
          return cv > lv + eps;
        case CompareOp::GE:
          return cv >= lv - eps;
      }
    } catch (...) {
      return false;
    }
  }

  if (ct == ColumnType::INT || ct == ColumnType::BIGINT) {
    try {
      const int64_t cv = std::stoll(cell);
      const int64_t lv =
          std::visit(
              [](auto&& v) -> int64_t {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, int64_t>) {
                  return v;
                } else if constexpr (std::is_same_v<T, double>) {
                  return static_cast<int64_t>(v);
                } else {
                  return std::stoll(std::string(v));
                }
              },
              lit.value);
      switch (op) {
        case CompareOp::EQ:
          return cv == lv;
        case CompareOp::NE:
          return cv != lv;
        case CompareOp::LT:
          return cv < lv;
        case CompareOp::LE:
          return cv <= lv;
        case CompareOp::GT:
          return cv > lv;
        case CompareOp::GE:
          return cv >= lv;
      }
    } catch (...) {
      return false;
    }
  }

  const std::string lv = LiteralToString(lit);
  switch (op) {
    case CompareOp::EQ:
      return cell == lv;
    case CompareOp::NE:
      return cell != lv;
    case CompareOp::LT:
      return cell < lv;
    case CompareOp::LE:
      return cell <= lv;
    case CompareOp::GT:
      return cell > lv;
    case CompareOp::GE:
      return cell >= lv;
  }
  return false;
}

std::string Executor::SerializeRow(const TableDef& table,
                                   const std::vector<std::string>& col_names,
                                   const std::vector<LiteralExpr>& values) {
  std::unordered_map<std::string, std::string> m;
  for (size_t i = 0; i < col_names.size(); ++i) {
    m[col_names[i]] = LiteralToString(values[i]);
  }
  std::string out;
  AppendU16LE(&out, static_cast<uint16_t>(table.columns.size()));
  for (const auto& c : table.columns) {
    auto it = m.find(c.name);
    const std::string cell = it == m.end() ? std::string() : it->second;
    AppendU32LE(&out, static_cast<uint32_t>(cell.size()));
    out.append(cell);
  }
  return out;
}

std::vector<std::string> Executor::DeserializeRow(const TableDef& table, std::string_view data) {
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

std::optional<Executor::KeyRange> Executor::ExtractKeyRange(const WhereClause& where,
                                                            const TableDef& table) {
  const std::string& pk = table.primary_key;
  const ColumnDef* pkcol = FindColumn(table, pk);
  if (!pkcol) {
    return std::nullopt;
  }

  bool saw_pk = false;
  bool saw_ne = false;
  bool is_float = pkcol->type == ColumnType::FLOAT;
  bool is_int_like = pkcol->type == ColumnType::INT || pkcol->type == ColumnType::BIGINT;
  bool is_text = pkcol->type == ColumnType::VARCHAR;

  // Integer / closed interval [lo, hi] inclusive; invalid if lo>hi.
  bool have_int_bounds = false;
  int64_t lo_i = std::numeric_limits<int64_t>::min();
  int64_t hi_i = std::numeric_limits<int64_t>::max();

  bool have_f_bounds = false;
  double lo_f = -std::numeric_limits<double>::infinity();
  double hi_f = std::numeric_limits<double>::infinity();

  std::optional<std::string> eq_text;

  auto apply_int_cmp = [&](CompareOp op, int64_t v) {
    have_int_bounds = true;
    switch (op) {
      case CompareOp::EQ:
        lo_i = std::max(lo_i, v);
        hi_i = std::min(hi_i, v);
        break;
      case CompareOp::NE:
        saw_ne = true;
        break;
      case CompareOp::GE:
        lo_i = std::max(lo_i, v);
        break;
      case CompareOp::GT:
        lo_i = std::max(lo_i, v + 1);
        break;
      case CompareOp::LE:
        hi_i = std::min(hi_i, v);
        break;
      case CompareOp::LT:
        hi_i = std::min(hi_i, v - 1);
        break;
    }
  };

  auto apply_f_cmp = [&](CompareOp op, double v) {
    have_f_bounds = true;
    const double eps = 1e-9;
    switch (op) {
      case CompareOp::EQ:
        lo_f = std::max(lo_f, v);
        hi_f = std::min(hi_f, v);
        break;
      case CompareOp::NE:
        saw_ne = true;
        break;
      case CompareOp::GE:
        lo_f = std::max(lo_f, v);
        break;
      case CompareOp::GT:
        lo_f = std::max(lo_f, std::nextafter(v, hi_f));
        break;
      case CompareOp::LE:
        hi_f = std::min(hi_f, v);
        break;
      case CompareOp::LT:
        hi_f = std::min(hi_f, std::nextafter(v, lo_f));
        break;
    }
    (void)eps;
  };

  for (const CompareExpr& ex : where.conditions) {
    if (ex.column.name != pk) {
      continue;
    }
    saw_pk = true;
    if (is_text) {
      // Only EQ on VARCHAR PK for lookups (ranges not represented in this slice).
      if (ex.op != CompareOp::EQ) {
        return std::nullopt;
      }
      const std::string v = LiteralToPkString(ex.value, pkcol->type);
      if (!eq_text) {
        eq_text = v;
      } else if (*eq_text != v) {
        return std::nullopt;
      }
      continue;
    }

    if (is_int_like) {
      const int64_t v = std::visit(
          [](auto&& x) -> int64_t {
            using T = std::decay_t<decltype(x)>;
            if constexpr (std::is_same_v<T, int64_t>) {
              return x;
            } else if constexpr (std::is_same_v<T, double>) {
              return static_cast<int64_t>(x);
            } else {
              return std::stoll(std::string(x));
            }
          },
          ex.value.value);
      apply_int_cmp(ex.op, v);
    } else if (is_float) {
      const double v = std::visit(
          [](auto&& x) -> double {
            using T = std::decay_t<decltype(x)>;
            if constexpr (std::is_same_v<T, int64_t>) {
              return static_cast<double>(x);
            } else if constexpr (std::is_same_v<T, double>) {
              return x;
            } else {
              return std::stod(std::string(x));
            }
          },
          ex.value.value);
      apply_f_cmp(ex.op, v);
    }
  }

  if (!saw_pk) {
    return std::nullopt;
  }
  if (saw_ne) {
    return std::nullopt;
  }

  if (is_text && eq_text) {
    return KeyRange{.start = *eq_text,
                    .end = {},
                    .start_unbounded = false,
                    .end_unbounded = true,
                    .is_point = true};
  }

  if (is_int_like && have_int_bounds) {
    if (lo_i > hi_i) {
      return KeyRange{.start = {}, .end = {}, .is_point = true};  // empty point - caller returns 0 rows
    }
    if (lo_i == hi_i) {
      const std::string k = std::to_string(lo_i);
      return KeyRange{.start = k, .end = {}, .is_point = true};
    }
    if (hi_i == std::numeric_limits<int64_t>::max()) {
      return KeyRange{.start = std::to_string(lo_i),
                      .end = {},
                      .start_unbounded = false,
                      .end_unbounded = true,
                      .is_point = false};
    }
    return KeyRange{.start = std::to_string(lo_i),
                    .end = std::to_string(hi_i + 1),
                    .start_unbounded = false,
                    .end_unbounded = false,
                    .is_point = false};
  }

  if (is_float && have_f_bounds) {
    if (lo_f > hi_f + 1e-9) {
      return KeyRange{.start = {}, .end = {}, .is_point = true};
    }
    if (std::fabs(lo_f - hi_f) < 1e-9) {
      std::ostringstream oss;
      oss << lo_f;
      return KeyRange{.start = oss.str(), .end = {}, .is_point = true};
    }
    return std::nullopt;
  }

  return std::nullopt;
}

std::optional<std::string> Executor::ExtractPrimaryKey(const WhereClause& where,
                                                       const TableDef& table) {
  const auto kr = ExtractKeyRange(where, table);
  if (!kr || !kr->is_point) {
    return std::nullopt;
  }
  return kr->start;
}

Executor::ExecOutput Executor::Execute(const Statement& stmt, uint64_t current_txn_id) {
  return std::visit(
      [this, current_txn_id](auto&& s) -> ExecOutput {
        using T = std::decay_t<decltype(s)>;
        if constexpr (std::is_same_v<T, CreateTableStmt>) {
          return ExecCreateTable(s, current_txn_id);
        } else if constexpr (std::is_same_v<T, InsertStmt>) {
          return ExecInsert(s, current_txn_id);
        } else if constexpr (std::is_same_v<T, SelectStmt>) {
          return ExecSelect(s, current_txn_id);
        } else if constexpr (std::is_same_v<T, UpdateStmt>) {
          return ExecUpdate(s, current_txn_id);
        } else if constexpr (std::is_same_v<T, DeleteStmt>) {
          return ExecDelete(s, current_txn_id);
        } else if constexpr (std::is_same_v<T, BeginStmt>) {
          return ExecBegin(current_txn_id);
        } else if constexpr (std::is_same_v<T, CommitStmt>) {
          return ExecCommit(current_txn_id);
        } else if constexpr (std::is_same_v<T, RollbackStmt>) {
          return ExecRollback(current_txn_id);
        } else {
          return ExecOutput{ExecResult{}, current_txn_id};
        }
      },
      stmt);
}

Executor::ExecOutput Executor::ExecCreateTable(const CreateTableStmt& stmt, uint64_t txn_id) {
  (void)txn_id;
  ExecOutput out;
  const uint32_t tid = catalog_->CreateTable(stmt.table_name, stmt.columns, stmt.primary_key);
  if (tid == 0) {
    out.result.ok = false;
    out.result.error = "create table failed (exists or invalid primary key)";
    out.txn_id = txn_id;
    return out;
  }
  out.result.command_tag = "CREATE TABLE";
  out.txn_id = txn_id;
  return out;
}

Executor::ExecOutput Executor::ExecInsert(const InsertStmt& stmt, uint64_t txn_id) {
  ExecOutput out;
  const TableDef* table = catalog_->GetTable(stmt.table_name);
  if (!table) {
    out.result.ok = false;
    out.result.error = "unknown table";
    out.txn_id = txn_id;
    return out;
  }
  for (const auto& c : table->columns) {
    if (std::find(stmt.columns.begin(), stmt.columns.end(), c.name) == stmt.columns.end()) {
      out.result.ok = false;
      out.result.error = "INSERT must specify all columns";
      out.txn_id = txn_id;
      return out;
    }
  }
  const std::string row = SerializeRow(*table, stmt.columns, stmt.values);
  const auto pk_it = std::find(stmt.columns.begin(), stmt.columns.end(), table->primary_key);
  if (pk_it == stmt.columns.end()) {
    out.result.ok = false;
    out.result.error = "primary key column missing in INSERT";
    out.txn_id = txn_id;
    return out;
  }
  const size_t pk_ix = static_cast<size_t>(pk_it - stmt.columns.begin());
  const ColumnDef* pk_def = FindColumn(*table, table->primary_key);
  if (!pk_def) {
    out.result.ok = false;
    out.result.error = "table missing primary key column";
    out.txn_id = txn_id;
    return out;
  }
  const std::string pk = LiteralToPkString(stmt.values[pk_ix], pk_def->type);

  auto run = [&](uint64_t t) -> Status {
    return coordinator_->Write(t, table->table_id, pk, row);
  };

  if (txn_id != 0) {
    const Status st = run(txn_id);
    if (!st.ok()) {
      out.result.ok = false;
      out.result.error = st.message();
    } else {
      out.result.affected_rows = 1;
      out.result.command_tag = "INSERT 0 1";
    }
    out.txn_id = txn_id;
    return out;
  }

  const uint64_t t = coordinator_->Begin();
  const Status st = run(t);
  if (!st.ok()) {
    (void)coordinator_->Abort(t);
    out.result.ok = false;
    out.result.error = st.message();
    out.txn_id = 0;
    return out;
  }
  const Status c = coordinator_->Commit(t);
  if (!c.ok()) {
    out.result.ok = false;
    out.result.error = c.message();
    out.txn_id = 0;
    return out;
  }
  out.result.affected_rows = 1;
  out.result.command_tag = "INSERT 0 1";
  out.txn_id = 0;
  return out;
}

Executor::ExecOutput Executor::ExecSelect(const SelectStmt& stmt, uint64_t txn_id) {
  ExecOutput out;
  const TableDef* table = catalog_->GetTable(stmt.table_name);
  if (!table) {
    out.result.ok = false;
    out.result.error = "unknown table";
    out.txn_id = txn_id;
    return out;
  }

  const bool extended = stmt.join.has_value() || stmt.has_order_by || stmt.has_limit ||
                        !stmt.aggregates.empty() || !stmt.has_where;
  if (extended) {
    const TableDef* right_table = nullptr;
    if (stmt.join) {
      right_table = catalog_->GetTable(stmt.join->table_name);
      if (!right_table) {
        out.result.ok = false;
        out.result.error = "unknown JOIN table";
        out.txn_id = txn_id;
        return out;
      }
    }

    auto run_extended = [&](uint64_t tid) -> Status {
      using RowMap = std::unordered_map<std::string, std::string>;
      std::vector<RowMap> rows;

      if (right_table && stmt.join.has_value() && coordinator_->NumShards() > 1) {
        DistributedJoinPlanner planner(coordinator_, catalog_, coordinator_->NumShards());
        const DistributedJoinPlanner::PhysicalOp op = ParseJoinPhysicalOpFromEnv();
        Status js = planner.ExecuteInnerJoin(stmt, *table, *right_table, tid, op, &rows);
        if (!js.ok()) {
          return js;
        }
      } else {
        std::vector<std::pair<std::string, std::string>> left_scan;
        Status s = coordinator_->Scan(tid, table->table_id, "", "", true, &left_scan);
        if (!s.ok()) {
          return s;
        }

        std::vector<std::pair<std::string, std::string>> right_scan;
        if (right_table) {
          s = coordinator_->Scan(tid, right_table->table_id, "", "", true, &right_scan);
          if (!s.ok()) {
            return s;
          }
        }

        rows.reserve(left_scan.size());
        for (auto& lkv : left_scan) {
          auto lcells = DeserializeRow(*table, lkv.second);
          if (lcells.size() != table->columns.size()) {
            return Status::Error(StatusCode::InvalidArgument, "corrupt left row");
          }
          RowMap base;
          for (size_t i = 0; i < table->columns.size(); ++i) {
            base[table->columns[i].name] = lcells[i];
            base[table->name + "." + table->columns[i].name] = lcells[i];
          }
          if (!right_table) {
            rows.push_back(std::move(base));
            continue;
          }
          for (auto& rkv : right_scan) {
            auto rcells = DeserializeRow(*right_table, rkv.second);
            if (rcells.size() != right_table->columns.size()) {
              return Status::Error(StatusCode::InvalidArgument, "corrupt right row");
            }
            RowMap joined = base;
            for (size_t i = 0; i < right_table->columns.size(); ++i) {
              joined[right_table->columns[i].name] = rcells[i];
              joined[right_table->name + "." + right_table->columns[i].name] = rcells[i];
            }
            auto lit = joined.find(stmt.join->left_column);
            auto rit = joined.find(stmt.join->right_column);
            if (lit != joined.end() && rit != joined.end() && lit->second == rit->second) {
              rows.push_back(std::move(joined));
            }
          }
        }
      }

      if (stmt.has_where) {
        std::vector<RowMap> filtered;
        filtered.reserve(rows.size());
        for (auto& row : rows) {
          bool ok = true;
          for (const auto& c : stmt.where.conditions) {
            auto it = row.find(c.column.name);
            if (it == row.end()) {
              it = row.find(BaseColName(c.column.name));
            }
            if (it == row.end()) {
              ok = false;
              break;
            }
            ColumnType ct = ColumnType::VARCHAR;
            std::string bn = BaseColName(c.column.name);
            if (const auto* col = FindColumn(*table, bn)) {
              ct = col->type;
            } else if (right_table) {
              if (const auto* col2 = FindColumn(*right_table, bn)) {
                ct = col2->type;
              }
            }
            if (!CompareCell(it->second, c.op, c.value, ct)) {
              ok = false;
              break;
            }
          }
          if (ok) {
            filtered.push_back(std::move(row));
          }
        }
        rows = std::move(filtered);
      }

      if (stmt.has_order_by) {
        std::sort(rows.begin(), rows.end(), [&](const RowMap& a, const RowMap& b) {
          auto av = a.find(stmt.order_by_column);
          auto bv = b.find(stmt.order_by_column);
          std::string as = (av != a.end()) ? av->second : "";
          std::string bs = (bv != b.end()) ? bv->second : "";
          if (stmt.order_desc) {
            return as > bs;
          }
          return as < bs;
        });
      }

      if (stmt.has_limit && rows.size() > stmt.limit) {
        rows.resize(static_cast<size_t>(stmt.limit));
      }

      if (!stmt.aggregates.empty()) {
        ResultRow rr;
        out.result.columns.clear();
        rr.values.reserve(stmt.aggregates.size() + stmt.columns.size());
        for (const auto& col : stmt.columns) {
          out.result.columns.push_back(col);
          if (rows.empty()) {
            rr.values.push_back("");
          } else {
            auto it = rows[0].find(col);
            if (it == rows[0].end()) {
              it = rows[0].find(BaseColName(col));
            }
            rr.values.push_back(it == rows[0].end() ? "" : it->second);
          }
        }
        for (const auto& agg : stmt.aggregates) {
          std::string cname = "agg";
          if (agg.func == SelectStmt::Aggregate::Func::COUNT) cname = "count";
          if (agg.func == SelectStmt::Aggregate::Func::SUM) cname = "sum";
          if (agg.func == SelectStmt::Aggregate::Func::MIN) cname = "min";
          if (agg.func == SelectStmt::Aggregate::Func::MAX) cname = "max";
          out.result.columns.push_back(cname);

          if (agg.func == SelectStmt::Aggregate::Func::COUNT) {
            if (agg.star) {
              rr.values.push_back(std::to_string(rows.size()));
            } else {
              uint64_t cnt = 0;
              for (const auto& row : rows) {
                auto it = row.find(agg.column);
                if (it == row.end()) {
                  it = row.find(BaseColName(agg.column));
                }
                if (it != row.end() && !it->second.empty()) {
                  ++cnt;
                }
              }
              rr.values.push_back(std::to_string(cnt));
            }
          } else {
            bool have = false;
            double sum = 0;
            double mn = 0;
            double mx = 0;
            for (const auto& row : rows) {
              auto it = row.find(agg.column);
              if (it == row.end()) {
                it = row.find(BaseColName(agg.column));
              }
              if (it == row.end()) {
                continue;
              }
              double v = 0;
              try {
                v = std::stod(it->second);
              } catch (...) {
                continue;
              }
              if (!have) {
                have = true;
                mn = mx = v;
              }
              sum += v;
              mn = std::min(mn, v);
              mx = std::max(mx, v);
            }
            if (!have) {
              rr.values.push_back("0");
            } else if (agg.func == SelectStmt::Aggregate::Func::SUM) {
              rr.values.push_back(std::to_string(sum));
            } else if (agg.func == SelectStmt::Aggregate::Func::MIN) {
              rr.values.push_back(std::to_string(mn));
            } else {
              rr.values.push_back(std::to_string(mx));
            }
          }
        }
        out.result.rows.push_back(std::move(rr));
      } else {
        out.result.columns.clear();
        if (stmt.select_all || stmt.columns.empty()) {
          for (const auto& c : table->columns) {
            out.result.columns.push_back(c.name);
          }
          if (right_table) {
            for (const auto& c : right_table->columns) {
              out.result.columns.push_back(c.name);
            }
          }
        } else {
          out.result.columns = stmt.columns;
        }
        for (const auto& row : rows) {
          ResultRow rr;
          rr.values.reserve(out.result.columns.size());
          for (const auto& c : out.result.columns) {
            auto it = row.find(c);
            if (it == row.end()) {
              it = row.find(BaseColName(c));
            }
            rr.values.push_back(it == row.end() ? "" : it->second);
          }
          out.result.rows.push_back(std::move(rr));
        }
      }
      out.result.command_tag = "SELECT " + std::to_string(out.result.rows.size());
      return Status::OK();
    };

    if (txn_id != 0) {
      Status st = run_extended(txn_id);
      if (!st.ok()) {
        out.result.ok = false;
        out.result.error = st.message();
      }
      out.txn_id = txn_id;
      return out;
    }
    uint64_t t = coordinator_->Begin();
    Status st = run_extended(t);
    if (!st.ok()) {
      (void)coordinator_->Abort(t);
      out.result.ok = false;
      out.result.error = st.message();
      out.txn_id = 0;
      return out;
    }
    Status c = coordinator_->Commit(t);
    if (!c.ok()) {
      out.result.ok = false;
      out.result.error = c.message();
    }
    out.txn_id = 0;
    return out;
  }

  const auto kr_opt = ExtractKeyRange(stmt.where, *table);
  if (!kr_opt) {
    out.result.ok = false;
    out.result.error =
        "WHERE must constrain primary key with a supported predicate (INT/BIGINT point or bounded "
        "range; FLOAT point only)";
    out.txn_id = txn_id;
    return out;
  }
  const KeyRange kr = *kr_opt;

  std::vector<std::string> select_col_names = stmt.columns;
  if (select_col_names.empty()) {
    for (const auto& c : table->columns) {
      select_col_names.push_back(c.name);
    }
  }

  // 0=fatal/corrupt, 1=skip row, 2=row appended
  auto classify_cells = [&](const std::vector<std::string>& cells) -> int {
    if (cells.size() != table->columns.size()) {
      out.result.ok = false;
      out.result.error = "corrupt row";
      return 0;
    }
    for (const CompareExpr& ex : stmt.where.conditions) {
      if (ex.column.name == table->primary_key) {
        continue;
      }
      const int ci = ColumnIndex(*table, ex.column.name);
      if (ci < 0) {
        out.result.ok = false;
        out.result.error = "unknown column in WHERE";
        return 0;
      }
      if (!CompareCell(cells[static_cast<size_t>(ci)], ex.op, ex.value,
                       table->columns[static_cast<size_t>(ci)].type)) {
        return 1;
      }
    }

    std::vector<std::string> projected;
    projected.reserve(select_col_names.size());
    for (const std::string& cn : select_col_names) {
      const int ci = ColumnIndex(*table, cn);
      if (ci < 0) {
        out.result.ok = false;
        out.result.error = "unknown column in SELECT";
        return 0;
      }
      projected.push_back(cells[static_cast<size_t>(ci)]);
    }
    if (out.result.columns.empty()) {
      out.result.columns = select_col_names;
    }
    out.result.rows.push_back(ResultRow{std::move(projected)});
    return 2;
  };

  auto finalize_tag = [&](uint64_t emitted) {
    out.result.command_tag = "SELECT " + std::to_string(emitted);
  };

  if (kr.is_point) {
    if (kr.start.empty()) {
      out.result.command_tag = "SELECT 0";
      out.txn_id = txn_id;
      return out;
    }

    auto run_point = [&](uint64_t tid) -> Status {
      std::string data;
      const Status rst = coordinator_->Read(tid, table->table_id, kr.start, &data);
      if (!rst.ok()) {
        return rst;
      }
      std::vector<std::string> cells = DeserializeRow(*table, data);
      const int cls = classify_cells(cells);
      if (cls == 0) {
        return Status::Error(StatusCode::InvalidArgument,
                             out.result.error.empty() ? "invalid row" : out.result.error);
      }
      finalize_tag(cls == 2 ? 1u : 0u);
      return Status::OK();
    };

    if (txn_id != 0) {
      const Status st = run_point(txn_id);
      if (!st.ok()) {
        if (st.code() == StatusCode::NotFound) {
          out.result.command_tag = "SELECT 0";
          out.txn_id = txn_id;
          return out;
        }
        out.result.ok = false;
        out.result.error = st.message();
      }
      out.txn_id = txn_id;
      return out;
    }

    const uint64_t t = coordinator_->Begin();
    const Status st = run_point(t);
    if (!st.ok()) {
      (void)coordinator_->Abort(t);
      if (st.code() == StatusCode::NotFound) {
        out.result.command_tag = "SELECT 0";
        out.txn_id = 0;
        return out;
      }
      out.result.ok = false;
      out.result.error = st.message();
      out.txn_id = 0;
      return out;
    }
    if (!out.result.ok) {
      (void)coordinator_->Abort(t);
      out.txn_id = 0;
      return out;
    }
    const Status c = coordinator_->Commit(t);
    if (!c.ok()) {
      out.result.ok = false;
      out.result.error = c.message();
    }
    out.txn_id = 0;
    return out;
  }

  if ((!kr.start_unbounded && kr.start.empty()) || (!kr.end_unbounded && kr.end.empty())) {
    out.result.ok = false;
    out.result.error = "invalid primary key range";
    out.txn_id = txn_id;
    return out;
  }

  auto run_range = [&](uint64_t tid) -> Status {
    std::vector<std::pair<std::string, std::string>> fetched;
    const Status rst = coordinator_->Scan(tid, table->table_id, kr.start, kr.end, kr.end_unbounded,
                                          &fetched);
    if (!rst.ok()) {
      return rst;
    }
    uint64_t emitted = 0;
    for (auto& kv : fetched) {
      (void)kv.first;
      std::vector<std::string> cells = DeserializeRow(*table, kv.second);
      const int cls = classify_cells(cells);
      if (cls == 0) {
        return Status::Error(StatusCode::InvalidArgument,
                             out.result.error.empty() ? "invalid row" : out.result.error);
      }
      if (cls == 2) {
        ++emitted;
      }
    }
    finalize_tag(emitted);
    return Status::OK();
  };

  if (txn_id != 0) {
    const Status st = run_range(txn_id);
    if (!st.ok()) {
      out.result.ok = false;
      out.result.error = st.message();
    }
    out.txn_id = txn_id;
    return out;
  }

  const uint64_t t = coordinator_->Begin();
  const Status st = run_range(t);
  if (!st.ok()) {
    (void)coordinator_->Abort(t);
    out.result.ok = false;
    out.result.error = st.message();
    out.txn_id = 0;
    return out;
  }
  if (!out.result.ok) {
    (void)coordinator_->Abort(t);
    out.txn_id = 0;
    return out;
  }
  const Status c = coordinator_->Commit(t);
  if (!c.ok()) {
    out.result.ok = false;
    out.result.error = c.message();
  }
  out.txn_id = 0;
  return out;
}

Executor::ExecOutput Executor::ExecUpdate(const UpdateStmt& stmt, uint64_t txn_id) {
  ExecOutput out;
  const TableDef* table = catalog_->GetTable(stmt.table_name);
  if (!table) {
    out.result.ok = false;
    out.result.error = "unknown table";
    out.txn_id = txn_id;
    return out;
  }
  const auto pk = ExtractPrimaryKey(stmt.where, *table);
  if (!pk) {
    out.result.ok = false;
    out.result.error = "UPDATE requires point lookup on primary key";
    out.txn_id = txn_id;
    return out;
  }

  auto apply = [&](uint64_t t) -> Status {
    std::string data;
    Status rs = coordinator_->Read(t, table->table_id, *pk, &data);
    if (!rs.ok()) {
      return rs;
    }
    std::vector<std::string> cells = DeserializeRow(*table, data);
    if (cells.size() != table->columns.size()) {
      return Status::Error(StatusCode::InvalidArgument, "corrupt row");
    }
    for (const CompareExpr& ex : stmt.where.conditions) {
      if (ex.column.name == table->primary_key) {
        continue;
      }
      const int ci = ColumnIndex(*table, ex.column.name);
      if (ci < 0) {
        return Status::Error(StatusCode::InvalidArgument, "unknown column in WHERE");
      }
      if (!CompareCell(cells[static_cast<size_t>(ci)], ex.op, ex.value,
                       table->columns[static_cast<size_t>(ci)].type)) {
        return Status::NotFound("filter mismatch");
      }
    }

    for (const auto& as : stmt.assignments) {
      const int ci = ColumnIndex(*table, as.column);
      if (ci < 0) {
        return Status::Error(StatusCode::InvalidArgument, "unknown SET column");
      }
      const ColumnType ct = table->columns[static_cast<size_t>(ci)].type;
      if (as.literal) {
        cells[static_cast<size_t>(ci)] = LiteralToString(*as.literal);
      } else if (as.arith) {
        const std::string& cur = cells[static_cast<size_t>(ci)];
        if (ct == ColumnType::FLOAT) {
          double base = std::stod(cur);
          const double opd = std::visit(
              [](auto&& v) -> double {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, int64_t>) {
                  return static_cast<double>(v);
                } else if constexpr (std::is_same_v<T, double>) {
                  return v;
                } else {
                  return std::stod(std::string(v));
                }
              },
              as.arith->operand.value);
          if (as.arith->op == ArithOp::ADD) {
            base += opd;
          } else {
            base -= opd;
          }
          std::ostringstream oss;
          oss << base;
          cells[static_cast<size_t>(ci)] = oss.str();
        } else {
          int64_t base = std::stoll(cur);
          const int64_t opd = std::visit(
              [](auto&& v) -> int64_t {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, int64_t>) {
                  return v;
                } else if constexpr (std::is_same_v<T, double>) {
                  return static_cast<int64_t>(v);
                } else {
                  return std::stoll(std::string(v));
                }
              },
              as.arith->operand.value);
          if (as.arith->op == ArithOp::ADD) {
            base += opd;
          } else {
            base -= opd;
          }
          cells[static_cast<size_t>(ci)] = std::to_string(base);
        }
      } else {
        return Status::Error(StatusCode::InvalidArgument, "bad assignment");
      }
    }

    std::string new_row;
    AppendU16LE(&new_row, static_cast<uint16_t>(table->columns.size()));
    for (const auto& cell : cells) {
      AppendU32LE(&new_row, static_cast<uint32_t>(cell.size()));
      new_row.append(cell);
    }
    return coordinator_->Write(t, table->table_id, *pk, new_row);
  };

  if (txn_id != 0) {
    const Status st = apply(txn_id);
    if (!st.ok()) {
      if (st.code() == StatusCode::NotFound) {
        out.result.affected_rows = 0;
        out.result.command_tag = "UPDATE 0";
        out.txn_id = txn_id;
        return out;
      }
      out.result.ok = false;
      out.result.error = st.message();
      out.txn_id = txn_id;
      return out;
    }
    out.result.affected_rows = 1;
    out.result.command_tag = "UPDATE 1";
    out.txn_id = txn_id;
    return out;
  }

  const uint64_t t = coordinator_->Begin();
  const Status st = apply(t);
  if (!st.ok()) {
    (void)coordinator_->Abort(t);
    if (st.code() == StatusCode::NotFound) {
      out.result.affected_rows = 0;
      out.result.command_tag = "UPDATE 0";
      out.txn_id = 0;
      return out;
    }
    out.result.ok = false;
    out.result.error = st.message();
    out.txn_id = 0;
    return out;
  }
  const Status c = coordinator_->Commit(t);
  if (!c.ok()) {
    out.result.ok = false;
    out.result.error = c.message();
    out.txn_id = 0;
    return out;
  }
  out.result.affected_rows = 1;
  out.result.command_tag = "UPDATE 1";
  out.txn_id = 0;
  return out;
}

Executor::ExecOutput Executor::ExecDelete(const DeleteStmt& stmt, uint64_t txn_id) {
  ExecOutput out;
  const TableDef* table = catalog_->GetTable(stmt.table_name);
  if (!table) {
    out.result.ok = false;
    out.result.error = "unknown table";
    out.txn_id = txn_id;
    return out;
  }
  const auto pk = ExtractPrimaryKey(stmt.where, *table);
  if (!pk) {
    out.result.ok = false;
    out.result.error = "DELETE requires point lookup on primary key";
    out.txn_id = txn_id;
    return out;
  }

  auto run = [&](uint64_t t) -> Status {
    std::string data;
    const Status rs = coordinator_->Read(t, table->table_id, *pk, &data);
    if (!rs.ok()) {
      return rs;
    }
    std::vector<std::string> cells = DeserializeRow(*table, data);
    if (cells.size() != table->columns.size()) {
      return Status::Error(StatusCode::InvalidArgument, "corrupt row");
    }
    for (const CompareExpr& ex : stmt.where.conditions) {
      if (ex.column.name == table->primary_key) {
        continue;
      }
      const int ci = ColumnIndex(*table, ex.column.name);
      if (ci < 0) {
        return Status::Error(StatusCode::InvalidArgument, "unknown column in WHERE");
      }
      if (!CompareCell(cells[static_cast<size_t>(ci)], ex.op, ex.value,
                       table->columns[static_cast<size_t>(ci)].type)) {
        return Status::NotFound("filter mismatch");
      }
    }
    return coordinator_->Delete(t, table->table_id, *pk);
  };

  if (txn_id != 0) {
    const Status st = run(txn_id);
    if (!st.ok()) {
      if (st.code() == StatusCode::NotFound) {
        out.result.affected_rows = 0;
        out.result.command_tag = "DELETE 0";
        out.txn_id = txn_id;
        return out;
      }
      out.result.ok = false;
      out.result.error = st.message();
      out.txn_id = txn_id;
      return out;
    }
    out.result.affected_rows = 1;
    out.result.command_tag = "DELETE 1";
    out.txn_id = txn_id;
    return out;
  }

  const uint64_t t = coordinator_->Begin();
  const Status st = run(t);
  if (!st.ok()) {
    (void)coordinator_->Abort(t);
    if (st.code() == StatusCode::NotFound) {
      out.result.affected_rows = 0;
      out.result.command_tag = "DELETE 0";
      out.txn_id = 0;
      return out;
    }
    out.result.ok = false;
    out.result.error = st.message();
    out.txn_id = 0;
    return out;
  }
  const Status c = coordinator_->Commit(t);
  if (!c.ok()) {
    out.result.ok = false;
    out.result.error = c.message();
    out.txn_id = 0;
    return out;
  }
  out.result.affected_rows = 1;
  out.result.command_tag = "DELETE 1";
  out.txn_id = 0;
  return out;
}

Executor::ExecOutput Executor::ExecBegin(uint64_t txn_id) {
  ExecOutput out;
  if (txn_id != 0) {
    out.result.ok = false;
    out.result.error = "already in transaction";
    out.txn_id = txn_id;
    return out;
  }
  out.txn_id = coordinator_->Begin();
  out.result.command_tag = "BEGIN";
  return out;
}

Executor::ExecOutput Executor::ExecCommit(uint64_t txn_id) {
  ExecOutput out;
  if (txn_id == 0) {
    out.result.ok = false;
    out.result.error = "no transaction in progress";
    out.txn_id = 0;
    return out;
  }
  const Status st = coordinator_->Commit(txn_id);
  if (!st.ok()) {
    out.result.ok = false;
    out.result.error = st.message();
    out.txn_id = txn_id;
    return out;
  }
  out.result.command_tag = "COMMIT";
  out.txn_id = 0;
  return out;
}

Executor::ExecOutput Executor::ExecRollback(uint64_t txn_id) {
  ExecOutput out;
  if (txn_id != 0) {
    (void)coordinator_->Abort(txn_id);
  }
  out.result.command_tag = "ROLLBACK";
  out.txn_id = 0;
  return out;
}

}  // namespace txndb
