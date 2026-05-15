#pragma once

#include "coordinator/catalog.h"
#include "coordinator/coordinator.h"
#include "coordinator/parser.h"
#include "storage/status.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace txndb {

// A single result row
struct ResultRow {
  std::vector<std::string> values;  // one string per column, in column order
};

// Execution result for a statement
struct ExecResult {
  bool ok{true};
  std::string error;

  // For SELECT: column names + rows
  std::vector<std::string> columns;
  std::vector<ResultRow> rows;

  // For INSERT/UPDATE/DELETE: affected row count
  uint64_t affected_rows{0};

  // Command tag for pgwire (e.g., "SELECT 5", "INSERT 0 1", "UPDATE 3")
  std::string command_tag;
};

class Executor {
public:
  Executor(Coordinator* coordinator, Catalog* catalog);

  // Execute a parsed statement within a session.
  // txn_id: the current transaction id (0 = auto-commit / no txn).
  // Returns the txn_id to use going forward (may change on BEGIN).
  struct ExecOutput {
    ExecResult result;
    uint64_t txn_id;  // updated txn id (changed on BEGIN, cleared on COMMIT/ROLLBACK)
  };

  ExecOutput Execute(const Statement& stmt, uint64_t current_txn_id);

private:
  ExecOutput ExecCreateTable(const CreateTableStmt& stmt, uint64_t txn_id);
  ExecOutput ExecInsert(const InsertStmt& stmt, uint64_t txn_id);
  ExecOutput ExecSelect(const SelectStmt& stmt, uint64_t txn_id);
  ExecOutput ExecUpdate(const UpdateStmt& stmt, uint64_t txn_id);
  ExecOutput ExecDelete(const DeleteStmt& stmt, uint64_t txn_id);
  ExecOutput ExecBegin(uint64_t txn_id);
  ExecOutput ExecCommit(uint64_t txn_id);
  ExecOutput ExecRollback(uint64_t txn_id);

  // Serialize a row's columns to a single string (key-value encoding for storage)
  std::string SerializeRow(const TableDef& table, const std::vector<std::string>& col_names,
                           const std::vector<LiteralExpr>& values);

  // Deserialize a stored value back into column strings
  std::vector<std::string> DeserializeRow(const TableDef& table, std::string_view data);

  // Extract primary key value from a WHERE clause (for point lookups)
  std::optional<std::string> ExtractPrimaryKey(const WhereClause& where, const TableDef& table);

  // Extract primary key range [start, end) from WHERE clause (for range scans)
  struct KeyRange {
    std::string start;     // inclusive
    std::string end;       // exclusive (empty = unbounded high)
    bool start_unbounded{false};
    bool end_unbounded{false};
    bool is_point{false};  // true if WHERE is pk = literal
  };
  std::optional<KeyRange> ExtractKeyRange(const WhereClause& where, const TableDef& table);

  static std::string LiteralToString(const LiteralExpr& lit);
  static std::string LiteralToPkString(const LiteralExpr& lit, ColumnType ct);
  static bool CompareCell(const std::string& cell, CompareOp op, const LiteralExpr& lit,
                          ColumnType ct);

  Coordinator* coordinator_;
  Catalog* catalog_;
};

}  // namespace txndb
