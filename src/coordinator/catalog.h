#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace txndb {

enum class ColumnType : uint8_t {
  INT,      // 4 bytes, maps to PostgreSQL int4 (OID 23)
  BIGINT,   // 8 bytes, maps to PostgreSQL int8 (OID 20)
  FLOAT,    // 8 bytes, maps to PostgreSQL float8 (OID 701)
  VARCHAR,  // variable length text, maps to PostgreSQL text (OID 25)
};

struct ColumnDef {
  std::string name;
  ColumnType type;
};

struct TableDef {
  uint32_t table_id;
  std::string name;
  std::vector<ColumnDef> columns;
  std::string primary_key;  // column name of the primary key
};

class Catalog {
public:
  Catalog() = default;

  // Create a table. Returns the assigned table_id.
  // Returns 0 if table already exists.
  uint32_t CreateTable(const std::string& name, const std::vector<ColumnDef>& columns,
                       const std::string& primary_key);

  // Look up table by name
  const TableDef* GetTable(const std::string& name) const;

  // Look up table by id
  const TableDef* GetTableById(uint32_t table_id) const;

  // List all tables
  std::vector<const TableDef*> ListTables() const;

private:
  mutable std::mutex mu_;
  uint32_t next_table_id_{1};
  std::unordered_map<std::string, TableDef> tables_;
  std::unordered_map<uint32_t, std::string> id_to_name_;
};

}  // namespace txndb
