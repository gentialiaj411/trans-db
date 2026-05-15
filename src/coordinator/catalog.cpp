#include "coordinator/catalog.h"

#include <algorithm>

namespace txndb {

uint32_t Catalog::CreateTable(const std::string& name, const std::vector<ColumnDef>& columns,
                              const std::string& primary_key) {
  std::scoped_lock lk(mu_);
  if (tables_.count(name)) {
    return 0;
  }
  const bool pk_ok =
      std::any_of(columns.begin(), columns.end(),
                  [&](const ColumnDef& c) { return c.name == primary_key; });
  if (!pk_ok) {
    return 0;
  }

  TableDef def;
  def.table_id = next_table_id_++;
  def.name = name;
  def.columns = columns;
  def.primary_key = primary_key;

  id_to_name_[def.table_id] = name;
  tables_.emplace(name, std::move(def));
  return tables_.at(name).table_id;
}

const TableDef* Catalog::GetTable(const std::string& name) const {
  std::scoped_lock lk(mu_);
  auto it = tables_.find(name);
  return it == tables_.end() ? nullptr : &it->second;
}

const TableDef* Catalog::GetTableById(uint32_t table_id) const {
  std::scoped_lock lk(mu_);
  auto it = id_to_name_.find(table_id);
  if (it == id_to_name_.end()) {
    return nullptr;
  }
  auto tit = tables_.find(it->second);
  return tit == tables_.end() ? nullptr : &tit->second;
}

std::vector<const TableDef*> Catalog::ListTables() const {
  std::scoped_lock lk(mu_);
  std::vector<const TableDef*> out;
  out.reserve(tables_.size());
  for (const auto& [_, t] : tables_) {
    out.push_back(&t);
  }
  std::sort(out.begin(), out.end(),
            [](const TableDef* a, const TableDef* b) { return a->table_id < b->table_id; });
  return out;
}

}  // namespace txndb
