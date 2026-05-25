#pragma once

#include "coordinator/catalog.h"
#include "coordinator/coordinator.h"

#include <cstdint>
#include <string>

namespace txndb {
namespace tpcc {

struct TPCCConfig {
  uint32_t num_warehouses = 2;
  uint32_t districts_per_wh = 10;
  uint32_t customers_per_dist = 100;
  uint32_t items_per_wh = 1000;
};

uint64_t LoadTPCCData(Coordinator* coordinator, Catalog* catalog, const TPCCConfig& config);
std::string FormatPK(int64_t id);
int64_t ParsePK(const std::string& pk);

}  // namespace tpcc
}  // namespace txndb
