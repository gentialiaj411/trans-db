#pragma once

#include "storage/status.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace txndb {

enum class TxnState : uint8_t {
  ACTIVE,
  PREPARED,
  COMMITTED,
  ABORTED,
};

struct BufferedWrite {
  uint32_t table_id{};
  std::string key;
  std::string value;
  bool is_delete{false};
};

struct ReadEntry {
  uint32_t table_id{};
  std::string key;
  uint64_t observed_write_ts{0};
};

class Transaction {
public:
  uint64_t txn_id{0};
  uint64_t snapshot_ts{0};
  uint64_t prepare_commit_ts{0};
  TxnState state{TxnState::ACTIVE};

  std::vector<BufferedWrite> write_set;
  std::vector<ReadEntry> read_set;

  std::vector<std::pair<uint32_t, std::string>> locked_keys;
};

}  // namespace txndb
