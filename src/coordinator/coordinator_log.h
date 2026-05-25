#pragma once

#include "storage/status.h"

#include <cstdint>
#include <functional>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace txndb {

enum class CoordinatorLogRecordType : uint8_t {
  PREPARING = 1,
  COMMITTING = 2,
  COMMITTED = 3,
  ABORTING = 4,
  ABORTED = 5,
};

struct CoordinatorLogRecord {
  CoordinatorLogRecordType type{CoordinatorLogRecordType::PREPARING};
  uint64_t txn_id{0};
  uint64_t timestamp_us{0};
  std::vector<uint32_t> shards;
};

class CoordinatorLog {
public:
  static Status Open(const std::string& path, std::unique_ptr<CoordinatorLog>* out);
  ~CoordinatorLog();

  CoordinatorLog(CoordinatorLog&&) = delete;
  CoordinatorLog& operator=(CoordinatorLog&&) = delete;

  Status Append(const CoordinatorLogRecord& record);
  Status AppendWrite(const CoordinatorLogRecord& record);
  Status Flush();
  Status Replay(const std::function<void(const CoordinatorLogRecord&)>& visitor);

private:
  CoordinatorLog() = default;
  Status WriteRecord(const CoordinatorLogRecord& record);
  Status SyncLocked();

  std::mutex mu_;
  std::string path_;
  std::ofstream ofs_;
#ifdef _WIN32
  HANDLE sync_handle_{INVALID_HANDLE_VALUE};
#else
  int sync_fd_{-1};
#endif
};

}  // namespace txndb
