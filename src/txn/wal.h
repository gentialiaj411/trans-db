#pragma once

#include "storage/status.h"

#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#undef DELETE  // windows.h defines DELETE as 0x00010000L; conflicts with WALRecordType::DELETE
#else
using WalSyncFd = int;
#endif

namespace txndb {

enum class WALRecordType : uint8_t {
  BEGIN = 1,
  WRITE = 2,
  DELETE = 3,
  PREPARE = 4,
  COMMIT = 5,
  ABORT = 6,
};

struct WALRecord {
  uint64_t lsn{0};
  uint64_t txn_id{0};
  WALRecordType type{WALRecordType::BEGIN};
  std::string payload;
};

class WAL {
public:
  static Status Open(std::string_view path, std::unique_ptr<WAL>* out);

  ~WAL();

  WAL(WAL&&) = delete;
  WAL& operator=(WAL&&) = delete;

  Status Append(uint64_t txn_id, WALRecordType type, std::string_view payload);

  Status Sync();

  Status AppendSync(uint64_t txn_id, WALRecordType type, std::string_view payload);

  Status Replay(std::function<void(const WALRecord&)> visitor);

  uint64_t CurrentLSN() const { return next_lsn_; }

  Status Truncate();

  static std::string SerializeBeginPayload(uint64_t snapshot_ts);
  static std::string SerializeWritePayload(uint32_t table_id, std::string_view key, std::string_view value,
                                          uint64_t write_ts);
  static std::string SerializeDeletePayload(uint32_t table_id, std::string_view key, uint64_t write_ts);
  static std::string SerializePreparePayload(uint64_t commit_ts);
  static std::string SerializeCommitPayload(uint64_t commit_ts);

  struct BeginInfo {
    uint64_t snapshot_ts{0};
  };
  struct WriteInfo {
    uint32_t table_id{0};
    std::string key;
    std::string value;
    uint64_t write_ts{0};
  };
  struct DeleteInfo {
    uint32_t table_id{0};
    std::string key;
    uint64_t write_ts{0};
  };
  struct PrepareInfo {
    uint64_t commit_ts{0};
  };
  struct CommitInfo {
    uint64_t commit_ts{0};
  };

  static bool DeserializeBeginPayload(std::string_view payload, BeginInfo* out);
  static bool DeserializeWritePayload(std::string_view payload, WriteInfo* out);
  static bool DeserializeDeletePayload(std::string_view payload, DeleteInfo* out);
  static bool DeserializePreparePayload(std::string_view payload, PrepareInfo* out);
  static bool DeserializeCommitPayload(std::string_view payload, CommitInfo* out);

private:
  WAL() = default;

  Status WriteRecord(uint64_t lsn, uint64_t txn_id, WALRecordType type, std::string_view payload);
  static void AppendFixedU8(std::string* dst, uint8_t v);
  static void AppendFixedLe32(std::string* dst, uint32_t v);
  static void AppendFixedLe64(std::string* dst, uint64_t v);

  static bool ConsumeLe32(std::string_view* data, uint32_t* out);
  static bool ConsumeLe64(std::string_view* data, uint64_t* out);

  mutable std::mutex mu_;
  std::string path_;
  std::ofstream ofs_;
#ifdef _WIN32
  HANDLE sync_handle_{INVALID_HANDLE_VALUE};
#else
  WalSyncFd sync_fd_{-1};
#endif
  uint64_t next_lsn_{1};
};

}  // namespace txndb
