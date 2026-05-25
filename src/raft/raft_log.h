#pragma once

#include <cstdint>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
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

enum class RaftEntryType : uint8_t {
  TXN_BEGIN = 1,
  TXN_WRITE = 2,
  TXN_DELETE = 3,
  TXN_PREPARE = 4,
  TXN_COMMIT = 5,
  TXN_ABORT = 6,
  NOOP = 7,
  TXN_PREPARE_BATCH = 8,
};

struct RaftLogEntry {
  uint64_t index{0};
  uint64_t term{0};
  RaftEntryType type{RaftEntryType::TXN_BEGIN};
  std::string payload;
};

class RaftLog {
public:
  RaftLog() = default;
  explicit RaftLog(std::string path);

  uint64_t Append(uint64_t term, RaftEntryType type, std::string payload);

  std::optional<RaftLogEntry> Get(uint64_t index) const;

  std::vector<RaftLogEntry> GetRange(uint64_t start_index, uint64_t end_index) const;

  uint64_t LastIndex() const;

  uint64_t TermAt(uint64_t index) const;

  void TruncateFrom(uint64_t index);

  void AppendEntries(const std::vector<RaftLogEntry>& entries);

private:
  void Load();
  bool EnsureAppendFileOpen();
  bool AppendEntryToFile(const RaftLogEntry& entry);
  bool SyncFileLocked();
  bool RewriteFileFromMemory();

  mutable std::mutex mu_;
  std::vector<RaftLogEntry> entries_;
  std::string path_;
  std::ofstream ofs_;
#ifdef _WIN32
  HANDLE sync_handle_{INVALID_HANDLE_VALUE};
#else
  int sync_fd_{-1};
#endif
};

}  // namespace txndb
