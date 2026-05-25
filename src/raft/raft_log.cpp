#include "raft/raft_log.h"

#include "txn/group_commit.h"
#include "storage/status.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif
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

namespace {

void AppendLe64(std::string* out, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>(v & 0xffu));
    v >>= 8;
  }
}

void AppendLe32(std::string* out, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    out->push_back(static_cast<char>(v & 0xffu));
    v >>= 8;
  }
}

uint64_t ReadLe64(const char* p) {
  uint64_t out = 0;
  for (int i = 0; i < 8; ++i) {
    out |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  }
  return out;
}

uint32_t ReadLe32(const char* p) {
  uint32_t out = 0;
  for (int i = 0; i < 4; ++i) {
    out |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  }
  return out;
}

std::string SerializeRaftEntry(const RaftLogEntry& e) {
  std::string rec;
  rec.reserve(21 + e.payload.size());
  AppendLe64(&rec, e.index);
  AppendLe64(&rec, e.term);
  rec.push_back(static_cast<char>(e.type));
  AppendLe32(&rec, static_cast<uint32_t>(e.payload.size()));
  rec.append(e.payload);
  return rec;
}

}  // namespace

RaftLog::RaftLog(std::string path) : path_(std::move(path)) {
  Load();
  (void)EnsureAppendFileOpen();
}

uint64_t RaftLog::Append(uint64_t term, RaftEntryType type, std::string payload) {
  std::scoped_lock lk(mu_);
  const uint64_t idx = entries_.empty() ? 1u : (entries_.back().index + 1);
  RaftLogEntry e;
  e.index = idx;
  e.term = term;
  e.type = type;
  e.payload = std::move(payload);
  if (!AppendEntryToFile(e)) {
    return 0;
  }
  entries_.push_back(e);

  if (!SyncFileLocked()) {
    return 0;
  }
  return idx;
}

std::optional<RaftLogEntry> RaftLog::Get(uint64_t index) const {
  std::scoped_lock lk(mu_);
  if (index == 0 || entries_.empty() || index > entries_.back().index) {
    return std::nullopt;
  }
  if (index < entries_.front().index) {
    return std::nullopt;
  }
  return entries_[index - entries_.front().index];
}

std::vector<RaftLogEntry> RaftLog::GetRange(uint64_t start_index, uint64_t end_index) const {
  std::scoped_lock lk(mu_);
  std::vector<RaftLogEntry> out;
  if (entries_.empty() || start_index == 0 || end_index < start_index) {
    return out;
  }
  const uint64_t first = entries_.front().index;
  const uint64_t last = entries_.back().index;
  const uint64_t s = std::max(start_index, first);
  const uint64_t e = std::min(end_index, last);
  for (uint64_t i = s; i <= e; ++i) {
    out.push_back(entries_[i - first]);
  }
  return out;
}

uint64_t RaftLog::LastIndex() const {
  std::scoped_lock lk(mu_);
  return entries_.empty() ? 0u : entries_.back().index;
}

uint64_t RaftLog::TermAt(uint64_t index) const {
  std::scoped_lock lk(mu_);
  if (index == 0 || entries_.empty()) {
    return 0u;
  }
  const uint64_t first = entries_.front().index;
  if (index < first || index > entries_.back().index) {
    return 0u;
  }
  return entries_[index - first].term;
}

void RaftLog::TruncateFrom(uint64_t index) {
  std::scoped_lock lk(mu_);
  if (entries_.empty() || index == 0) {
    return;
  }
  const uint64_t first = entries_.front().index;
  if (index > entries_.back().index) {
    return;
  }
  const size_t erase_from = static_cast<size_t>(index - first);
  if (erase_from >= entries_.size()) {
    return;
  }
  entries_.erase(entries_.begin() + static_cast<std::ptrdiff_t>(erase_from), entries_.end());
  RewriteFileFromMemory();
}

void RaftLog::AppendEntries(const std::vector<RaftLogEntry>& entries) {
  bool needs_rewrite = false;
  {
    std::scoped_lock lk(mu_);
    for (const auto& in : entries) {
      if (!entries_.empty()) {
        const uint64_t cur_last = entries_.back().index;
        if (in.index <= cur_last) {
          const size_t off = static_cast<size_t>(in.index - entries_.front().index);
          if (entries_[off].term != in.term) {
            entries_.erase(entries_.begin() + static_cast<std::ptrdiff_t>(off), entries_.end());
            needs_rewrite = true;
          } else {
            continue;
          }
        }
      }
      const uint64_t expect = entries_.empty() ? 1u : (entries_.back().index + 1);
      if (in.index != expect) {
        continue;
      }
      if (needs_rewrite) {
        entries_.push_back(in);
      } else {
        if (!AppendEntryToFile(in)) {
          continue;
        }
        entries_.push_back(in);
      }
    }
    if (needs_rewrite) {
      RewriteFileFromMemory();
      return;
    }
    if (!path_.empty()) {
      (void)SyncFileLocked();
    }
  }
}

void RaftLog::Load() {
  std::scoped_lock lk(mu_);
  entries_.clear();
  if (path_.empty()) {
    return;
  }
  std::ifstream in(path_, std::ios::binary);
  if (!in) {
    return;
  }
  while (true) {
    char hdr[21];
    in.read(hdr, sizeof(hdr));
    if (in.gcount() == 0) {
      break;
    }
    if (in.gcount() != static_cast<std::streamsize>(sizeof(hdr))) {
      break;
    }
    const uint64_t idx = ReadLe64(hdr);
    const uint64_t term = ReadLe64(hdr + 8);
    const auto type = static_cast<RaftEntryType>(static_cast<uint8_t>(hdr[16]));
    const uint32_t len = ReadLe32(hdr + 17);
    std::string payload(len, '\0');
    if (len > 0) {
      in.read(payload.data(), static_cast<std::streamsize>(len));
      if (!in || in.gcount() != static_cast<std::streamsize>(len)) {
        break;
      }
    }
    entries_.push_back(RaftLogEntry{idx, term, type, std::move(payload)});
  }
}

bool RaftLog::EnsureAppendFileOpen() {
  if (path_.empty()) {
    return true;
  }
  if (ofs_.is_open()) {
    return true;
  }
  ofs_.open(path_, std::ios::binary | std::ios::app);
  if (!ofs_) {
    return false;
  }
#ifdef _WIN32
  if (sync_handle_ == INVALID_HANDLE_VALUE) {
    sync_handle_ = CreateFileA(path_.c_str(),
                               GENERIC_WRITE,
                               FILE_SHARE_READ | FILE_SHARE_WRITE,
                               nullptr,
                               OPEN_EXISTING,
                               FILE_ATTRIBUTE_NORMAL,
                               nullptr);
  }
#else
  if (sync_fd_ < 0) {
    sync_fd_ = ::open(path_.c_str(), O_WRONLY);
  }
#endif
  return true;
}

bool RaftLog::SyncFileLocked() {
  if (path_.empty()) {
    return true;
  }
  if (!ofs_.is_open()) {
    return false;
  }
  ofs_.flush();
  if (!ofs_) {
    return false;
  }
#ifdef _WIN32
  if (sync_handle_ == INVALID_HANDLE_VALUE) {
    return false;
  }
  return FlushFileBuffers(sync_handle_) != 0;
#else
  if (sync_fd_ < 0) {
    return false;
  }
  return ::fsync(sync_fd_) == 0;
#endif
}

bool RaftLog::AppendEntryToFile(const RaftLogEntry& entry) {
  if (path_.empty()) {
    return true;
  }
  if (!EnsureAppendFileOpen()) {
    return false;
  }
  const std::string rec = SerializeRaftEntry(entry);
  ofs_.write(rec.data(), static_cast<std::streamsize>(rec.size()));
  return static_cast<bool>(ofs_);
}

bool RaftLog::RewriteFileFromMemory() {
  if (path_.empty()) {
    return true;
  }
  if (ofs_.is_open()) {
    ofs_.flush();
    ofs_.close();
  }
#ifdef _WIN32
  if (sync_handle_ != INVALID_HANDLE_VALUE) {
    CloseHandle(sync_handle_);
    sync_handle_ = INVALID_HANDLE_VALUE;
  }
#else
  if (sync_fd_ >= 0) {
    ::close(sync_fd_);
    sync_fd_ = -1;
  }
#endif
  const std::string tmp = path_ + ".tmp";
  std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
  if (!out) {
    return false;
  }
  for (const auto& e : entries_) {
    const std::string rec = SerializeRaftEntry(e);
    out.write(rec.data(), static_cast<std::streamsize>(rec.size()));
    if (!out) {
      return false;
    }
  }
  out.flush();
  out.close();
#ifdef _WIN32
  {
    HANDLE h = CreateFileA(tmp.c_str(),
                           GENERIC_WRITE,
                           FILE_SHARE_READ | FILE_SHARE_WRITE,
                           nullptr,
                           OPEN_EXISTING,
                           FILE_ATTRIBUTE_NORMAL,
                           nullptr);
    if (h != INVALID_HANDLE_VALUE) {
      FlushFileBuffers(h);
      CloseHandle(h);
    }
  }
#else
  {
    const int fd = ::open(tmp.c_str(), O_WRONLY);
    if (fd >= 0) {
      (void)::fsync(fd);
      ::close(fd);
    }
  }
#endif
  std::error_code ec;
  std::filesystem::rename(tmp, path_, ec);
  if (ec) {
    std::filesystem::remove(path_, ec);
    ec.clear();
    std::filesystem::rename(tmp, path_, ec);
    if (ec) {
      std::filesystem::remove(tmp, ec);
      return false;
    }
  }
  return EnsureAppendFileOpen();
}

}  // namespace txndb
