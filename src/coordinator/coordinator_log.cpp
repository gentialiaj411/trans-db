#include "coordinator/coordinator_log.h"
#include "txn/group_commit.h"

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string_view>
#include <vector>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {

constexpr uint32_t kCrc32Poly = 0xEDB88320;

std::array<uint32_t, 256> MakeCrc32Table() {
  std::array<uint32_t, 256> tbl{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) {
      c = (c & 1U) ? (kCrc32Poly ^ (c >> 1)) : (c >> 1);
    }
    tbl[i] = c;
  }
  return tbl;
}

const std::array<uint32_t, 256> kCrcTbl = MakeCrc32Table();

uint32_t Crc32Ieee(std::string_view data) {
  uint32_t c = UINT32_MAX;
  for (unsigned char uc : data) {
    c = kCrcTbl[(c ^ uc) & 0xFFu] ^ (c >> 8);
  }
  return UINT32_MAX ^ c;
}

void AppendLe32(std::string* dst, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    dst->push_back(static_cast<char>(v & 0xffu));
    v >>= 8;
  }
}

void AppendLe64(std::string* dst, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    dst->push_back(static_cast<char>(v & 0xffu));
    v >>= 8;
  }
}

txndb::Status DurableFileSync(std::ofstream* ofs
#ifdef _WIN32
                              , HANDLE sync_handle
#else
                              , int sync_fd
#endif
) {
  ofs->flush();
  if (!(*ofs)) {
    return txndb::Status::IOError("CoordinatorLog flush failed");
  }
#ifdef _WIN32
  if (sync_handle == INVALID_HANDLE_VALUE) {
    return txndb::Status::IOError("CoordinatorLog sync handle unavailable");
  }
  if (!FlushFileBuffers(sync_handle)) {
    return txndb::Status::IOError("CoordinatorLog FlushFileBuffers failed");
  }
#else
  if (sync_fd < 0) {
    return txndb::Status::IOError("CoordinatorLog sync fd unavailable");
  }
  if (::fsync(sync_fd) != 0) {
    return txndb::Status::IOError("CoordinatorLog fsync failed");
  }
#endif
  return txndb::Status::OK();
}

}  // namespace

namespace txndb {

Status CoordinatorLog::Open(const std::string& path, std::unique_ptr<CoordinatorLog>* out) {
  auto log = std::unique_ptr<CoordinatorLog>(new CoordinatorLog());
  log->path_ = path;

  log->ofs_.open(path, std::ios::binary | std::ios::app);
  if (!log->ofs_) {
    return Status::IOError("CoordinatorLog open failed");
  }
#ifdef _WIN32
  log->sync_handle_ = CreateFileA(path.c_str(),
                                  GENERIC_WRITE,
                                  FILE_SHARE_READ | FILE_SHARE_WRITE,
                                  nullptr,
                                  OPEN_ALWAYS,
                                  FILE_ATTRIBUTE_NORMAL,
                                  nullptr);
#else
  log->sync_fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
#endif
  *out = std::move(log);
  return Status::OK();
}

CoordinatorLog::~CoordinatorLog() {
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
}

Status CoordinatorLog::WriteRecord(const CoordinatorLogRecord& record) {
  std::string body;
  body.reserve(8 + 8 + 1 + 4 + (record.shards.size() * 4) + 4);
  AppendLe64(&body, record.txn_id);
  AppendLe64(&body, record.timestamp_us);
  body.push_back(static_cast<char>(record.type));
  AppendLe32(&body, static_cast<uint32_t>(record.shards.size()));
  for (uint32_t sid : record.shards) {
    AppendLe32(&body, sid);
  }
  AppendLe32(&body, Crc32Ieee(body));

  const uint32_t rec_len = static_cast<uint32_t>(body.size());
  char len_le[4];
  for (int i = 0; i < 4; ++i) {
    len_le[i] = static_cast<char>((rec_len >> (8 * i)) & 0xff);
  }
  ofs_.write(len_le, sizeof(len_le));
  ofs_.write(body.data(), static_cast<std::streamsize>(body.size()));
  if (!ofs_) {
    return Status::IOError("CoordinatorLog write failed");
  }
  return Status::OK();
}

Status CoordinatorLog::SyncLocked() {
  return DurableFileSync(&ofs_
#ifdef _WIN32
                         , sync_handle_
#else
                         , sync_fd_
#endif
  );
}

Status CoordinatorLog::AppendWrite(const CoordinatorLogRecord& record) {
  std::scoped_lock lk(mu_);
  return WriteRecord(record);
}

Status CoordinatorLog::Flush() {
  return DurableSyncRegistry::Instance().QueueSync(this, [this]() {
    std::scoped_lock lk(mu_);
    return SyncLocked();
  });
}

Status CoordinatorLog::Append(const CoordinatorLogRecord& record) {
  Status ws = AppendWrite(record);
  if (!ws.ok()) {
    return ws;
  }
  return Flush();
}

Status CoordinatorLog::Replay(const std::function<void(const CoordinatorLogRecord&)>& visitor) {
  std::scoped_lock lk(mu_);
  ofs_.flush();

  std::error_code ec;
  const auto sz = std::filesystem::file_size(path_, ec);
  if (ec || sz == 0) {
    return Status::OK();
  }

  std::vector<uint8_t> buf(sz);
  std::ifstream in(path_, std::ios::binary);
  if (!in) {
    return Status::IOError("CoordinatorLog replay open failed");
  }
  in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(sz));
  if (!in) {
    return Status::IOError("CoordinatorLog replay read failed");
  }

  size_t off = 0;
  while (off + sizeof(uint32_t) <= buf.size()) {
    uint32_t rec_len = 0;
    std::memcpy(&rec_len, buf.data() + off, sizeof(uint32_t));
    off += sizeof(uint32_t);

    if (rec_len < (8 + 8 + 1 + 4 + 4) || off + rec_len > buf.size()) {
      break;
    }
    std::string_view body(reinterpret_cast<const char*>(buf.data() + off), rec_len);
    off += rec_len;

    uint32_t stored_crc = 0;
    std::memcpy(&stored_crc, body.data() + body.size() - 4, 4);
    std::string_view crc_region(body.data(), body.size() - 4);
    if (Crc32Ieee(crc_region) != stored_crc) {
      break;
    }

    CoordinatorLogRecord rec;
    size_t pos = 0;
    std::memcpy(&rec.txn_id, body.data() + pos, 8);
    pos += 8;
    std::memcpy(&rec.timestamp_us, body.data() + pos, 8);
    pos += 8;
    rec.type = static_cast<CoordinatorLogRecordType>(
        static_cast<uint8_t>(body[pos]));
    pos += 1;
    uint32_t shard_count = 0;
    std::memcpy(&shard_count, body.data() + pos, 4);
    pos += 4;

    if (pos + (static_cast<size_t>(shard_count) * 4) + 4 != body.size()) {
      break;
    }
    rec.shards.reserve(shard_count);
    for (uint32_t i = 0; i < shard_count; ++i) {
      uint32_t sid = 0;
      std::memcpy(&sid, body.data() + pos, 4);
      pos += 4;
      rec.shards.push_back(sid);
    }

    if (visitor) {
      visitor(rec);
    }
  }
  return Status::OK();
}

}  // namespace txndb
