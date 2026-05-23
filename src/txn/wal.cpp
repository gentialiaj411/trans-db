#include "txn/wal.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
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

std::vector<uint8_t> ReadWholeFileBinary(const std::string& path, txndb::Status* stat) {
  std::error_code ec;
  const auto sz = std::filesystem::file_size(path, ec);
  if (ec) {
    *stat = txndb::Status::OK();
    return {};
  }
  std::vector<uint8_t> buf(sz);
  if (sz == 0) {
    *stat = txndb::Status::OK();
    return buf;
  }
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    *stat = txndb::Status::IOError("WAL replay open failed");
    return {};
  }
  in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(sz));
  if (!in) {
    *stat = txndb::Status::IOError("WAL replay read failed");
    return {};
  }
  *stat = txndb::Status::OK();
  return buf;
}

bool ParseRecordsFromBytes(const uint8_t* data, size_t nbytes,
                           std::function<void(const txndb::WALRecord&)> visitor, uint64_t* max_lsn,
                           txndb::Status* err) {
  *max_lsn = 0;
  size_t off = 0;
  while (off + sizeof(uint32_t) <= nbytes) {
    uint32_t reclen = 0;
    std::memcpy(&reclen, data + off, sizeof(uint32_t));
    off += sizeof(uint32_t);

    const size_t need = reclen;
    if (need < 8 + 8 + 1 + sizeof(uint32_t) + sizeof(uint32_t)) {
      break;
    }
    if (off + need > nbytes) {
      break;
    }

    std::string_view chunk(reinterpret_cast<const char*>(data + off), need);
    std::string_view body = chunk;
    off += need;

    uint64_t lsn = 0;
    uint64_t txn_id = 0;
    std::memcpy(&lsn, body.data(), sizeof(uint64_t));
    std::memcpy(&txn_id, body.data() + 8, sizeof(uint64_t));
    uint8_t type_byte = static_cast<uint8_t>(body[16]);
    uint32_t payload_len = 0;
    std::memcpy(&payload_len, body.data() + 17, sizeof(uint32_t));

    constexpr size_t kMinBody = 8 + 8 + 1 + 4 + 4;
    if (body.size() < kMinBody) {
      break;
    }
    if (payload_len != body.size() - kMinBody) {
      break;
    }
    std::string_view payload_sv(body.data() + 21, payload_len);

    uint32_t stored_crc = 0;
    std::memcpy(&stored_crc,
                body.data() + body.size() - sizeof(uint32_t),
                sizeof(uint32_t));

    const std::string_view crc_calc_region(body.data(), body.size() - sizeof(uint32_t));
    if (Crc32Ieee(crc_calc_region) != stored_crc) {
      break;
    }

    txndb::WALRecord wr;
    wr.lsn = lsn;
    wr.txn_id = txn_id;
    wr.type = static_cast<txndb::WALRecordType>(type_byte);
    wr.payload.assign(payload_sv.data(), payload_sv.size());

    *max_lsn = std::max(*max_lsn, lsn);
    if (visitor) {
      visitor(wr);
    }
  }
  *err = txndb::Status::OK();
  return true;
}

}  // namespace

namespace txndb {

namespace {

Status DurableFileSync(std::ofstream* ofs
#ifdef _WIN32
                       , HANDLE sync_handle
#else
                       , int sync_fd
#endif
) {
  ofs->flush();
  if (!(*ofs)) {
    return Status::IOError("WAL flush failed");
  }
#ifdef _WIN32
  if (sync_handle == INVALID_HANDLE_VALUE) {
    return Status::IOError("WAL sync handle unavailable");
  }
  if (!FlushFileBuffers(sync_handle)) {
    return Status::IOError("WAL FlushFileBuffers failed");
  }
#else
  if (sync_fd < 0) {
    return Status::IOError("WAL sync fd unavailable");
  }
  if (::fsync(sync_fd) != 0) {
    return Status::IOError("WAL fsync failed");
  }
#endif
  return Status::OK();
}

}  // namespace

void WAL::AppendFixedU8(std::string* dst, uint8_t v) {
  dst->push_back(static_cast<char>(v));
}

void WAL::AppendFixedLe32(std::string* dst, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    dst->push_back(static_cast<char>(v & 0xffu));
    v >>= 8;
  }
}

void WAL::AppendFixedLe64(std::string* dst, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    dst->push_back(static_cast<char>(v & 0xffu));
    v >>= 8;
  }
}

bool WAL::ConsumeLe32(std::string_view* data, uint32_t* out) {
  if (data->size() < 4) {
    return false;
  }
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) {
    v |= static_cast<uint32_t>(static_cast<unsigned char>((*data)[i])) << (8 * i);
  }
  *data = data->substr(4);
  *out = v;
  return true;
}

bool WAL::ConsumeLe64(std::string_view* data, uint64_t* out) {
  if (data->size() < 8) {
    return false;
  }
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= static_cast<uint64_t>(static_cast<unsigned char>((*data)[i])) << (8 * i);
  }
  *data = data->substr(8);
  *out = v;
  return true;
}

Status WAL::Open(std::string_view pathv, std::unique_ptr<WAL>* out) {
  std::string path(pathv);

  auto wal = std::unique_ptr<WAL>(new WAL());
  wal->path_ = path;

  uint64_t max_lsn = 0;
  {
    Status st = Status::OK();
    auto bytes = ReadWholeFileBinary(path, &st);
    if (!st.ok()) {
      return st;
    }
    if (!bytes.empty()) {
      Status parse_err = Status::OK();
      ParseRecordsFromBytes(bytes.data(), bytes.size(), nullptr, &max_lsn, &parse_err);
      if (!parse_err.ok()) {
        return parse_err;
      }
    }
    wal->next_lsn_ = max_lsn + 1;
  }

  wal->ofs_.open(path, std::ios::binary | std::ios::app);
  if (!wal->ofs_) {
    return Status::IOError("WAL open for append failed");
  }
#ifdef _WIN32
  wal->sync_handle_ = CreateFileA(
      path.c_str(),
      GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_WRITE,
      nullptr,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL,
      nullptr);
#else
  wal->sync_fd_ = ::open(path.c_str(), O_WRONLY);
#endif

  *out = std::move(wal);
  return Status::OK();
}

WAL::~WAL() {
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

Status WAL::WriteRecord(uint64_t lsn, uint64_t txn_id, WALRecordType type,
                        std::string_view payload) {
  std::string body;
  body.reserve(8 + 8 + 1 + 4 + payload.size() + 4);
  AppendFixedLe64(&body, lsn);
  AppendFixedLe64(&body, txn_id);
  AppendFixedU8(&body, static_cast<uint8_t>(type));
  AppendFixedLe32(&body, static_cast<uint32_t>(payload.size()));
  body.append(payload.data(), payload.size());

  const uint32_t crc = Crc32Ieee(body);
  AppendFixedLe32(&body, crc);

  const uint32_t reclen = static_cast<uint32_t>(body.size());
  char len_le[4];
  for (int i = 0; i < 4; ++i) {
    len_le[i] = static_cast<char>((reclen >> (8 * i)) & 0xff);
  }
  ofs_.write(len_le, sizeof(len_le));
  ofs_.write(body.data(), static_cast<std::streamsize>(body.size()));
  if (!ofs_) {
    return Status::IOError("WAL write failed");
  }
  return Status::OK();
}

Status WAL::Append(uint64_t txn_id, WALRecordType type, std::string_view payload) {
  std::scoped_lock lk(mu_);
  const uint64_t lsn = next_lsn_++;
  return WriteRecord(lsn, txn_id, type, payload);
}

Status WAL::Sync() {
  std::scoped_lock lk(mu_);
  return DurableFileSync(&ofs_
#ifdef _WIN32
                         , sync_handle_
#else
                         , sync_fd_
#endif
  );
}

Status WAL::AppendSync(uint64_t txn_id, WALRecordType type, std::string_view payload) {
  Status s = Append(txn_id, type, payload);
  if (!s.ok()) {
    return s;
  }
  return Sync();
}

Status WAL::Replay(std::function<void(const WALRecord&)> visitor) {
  std::scoped_lock lk(mu_);
  if (ofs_.is_open()) {
    ofs_.flush();
  }
  Status st = Status::OK();
  auto bytes = ReadWholeFileBinary(path_, &st);
  if (!st.ok()) {
    return st;
  }
  uint64_t max_lsn = 0;
  if (!bytes.empty()) {
    Status parse_err = Status::OK();
    ParseRecordsFromBytes(bytes.data(), bytes.size(), visitor, &max_lsn, &parse_err);
    if (!parse_err.ok()) {
      return parse_err;
    }
  }
  next_lsn_ = max_lsn + 1;
  return Status::OK();
}

Status WAL::Truncate() {
  std::scoped_lock lk(mu_);
  if (ofs_.is_open()) {
    ofs_.flush();
    ofs_.close();
  }
  std::error_code ec;
  std::filesystem::resize_file(path_, 0, ec);
  if (ec) {
    return Status::IOError(ec.message());
  }
  next_lsn_ = 1;
  ofs_.open(path_, std::ios::binary | std::ios::app);
  if (!ofs_) {
    return Status::IOError("WAL reopen after truncate failed");
  }
#ifdef _WIN32
  if (sync_handle_ != INVALID_HANDLE_VALUE) {
    CloseHandle(sync_handle_);
    sync_handle_ = INVALID_HANDLE_VALUE;
  }
  sync_handle_ = CreateFileA(
      path_.c_str(),
      GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_WRITE,
      nullptr,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL,
      nullptr);
#else
  if (sync_fd_ >= 0) {
    ::close(sync_fd_);
    sync_fd_ = -1;
  }
  sync_fd_ = ::open(path_.c_str(), O_WRONLY);
#endif
  return Status::OK();
}

std::string WAL::SerializeBeginPayload(uint64_t snapshot_ts) {
  std::string s;
  s.reserve(8);
  AppendFixedLe64(&s, snapshot_ts);
  return s;
}

std::string WAL::SerializeWritePayload(uint32_t table_id, std::string_view key, std::string_view value,
                                      uint64_t write_ts) {
  std::string s;
  s.reserve(4 + 4 + key.size() + 4 + value.size() + 8);
  AppendFixedLe32(&s, table_id);
  AppendFixedLe32(&s, static_cast<uint32_t>(key.size()));
  s.append(key.data(), key.size());
  AppendFixedLe32(&s, static_cast<uint32_t>(value.size()));
  s.append(value.data(), value.size());
  AppendFixedLe64(&s, write_ts);
  return s;
}

std::string WAL::SerializeDeletePayload(uint32_t table_id, std::string_view key, uint64_t write_ts) {
  std::string s;
  s.reserve(4 + 4 + key.size() + 8);
  AppendFixedLe32(&s, table_id);
  AppendFixedLe32(&s, static_cast<uint32_t>(key.size()));
  s.append(key.data(), key.size());
  AppendFixedLe64(&s, write_ts);
  return s;
}

std::string WAL::SerializePreparePayload(uint64_t commit_ts) {
  std::string s;
  AppendFixedLe64(&s, commit_ts);
  return s;
}

std::string WAL::SerializeCommitPayload(uint64_t commit_ts) {
  std::string s;
  AppendFixedLe64(&s, commit_ts);
  return s;
}

bool WAL::DeserializeBeginPayload(std::string_view payload, BeginInfo* out) {
  if (payload.size() != sizeof(uint64_t)) {
    return false;
  }
  std::string_view pv = payload;
  return ConsumeLe64(&pv, &out->snapshot_ts) && pv.empty();
}

bool WAL::DeserializeWritePayload(std::string_view payload, WriteInfo* out) {
  std::string_view pv = payload;
  uint32_t tid = 0;
  uint32_t klen = 0;
  uint32_t vlen = 0;
  uint64_t wts = 0;
  if (!ConsumeLe32(&pv, &tid)) {
    return false;
  }
  if (!ConsumeLe32(&pv, &klen)) {
    return false;
  }
  if (pv.size() < klen) {
    return false;
  }
  out->key.assign(pv.data(), klen);
  pv.remove_prefix(klen);
  if (!ConsumeLe32(&pv, &vlen)) {
    return false;
  }
  if (pv.size() < vlen + sizeof(uint64_t)) {
    return false;
  }
  out->value.assign(pv.data(), vlen);
  pv.remove_prefix(vlen);
  if (!ConsumeLe64(&pv, &wts) || !pv.empty()) {
    return false;
  }
  out->table_id = tid;
  out->write_ts = wts;
  return true;
}

bool WAL::DeserializeDeletePayload(std::string_view payload, DeleteInfo* out) {
  std::string_view pv = payload;
  uint32_t tid = 0;
  uint32_t klen = 0;
  uint64_t wts = 0;
  if (!ConsumeLe32(&pv, &tid)) {
    return false;
  }
  if (!ConsumeLe32(&pv, &klen)) {
    return false;
  }
  if (pv.size() < klen + sizeof(uint64_t)) {
    return false;
  }
  out->key.assign(pv.data(), klen);
  pv.remove_prefix(klen);
  if (!ConsumeLe64(&pv, &wts) || !pv.empty()) {
    return false;
  }
  out->table_id = tid;
  out->write_ts = wts;
  return true;
}

bool WAL::DeserializePreparePayload(std::string_view payload, PrepareInfo* out) {
  std::string_view pv = payload;
  return ConsumeLe64(&pv, &out->commit_ts) && pv.empty();
}

bool WAL::DeserializeCommitPayload(std::string_view payload, CommitInfo* out) {
  std::string_view pv = payload;
  return ConsumeLe64(&pv, &out->commit_ts) && pv.empty();
}

}  // namespace txndb
