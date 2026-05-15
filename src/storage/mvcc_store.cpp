#include "storage/mvcc_store.h"

namespace txndb {

namespace {

const char kTombstone[] = {'\x00'};

}  // namespace

bool MVCCStore::IsTombstone(std::string_view value) {
  return value.size() == 1 && value[0] == kTombstone[0];
}

Status MVCCStore::Open(std::string_view path, std::unique_ptr<MVCCStore>* out) {
  rocksdb::Options options;
  options.create_if_missing = true;

  std::string p(path);
  std::unique_ptr<rocksdb::DB> raw;
  rocksdb::Status rs = rocksdb::DB::Open(options, p, &raw);
  if (!rs.ok()) {
    return Status::IOError(rs.ToString());
  }
  out->reset(new MVCCStore(std::move(raw)));
  return Status::OK();
}

MVCCStore::~MVCCStore() = default;

Status MVCCStore::Put(uint32_t table_id, std::string_view key, uint64_t write_ts,
                      std::string_view value) {
  std::string enc = EncodeKey(table_id, key, write_ts);
  rocksdb::Status rs =
      db_->Put(rocksdb::WriteOptions(), enc, rocksdb::Slice(value.data(), value.size()));
  if (!rs.ok()) {
    return Status::IOError(rs.ToString());
  }
  return Status::OK();
}

Status MVCCStore::Delete(uint32_t table_id, std::string_view key, uint64_t write_ts) {
  std::string enc = EncodeKey(table_id, key, write_ts);
  rocksdb::Slice tomb(kTombstone, 1);
  rocksdb::Status rs = db_->Put(rocksdb::WriteOptions(), enc, tomb);
  if (!rs.ok()) {
    return Status::IOError(rs.ToString());
  }
  return Status::OK();
}

static std::string_view SliceToView(const rocksdb::Slice& s) {
  return {s.data(), s.size()};
}

Status MVCCStore::Get(uint32_t table_id, std::string_view key, uint64_t snapshot_ts,
                      std::string* value, uint64_t* value_ts) {
  std::string start = EncodeKeyPrefixNewest(table_id, key);
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
  it->Seek(start);
  while (it->Valid()) {
    uint32_t tid;
    std::string pk;
    uint64_t wts;
    if (!DecodeKey(SliceToView(it->key()), &tid, &pk, &wts)) {
      break;
    }
    if (tid != table_id || pk != key) {
      break;
    }
    if (wts <= snapshot_ts) {
      std::string_view v = SliceToView(it->value());
      if (IsTombstone(v)) {
        return Status::NotFound();
      }
      *value = std::string(v);
      *value_ts = wts;
      return Status::OK();
    }
    it->Next();
  }
  return Status::NotFound();
}

std::unique_ptr<MVCCIterator> MVCCStore::Scan(uint32_t table_id, std::string_view start,
                                              std::string_view end_exclusive,
                                              uint64_t snapshot_ts, bool open_end_exclusive) {
  return std::make_unique<MVCCIterator>(db_.get(), table_id, start, end_exclusive, snapshot_ts,
                                        open_end_exclusive);
}

Status MVCCStore::AcquireLock(uint32_t /*table_id*/, std::string_view /*key*/,
                              uint64_t /*txn_id*/, std::chrono::milliseconds /*timeout*/) {
  return Status::OK();
}

Status MVCCStore::ReleaseLock(uint32_t /*table_id*/, std::string_view /*key*/,
                              uint64_t /*txn_id*/) {
  return Status::OK();
}

Status MVCCStore::ReplayPut(uint32_t table_id, std::string_view key, uint64_t write_ts,
                            std::string_view value) {
  return Put(table_id, key, write_ts, value);
}

}  // namespace txndb
