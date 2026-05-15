#pragma once

#include "storage/key_encoding.h"
#include "storage/status.h"

#include <rocksdb/db.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace txndb {

class MVCCStore;

class MVCCIterator {
public:
  MVCCIterator(rocksdb::DB* db, uint32_t table_id, std::string_view start_pk,
               std::string_view end_pk_exclusive, uint64_t snapshot_ts, bool open_end_exclusive);

  bool Valid() const { return valid_; }

  std::string_view PrimaryKey() const { return current_pk_; }

  std::string_view Value() const { return current_value_; }

  uint64_t ValueTimestamp() const { return current_write_ts_; }

  void Next();

private:
  void Advance();

  rocksdb::DB* db_;
  std::unique_ptr<rocksdb::Iterator> it_;
  uint32_t table_id_;
  std::string start_pk_;
  std::string end_pk_exclusive_;
  uint64_t snapshot_ts_;
  bool open_end_exclusive_{false};

  bool valid_{false};
  std::string current_pk_;
  std::string current_value_;
  uint64_t current_write_ts_{0};
};

class MVCCStore {
public:
  static Status Open(std::string_view path, std::unique_ptr<MVCCStore>* out);

  ~MVCCStore();

  MVCCStore(const MVCCStore&) = delete;
  MVCCStore& operator=(const MVCCStore&) = delete;

  Status Put(uint32_t table_id, std::string_view key, uint64_t write_ts, std::string_view value);

  Status Delete(uint32_t table_id, std::string_view key, uint64_t write_ts);

  Status Get(uint32_t table_id, std::string_view key, uint64_t snapshot_ts, std::string* value,
             uint64_t* value_ts);

  std::unique_ptr<MVCCIterator> Scan(uint32_t table_id, std::string_view start,
                                     std::string_view end_exclusive, uint64_t snapshot_ts,
                                     bool open_end_exclusive = false);

  Status AcquireLock(uint32_t table_id, std::string_view key, uint64_t txn_id,
                     std::chrono::milliseconds timeout);

  Status ReleaseLock(uint32_t table_id, std::string_view key, uint64_t txn_id);

  Status ReplayPut(uint32_t table_id, std::string_view key, uint64_t write_ts,
                   std::string_view value);

  rocksdb::DB* raw_db() { return db_.get(); }

private:
  explicit MVCCStore(std::unique_ptr<rocksdb::DB> db) : db_(std::move(db)) {}

  static bool IsTombstone(std::string_view value);

  std::unique_ptr<rocksdb::DB> db_;
};

}  // namespace txndb
