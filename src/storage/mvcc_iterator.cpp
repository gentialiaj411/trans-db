#include "storage/mvcc_store.h"

namespace txndb {

namespace {

const char kTombstoneByte = '\x00';

bool IsDeleteMarker(std::string_view value) {
  return value.size() == 1 && value[0] == kTombstoneByte;
}

std::string_view SliceToView(const rocksdb::Slice& s) {
  return {s.data(), s.size()};
}

}  // namespace

MVCCIterator::MVCCIterator(rocksdb::DB* db, uint32_t table_id, std::string_view start_pk,
                           std::string_view end_pk_exclusive, uint64_t snapshot_ts,
                           bool open_end_exclusive)
    : db_(db),
      it_(db->NewIterator(rocksdb::ReadOptions())),
      table_id_(table_id),
      start_pk_(start_pk),
      end_pk_exclusive_(end_pk_exclusive),
      snapshot_ts_(snapshot_ts),
      open_end_exclusive_(open_end_exclusive) {
  std::string seek_key = EncodeKeyPrefixNewest(table_id_, start_pk_);
  it_->Seek(seek_key);
  Advance();
}

void MVCCIterator::Next() {
  if (!it_->Valid()) {
    valid_ = false;
    return;
  }
  uint32_t tid = 0;
  std::string pk;
  uint64_t wts = 0;
  if (!DecodeKey(SliceToView(it_->key()), &tid, &pk, &wts) || tid != table_id_) {
    valid_ = false;
    return;
  }
  while (it_->Valid()) {
    std::string_view k = SliceToView(it_->key());
    if (!DecodeKey(k, &tid, &pk, &wts) || tid != table_id_) {
      break;
    }
    if (pk != current_pk_) {
      break;
    }
    it_->Next();
  }
  Advance();
}

void MVCCIterator::Advance() {
  while (it_->Valid()) {
    uint32_t tid = 0;
    std::string pk;
    uint64_t wts = 0;
    std::string_view k = SliceToView(it_->key());
    if (!DecodeKey(k, &tid, &pk, &wts)) {
      it_->Next();
      continue;
    }
    if (tid != table_id_) {
      break;
    }
    if (!open_end_exclusive_ && pk.compare(end_pk_exclusive_) >= 0) {
      break;
    }
    if (!start_pk_.empty() && pk.compare(start_pk_) < 0) {
      it_->Next();
      continue;
    }

    bool found_visible = false;
    std::string chosen_val;
    uint64_t chosen_ts = 0;

    while (it_->Valid()) {
      std::string_view k2 = SliceToView(it_->key());
      uint32_t t2 = 0;
      std::string pk2;
      uint64_t w2 = 0;
      if (!DecodeKey(k2, &t2, &pk2, &w2) || t2 != table_id_ || pk2 != pk) {
        break;
      }
      if (w2 <= snapshot_ts_) {
        std::string_view v = SliceToView(it_->value());
        found_visible = true;
        chosen_ts = w2;
        chosen_val.assign(v.data(), v.size());
        break;
      }
      it_->Next();
    }

    if (!found_visible) {
      continue;
    }

    if (IsDeleteMarker(chosen_val)) {
      while (it_->Valid()) {
        std::string_view k3 = SliceToView(it_->key());
        uint32_t t3 = 0;
        std::string pk3;
        uint64_t w3 = 0;
        if (!DecodeKey(k3, &t3, &pk3, &w3) || t3 != table_id_ || pk3 != pk) {
          break;
        }
        it_->Next();
      }
      continue;
    }

    current_pk_ = pk;
    current_value_.swap(chosen_val);
    current_write_ts_ = chosen_ts;
    valid_ = true;

    while (it_->Valid()) {
      std::string_view k4 = SliceToView(it_->key());
      uint32_t t4 = 0;
      std::string pk4;
      uint64_t w4 = 0;
      if (!DecodeKey(k4, &t4, &pk4, &w4) || t4 != table_id_ || pk4 != pk) {
        break;
      }
      it_->Next();
    }
    return;
  }

  valid_ = false;
}

}  // namespace txndb
