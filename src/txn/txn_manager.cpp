#include "txn/txn_manager.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace txndb {

namespace {

const BufferedWrite* LatestBuffered(const std::vector<BufferedWrite>& writes, uint32_t table_id,
                                    std::string_view key) {
  for (auto it = writes.rbegin(); it != writes.rend(); ++it) {
    if (it->table_id == table_id && it->key == key) {
      return &*it;
    }
  }
  return nullptr;
}

}  // namespace

TxnManager::TxnManager(MVCCStore* store, WAL* wal, LockManager* lock_mgr)
    : store_(store), wal_(wal), lock_mgr_(lock_mgr) {}

Transaction* TxnManager::GetTxn(uint64_t txn_id) {
  std::scoped_lock lk(mu_);
  auto it = txns_.find(txn_id);
  if (it == txns_.end()) {
    return nullptr;
  }
  return it->second.get();
}

uint64_t TxnManager::Begin(uint64_t snapshot_ts) {
  const uint64_t txn_id = next_txn_id_.fetch_add(1, std::memory_order_relaxed);
  auto txn = std::make_unique<Transaction>();
  txn->txn_id = txn_id;
  txn->snapshot_ts = snapshot_ts;
  txn->prepare_commit_ts = 0;
  txn->state = TxnState::ACTIVE;
  wal_->Append(txn_id, WALRecordType::BEGIN, WAL::SerializeBeginPayload(snapshot_ts));

  std::scoped_lock lk(mu_);
  txns_[txn_id] = std::move(txn);
  return txn_id;
}

Status TxnManager::Read(uint64_t txn_id, uint32_t table_id, std::string_view key,
                        std::string* value) {
  uint64_t snap = 0;
  std::vector<BufferedWrite> writes_copy;
  {
    std::scoped_lock lk(mu_);
    auto it = txns_.find(txn_id);
    if (it == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    if (it->second->state != TxnState::ACTIVE) {
      return Status::Error(StatusCode::InvalidArgument, "txn not active");
    }
    snap = it->second->snapshot_ts;
    writes_copy = it->second->write_set;
  }

  if (const BufferedWrite* bw = LatestBuffered(writes_copy, table_id, key)) {
    if (bw->is_delete) {
      return Status::NotFound();
    }
    *value = bw->value;
    return Status::OK();
  }

  std::string mv;
  uint64_t value_ts = 0;
  Status st = store_->Get(table_id, key, snap, &mv, &value_ts);
  if (!st.ok()) {
    return st;
  }

  std::scoped_lock lk(mu_);
  auto it = txns_.find(txn_id);
  if (it == txns_.end() || it->second->state != TxnState::ACTIVE) {
    return Status::Error(StatusCode::InvalidArgument, "txn inactive");
  }
  it->second->read_set.push_back(ReadEntry{table_id, std::string(key), value_ts});
  *value = std::move(mv);
  return Status::OK();
}

Status TxnManager::Scan(uint64_t txn_id, uint32_t table_id,
                        std::string_view range_start_pk, std::string_view range_end_exclusive,
                        bool range_end_open, std::vector<std::pair<std::string, std::string>>* rows_out) {
  rows_out->clear();

  uint64_t snap = 0;
  std::vector<BufferedWrite> writes_copy;
  {
    std::scoped_lock lk(mu_);
    auto it = txns_.find(txn_id);
    if (it == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    if (it->second->state != TxnState::ACTIVE) {
      return Status::Error(StatusCode::InvalidArgument, "txn not active");
    }
    snap = it->second->snapshot_ts;
    writes_copy = it->second->write_set;
  }

  auto scan_it =
      store_->Scan(table_id, range_start_pk, range_end_open ? "" : range_end_exclusive, snap,
                   range_end_open);

  while (scan_it && scan_it->Valid()) {
    std::string pk(scan_it->PrimaryKey());
    const uint64_t value_ts = scan_it->ValueTimestamp();
    std::string mv(scan_it->Value());
    scan_it->Next();

    if (const BufferedWrite* bw = LatestBuffered(writes_copy, table_id, pk)) {
      if (bw->is_delete) {
        continue;
      }
      rows_out->emplace_back(std::move(pk), bw->value);
      continue;
    }

    {
      std::scoped_lock lk(mu_);
      auto it = txns_.find(txn_id);
      if (it == txns_.end() || it->second->state != TxnState::ACTIVE) {
        return Status::Error(StatusCode::InvalidArgument, "txn inactive");
      }
      it->second->read_set.push_back(ReadEntry{table_id, pk, value_ts});
    }
    rows_out->emplace_back(std::move(pk), std::move(mv));
  }

  return Status::OK();
}

Status TxnManager::Write(uint64_t txn_id, uint32_t table_id, std::string_view key,
                         std::string_view value) {
  Status acq = lock_mgr_->Acquire(table_id, key, txn_id);
  if (!acq.ok()) {
    Abort(txn_id);
    return acq;
  }

  std::unique_lock<std::mutex> lk(mu_);
  auto it = txns_.find(txn_id);
  if (it == txns_.end()) {
    lk.unlock();
    lock_mgr_->ReleaseAll(txn_id);
    return Status::Error(StatusCode::InvalidArgument, "unknown txn");
  }
  Transaction* txn = it->second.get();
  if (txn->state != TxnState::ACTIVE) {
    lk.unlock();
    lock_mgr_->ReleaseAll(txn_id);
    return Status::Error(StatusCode::InvalidArgument, "txn not active");
  }

  txn->locked_keys.emplace_back(table_id, std::string(key));
  wal_->Append(txn_id, WALRecordType::WRITE,
               WAL::SerializeWritePayload(table_id, key, value, 0));

  BufferedWrite bw;
  bw.table_id = table_id;
  bw.key.assign(key.data(), key.size());
  bw.value.assign(value.data(), value.size());
  bw.is_delete = false;
  txn->write_set.push_back(std::move(bw));
  return Status::OK();
}

Status TxnManager::Delete(uint64_t txn_id, uint32_t table_id, std::string_view key) {
  Status acq = lock_mgr_->Acquire(table_id, key, txn_id);
  if (!acq.ok()) {
    Abort(txn_id);
    return acq;
  }

  std::unique_lock<std::mutex> lk(mu_);
  auto it = txns_.find(txn_id);
  if (it == txns_.end()) {
    lk.unlock();
    lock_mgr_->ReleaseAll(txn_id);
    return Status::Error(StatusCode::InvalidArgument, "unknown txn");
  }
  Transaction* txn = it->second.get();
  if (txn->state != TxnState::ACTIVE) {
    lk.unlock();
    lock_mgr_->ReleaseAll(txn_id);
    return Status::Error(StatusCode::InvalidArgument, "txn not active");
  }

  txn->locked_keys.emplace_back(table_id, std::string(key));
  wal_->Append(txn_id, WALRecordType::DELETE, WAL::SerializeDeletePayload(table_id, key, 0));

  BufferedWrite bw;
  bw.table_id = table_id;
  bw.key.assign(key.data(), key.size());
  bw.value.clear();
  bw.is_delete = true;
  txn->write_set.push_back(std::move(bw));
  return Status::OK();
}

Status TxnManager::ValidateReadSet(const Transaction& txn, uint64_t commit_ts) {
  for (const auto& r : txn.read_set) {
    std::string cur;
    uint64_t ts = 0;
    Status st = store_->Get(r.table_id, r.key, commit_ts, &cur, &ts);
    if (!st.ok()) {
      return Status::Error(StatusCode::Conflict, "read validation failed");
    }
    if (ts != r.observed_write_ts) {
      return Status::Error(StatusCode::Conflict, "read validation failed");
    }
  }
  return Status::OK();
}

void TxnManager::ApplyWrites(Transaction& txn, uint64_t commit_ts) {
  for (const auto& w : txn.write_set) {
    if (w.is_delete) {
      store_->Delete(w.table_id, w.key, commit_ts);
    } else {
      store_->Put(w.table_id, w.key, commit_ts, w.value);
    }
  }
}

Status TxnManager::Prepare(uint64_t txn_id, uint64_t commit_ts) {
  std::unique_ptr<Transaction> removed;
  {
    std::scoped_lock lk(mu_);
    auto it = txns_.find(txn_id);
    if (it == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    Transaction* txn = it->second.get();
    if (txn->state != TxnState::ACTIVE) {
      return Status::Error(StatusCode::InvalidArgument, "txn not active");
    }

    Status v = ValidateReadSet(*txn, commit_ts);
    if (!v.ok()) {
      removed = std::move(it->second);
      txns_.erase(it);
    } else {
      Status ws =
          wal_->AppendSync(txn_id, WALRecordType::PREPARE, WAL::SerializePreparePayload(commit_ts));
      if (!ws.ok()) {
        return ws;
      }
      txn->state = TxnState::PREPARED;
      txn->prepare_commit_ts = commit_ts;
      return Status::OK();
    }
  }

  wal_->AppendSync(txn_id, WALRecordType::ABORT, "");
  lock_mgr_->ReleaseAll(txn_id);
  return Status::Error(StatusCode::Conflict, "read validation failed");
}

Status TxnManager::Commit(uint64_t txn_id, uint64_t commit_ts) {
  std::unique_ptr<Transaction> txn;
  {
    std::scoped_lock lk(mu_);
    auto it = txns_.find(txn_id);
    if (it == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    if (it->second->state != TxnState::PREPARED) {
      return Status::Error(StatusCode::InvalidArgument, "txn not prepared");
    }
    txn = std::move(it->second);
    txns_.erase(it);
  }

  ApplyWrites(*txn, commit_ts);
  Status ws = wal_->AppendSync(txn_id, WALRecordType::COMMIT, WAL::SerializeCommitPayload(commit_ts));
  if (!ws.ok()) {
    return ws;
  }
  lock_mgr_->ReleaseAll(txn_id);
  return Status::OK();
}

Status TxnManager::Abort(uint64_t txn_id) {
  std::unique_ptr<Transaction> txn;
  {
    std::scoped_lock lk(mu_);
    auto it = txns_.find(txn_id);
    if (it == txns_.end()) {
      return Status::OK();
    }
    txn = std::move(it->second);
    txns_.erase(it);
  }

  wal_->AppendSync(txn_id, WALRecordType::ABORT, "");
  lock_mgr_->ReleaseAll(txn_id);
  return Status::OK();
}

Status TxnManager::CommitSingleShard(uint64_t txn_id, uint64_t commit_ts) {
  Status p = Prepare(txn_id, commit_ts);
  if (!p.ok()) {
    return p;
  }
  return Commit(txn_id, commit_ts);
}

Status TxnManager::Recover() {
  std::vector<WALRecord> records;
  wal_->Replay([&records](const WALRecord& r) { records.push_back(r); });

  uint64_t max_txn_id = 0;
  for (const auto& r : records) {
    max_txn_id = std::max(max_txn_id, r.txn_id);
  }
  next_txn_id_.store(max_txn_id + 1, std::memory_order_relaxed);

  std::unordered_map<uint64_t, WALRecordType> last_type;
  std::unordered_map<uint64_t, size_t> last_index;
  for (size_t i = 0; i < records.size(); ++i) {
    last_type[records[i].txn_id] = records[i].type;
    last_index[records[i].txn_id] = i;
  }

  std::vector<uint64_t> txn_order;
  txn_order.reserve(last_index.size());
  for (const auto& kv : last_index) {
    txn_order.push_back(kv.first);
  }
  std::sort(txn_order.begin(), txn_order.end(), [&](uint64_t a, uint64_t b) {
    return last_index[a] < last_index[b];
  });

  for (uint64_t tid : txn_order) {
    const size_t last_i = last_index[tid];
    const WALRecordType lt = last_type[tid];

    if (lt == WALRecordType::COMMIT) {
      WAL::CommitInfo ci{};
      if (!WAL::DeserializeCommitPayload(records[last_i].payload, &ci)) {
        return Status::IOError("bad WAL commit payload during recovery");
      }
      const uint64_t cts = ci.commit_ts;
      for (size_t i = 0; i < last_i; ++i) {
        if (records[i].txn_id != tid) {
          continue;
        }
        const WALRecord& wr = records[i];
        if (wr.type == WALRecordType::WRITE) {
          WAL::WriteInfo wi{};
          if (!WAL::DeserializeWritePayload(wr.payload, &wi)) {
            return Status::IOError("bad WAL write payload during recovery");
          }
          if (!store_->ReplayPut(wi.table_id, wi.key, cts, wi.value).ok()) {
            return Status::IOError("ReplayPut failed during recovery");
          }
        } else if (wr.type == WALRecordType::DELETE) {
          WAL::DeleteInfo di{};
          if (!WAL::DeserializeDeletePayload(wr.payload, &di)) {
            return Status::IOError("bad WAL delete payload during recovery");
          }
          const std::string_view tomb("\x00", 1);
          if (!store_->ReplayPut(di.table_id, di.key, cts, tomb).ok()) {
            return Status::IOError("ReplayPut tombstone failed during recovery");
          }
        }
      }
      continue;
    }

    if (lt == WALRecordType::ABORT) {
      continue;
    }

    if (lt == WALRecordType::PREPARE) {
      WAL::PrepareInfo pi{};
      if (!WAL::DeserializePreparePayload(records[last_i].payload, &pi)) {
        return Status::IOError("bad WAL prepare payload during recovery");
      }

      auto txn = std::make_unique<Transaction>();
      txn->txn_id = tid;
      txn->state = TxnState::PREPARED;
      txn->prepare_commit_ts = pi.commit_ts;

      std::set<std::pair<uint32_t, std::string>> unique_keys;

      for (size_t i = 0; i < last_i; ++i) {
        if (records[i].txn_id != tid) {
          continue;
        }
        const WALRecord& wr = records[i];
        if (wr.type == WALRecordType::BEGIN) {
          WAL::BeginInfo bi{};
          if (!WAL::DeserializeBeginPayload(wr.payload, &bi)) {
            return Status::IOError("bad WAL begin payload during recovery");
          }
          txn->snapshot_ts = bi.snapshot_ts;
        } else if (wr.type == WALRecordType::WRITE) {
          WAL::WriteInfo wi{};
          if (!WAL::DeserializeWritePayload(wr.payload, &wi)) {
            return Status::IOError("bad WAL write payload during recovery");
          }
          BufferedWrite bw;
          bw.table_id = wi.table_id;
          bw.key = wi.key;
          bw.value = wi.value;
          bw.is_delete = false;
          unique_keys.emplace(bw.table_id, bw.key);
          txn->write_set.push_back(std::move(bw));
        } else if (wr.type == WALRecordType::DELETE) {
          WAL::DeleteInfo di{};
          if (!WAL::DeserializeDeletePayload(wr.payload, &di)) {
            return Status::IOError("bad WAL delete payload during recovery");
          }
          BufferedWrite bw;
          bw.table_id = di.table_id;
          bw.key = di.key;
          bw.value.clear();
          bw.is_delete = true;
          unique_keys.emplace(bw.table_id, bw.key);
          txn->write_set.push_back(std::move(bw));
        }
      }

      for (const auto& tk : unique_keys) {
        Status acq = lock_mgr_->Acquire(
            tk.first, tk.second, tid,
            std::chrono::milliseconds(static_cast<int>(std::numeric_limits<int>::max() / 4)));
        if (!acq.ok()) {
          return acq;
        }
        txn->locked_keys.emplace_back(tk.first, tk.second);
      }

      std::scoped_lock lk(mu_);
      txns_[tid] = std::move(txn);
      continue;
    }
  }

  return Status::OK();
}

}  // namespace txndb
