#include "raft/raft_state_machine.h"

#include "storage/status.h"
#include "txn/wal.h"

#include <cstring>

namespace txndb {

namespace {

void AppendLe32(std::string* s, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    s->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
  }
}

void AppendLe64(std::string* s, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    s->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
  }
}

uint32_t ReadLe32(std::string_view sv) {
  uint32_t v = 0;
  for (int i = 0; i < 4 && i < static_cast<int>(sv.size()); ++i) {
    v |= static_cast<uint32_t>(static_cast<unsigned char>(sv[i])) << (8 * i);
  }
  return v;
}

uint64_t ReadLe64(std::string_view sv) {
  uint64_t v = 0;
  for (int i = 0; i < 8 && i < static_cast<int>(sv.size()); ++i) {
    v |= static_cast<uint64_t>(static_cast<unsigned char>(sv[i])) << (8 * i);
  }
  return v;
}

bool ConsumeU32(std::string_view* data, uint32_t* out) {
  if (data->size() < 4) {
    return false;
  }
  *out = ReadLe32(data->substr(0, 4));
  data->remove_prefix(4);
  return true;
}

bool ConsumeU64(std::string_view* data, uint64_t* out) {
  if (data->size() < 8) {
    return false;
  }
  *out = ReadLe64(data->substr(0, 8));
  data->remove_prefix(8);
  return true;
}

}  // namespace

RaftStateMachine::RaftStateMachine(TxnManager* txn_mgr) : txn_mgr_(txn_mgr) {}

std::string RaftStateMachine::PackTxnPayload(std::string_view wal_payload, uint64_t raft_txn_id) {
  std::string s(wal_payload.begin(), wal_payload.end());
  AppendLe64(&s, raft_txn_id);
  return s;
}

std::string RaftStateMachine::PackAbortPayload(uint64_t raft_txn_id) {
  std::string s;
  s.reserve(8);
  AppendLe64(&s, raft_txn_id);
  return s;
}

std::string RaftStateMachine::PackPrepareBatchPayload(
    uint64_t raft_txn_id, uint64_t snapshot_ts, uint64_t commit_ts,
    const std::vector<PrepareBatchWrite>& writes) {
  std::string payload;
  payload.reserve(32 + writes.size() * 24);
  AppendLe64(&payload, raft_txn_id);
  AppendLe64(&payload, snapshot_ts);
  AppendLe64(&payload, commit_ts);
  AppendLe32(&payload, static_cast<uint32_t>(writes.size()));
  for (const auto& w : writes) {
    payload.push_back(w.is_delete ? '\x01' : '\x00');
    AppendLe32(&payload, w.table_id);
    AppendLe32(&payload, static_cast<uint32_t>(w.key.size()));
    payload.append(w.key);
    AppendLe32(&payload, static_cast<uint32_t>(w.value.size()));
    payload.append(w.value);
  }
  return payload;
}

std::function<void(const RaftLogEntry&)> RaftStateMachine::WrapApply() {
  return [this](const RaftLogEntry& e) { Apply(e); };
}

uint64_t RaftStateMachine::GetLocalTxnId(uint64_t raft_txn_id) const {
  std::scoped_lock lk(mu_);
  auto it = txn_id_map_.find(raft_txn_id);
  return it != txn_id_map_.end() ? it->second : 0;
}

bool RaftStateMachine::IsTxnPrepared(uint64_t raft_txn_id) const {
  std::scoped_lock lk(mu_);
  const auto it = prepare_results_.find(raft_txn_id);
  return it != prepare_results_.end() && it->second.success;
}

std::optional<PrepareResult> RaftStateMachine::TakePrepareResult(uint64_t raft_txn_id) {
  std::scoped_lock lk(mu_);
  auto it = prepare_results_.find(raft_txn_id);
  if (it == prepare_results_.end()) {
    return std::nullopt;
  }
  PrepareResult out = std::move(it->second);
  prepare_results_.erase(it);
  return out;
}

void RaftStateMachine::RegisterLocalTxn(uint64_t raft_txn_id, uint64_t local_txn_id) {
  std::scoped_lock lk(mu_);
  txn_id_map_[raft_txn_id] = local_txn_id;
}

void RaftStateMachine::ForgetTxn(uint64_t raft_txn_id) {
  std::scoped_lock lk(mu_);
  txn_id_map_.erase(raft_txn_id);
  prepare_results_.erase(raft_txn_id);
}

bool RaftStateMachine::UnpackPayload(std::string_view full_payload, std::string* wal_payload,
                                     uint64_t* raft_txn_id) {
  if (full_payload.size() < sizeof(uint64_t)) {
    return false;
  }
  const size_t n = full_payload.size();
  *wal_payload = std::string(full_payload.data(), n - sizeof(uint64_t));
  *raft_txn_id = ReadLe64(full_payload.substr(n - sizeof(uint64_t)));
  return true;
}

bool RaftStateMachine::UnpackPrepareBatchPayload(std::string_view payload, uint64_t* raft_txn_id,
                                                 uint64_t* snapshot_ts, uint64_t* commit_ts,
                                                 std::vector<PrepareBatchWrite>* writes) {
  writes->clear();
  uint32_t num_writes = 0;
  if (!ConsumeU64(&payload, raft_txn_id) || !ConsumeU64(&payload, snapshot_ts) ||
      !ConsumeU64(&payload, commit_ts) || !ConsumeU32(&payload, &num_writes)) {
    return false;
  }
  writes->reserve(num_writes);
  for (uint32_t i = 0; i < num_writes; ++i) {
    if (payload.empty()) {
      return false;
    }
    PrepareBatchWrite w;
    w.is_delete = payload.front() != 0;
    payload.remove_prefix(1);
    uint32_t key_len = 0;
    uint32_t val_len = 0;
    if (!ConsumeU32(&payload, &w.table_id) || !ConsumeU32(&payload, &key_len) ||
        payload.size() < key_len) {
      return false;
    }
    w.key.assign(payload.substr(0, key_len));
    payload.remove_prefix(key_len);
    if (!ConsumeU32(&payload, &val_len) || payload.size() < val_len) {
      return false;
    }
    w.value.assign(payload.substr(0, val_len));
    payload.remove_prefix(val_len);
    writes->push_back(std::move(w));
  }
  return true;
}

void RaftStateMachine::Apply(const RaftLogEntry& entry) {
  switch (entry.type) {
    case RaftEntryType::NOOP:
      return;
    case RaftEntryType::TXN_ABORT: {
      if (entry.payload.size() != sizeof(uint64_t)) {
        return;
      }
      const uint64_t raft_tid = ReadLe64(entry.payload);
      std::scoped_lock lk(mu_);
      auto it = txn_id_map_.find(raft_tid);
      if (it == txn_id_map_.end()) {
        return;
      }
      const uint64_t local = it->second;
      txn_mgr_->Abort(local);
      txn_id_map_.erase(it);
      prepare_results_.erase(raft_tid);
      return;
    }
    case RaftEntryType::TXN_PREPARE_BATCH: {
      uint64_t raft_txn_id = 0;
      uint64_t snapshot_ts = 0;
      uint64_t commit_ts = 0;
      std::vector<PrepareBatchWrite> writes;
      if (!UnpackPrepareBatchPayload(entry.payload, &raft_txn_id, &snapshot_ts, &commit_ts,
                                     &writes)) {
        return;
      }

      {
        std::scoped_lock lk(mu_);
        if (txn_id_map_.find(raft_txn_id) != txn_id_map_.end()) {
          prepare_results_[raft_txn_id] = {true, ""};
          return;
        }
      }

      std::vector<BufferedWrite> buffered;
      buffered.reserve(writes.size());
      for (const auto& w : writes) {
        BufferedWrite bw;
        bw.is_delete = w.is_delete;
        bw.table_id = w.table_id;
        bw.key = w.key;
        bw.value = w.value;
        buffered.push_back(std::move(bw));
      }
      const uint64_t local_id = txn_mgr_->ImportPreparedTxn(snapshot_ts, commit_ts, buffered);
      Status ps = Status::OK();

      std::scoped_lock lk(mu_);
      if (ps.ok()) {
        txn_id_map_[raft_txn_id] = local_id;
      }
      prepare_results_[raft_txn_id] = {ps.ok(), ps.message()};
      return;
    }
    default:
      break;
  }

  std::string wal_pl;
  uint64_t raft_txn_id = 0;
  if (!UnpackPayload(entry.payload, &wal_pl, &raft_txn_id)) {
    return;
  }

  std::unique_lock<std::mutex> lk(mu_);
  switch (entry.type) {
    case RaftEntryType::TXN_BEGIN: {
      WAL::BeginInfo bi{};
      if (!WAL::DeserializeBeginPayload(wal_pl, &bi)) {
        return;
      }
      const uint64_t local = txn_mgr_->Begin(bi.snapshot_ts);
      txn_id_map_[raft_txn_id] = local;
      return;
    }
    case RaftEntryType::TXN_WRITE: {
      WAL::WriteInfo wi{};
      if (!WAL::DeserializeWritePayload(wal_pl, &wi)) {
        return;
      }
      auto it = txn_id_map_.find(raft_txn_id);
      if (it == txn_id_map_.end()) {
        return;
      }
      txn_mgr_->Write(it->second, wi.table_id, wi.key, wi.value);
      return;
    }
    case RaftEntryType::TXN_DELETE: {
      WAL::DeleteInfo di{};
      if (!WAL::DeserializeDeletePayload(wal_pl, &di)) {
        return;
      }
      auto it = txn_id_map_.find(raft_txn_id);
      if (it == txn_id_map_.end()) {
        return;
      }
      txn_mgr_->Delete(it->second, di.table_id, di.key);
      return;
    }
    case RaftEntryType::TXN_PREPARE: {
      WAL::PrepareInfo pi{};
      if (!WAL::DeserializePreparePayload(wal_pl, &pi)) {
        prepare_results_[raft_txn_id] = {false, "bad prepare payload"};
        return;
      }
      auto it = txn_id_map_.find(raft_txn_id);
      if (it == txn_id_map_.end()) {
        prepare_results_[raft_txn_id] = {false, "unknown txn"};
        return;
      }
      const uint64_t local = it->second;
      lk.unlock();
      Status st = txn_mgr_->Prepare(local, pi.commit_ts);
      lk.lock();
      prepare_results_[raft_txn_id] = {st.ok(), st.message()};
      if (!st.ok()) {
        txn_mgr_->Abort(local);
        txn_id_map_.erase(raft_txn_id);
      }
      return;
    }
    case RaftEntryType::TXN_COMMIT: {
      WAL::CommitInfo ci{};
      if (!WAL::DeserializeCommitPayload(wal_pl, &ci)) {
        return;
      }
      auto it = txn_id_map_.find(raft_txn_id);
      if (it == txn_id_map_.end()) {
        return;
      }
      const uint64_t local = it->second;
      txn_mgr_->Commit(local, ci.commit_ts);
      txn_id_map_.erase(it);
      prepare_results_.erase(raft_txn_id);
      return;
    }
    default:
      return;
  }
}

}  // namespace txndb
