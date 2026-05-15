#include "raft/raft_state_machine.h"

#include "storage/status.h"
#include "txn/wal.h"

#include <cstring>
#include <mutex>

namespace txndb {

namespace {

void AppendLe64(std::string* s, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    s->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
  }
}

uint64_t ReadLe64(std::string_view sv) {
  uint64_t v = 0;
  for (int i = 0; i < 8 && i < static_cast<int>(sv.size()); ++i) {
    v |= static_cast<uint64_t>(static_cast<unsigned char>(sv[i])) << (8 * i);
  }
  return v;
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

std::function<void(const RaftLogEntry&)> RaftStateMachine::WrapApply() {
  return [this](const RaftLogEntry& e) { Apply(e); };
}

uint64_t RaftStateMachine::GetLocalTxnId(uint64_t raft_txn_id) const {
  std::scoped_lock lk(mu_);
  auto it = txn_id_map_.find(raft_txn_id);
  return it != txn_id_map_.end() ? it->second : 0;
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
      uint64_t local = it->second;
      txn_mgr_->Abort(local);
      txn_id_map_.erase(it);
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
      uint64_t local = txn_mgr_->Begin(bi.snapshot_ts);
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
      uint64_t local = it->second;
      txn_mgr_->Commit(local, ci.commit_ts);
      txn_id_map_.erase(it);
      return;
    }
    default:
      return;
  }
}

}  // namespace txndb
