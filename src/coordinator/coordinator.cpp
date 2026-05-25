#include "coordinator/coordinator.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <future>
#include <stdexcept>
#include <string_view>
#include <thread>

namespace txndb {

namespace {

grpc::ChannelArguments MakeChannelArgs() {
  grpc::ChannelArguments opts;
  return opts;
}

Status FromExecuteResponse(bool grpc_ok, const ExecuteResponse* resp, std::string* value_out = nullptr) {
  if (!grpc_ok || resp == nullptr) {
    return Status::IOError("gRPC Execute failed");
  }
  if (!resp->ok()) {
    return Status::Error(static_cast<StatusCode>(resp->error_code()), resp->error_message());
  }
  if (value_out != nullptr && !resp->value().empty()) {
    *value_out = resp->value();
  }
  return Status::OK();
}

}  // namespace

void Coordinator::ApplyDeadline(grpc::ClientContext* ctx) {
  const auto deadline = std::chrono::system_clock::now() + kGrpcTimeout;
  ctx->set_deadline(deadline);
}

bool Coordinator::IsLeaderUnavailableMessage(std::string_view message) {
  return message.find("no raft leader") != std::string_view::npos ||
         message.find("no leader") != std::string_view::npos;
}

Coordinator::Coordinator(const std::unordered_map<uint32_t, std::string>& shard_addresses,
                         uint32_t num_shards, std::string coordinator_log_path,
                         std::unordered_map<uint32_t, std::vector<std::string>>
                             shard_replica_addresses)
    : num_shards_(num_shards) {
  if (coordinator_log_path.empty()) {
    coordinator_log_path = (std::filesystem::temp_directory_path() / "trans_db_coordinator.log").string();
  }
  if (!CoordinatorLog::Open(coordinator_log_path, &coordinator_log_).ok()) {
    throw std::runtime_error("failed to open coordinator log");
  }

  for (const auto& [id, addr] : shard_addresses) {
    if (id >= num_shards) {
      throw std::invalid_argument("shard id out of range");
    }
    auto ch =
        grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), MakeChannelArgs());
    channels_[id] = std::move(ch);
    stubs_[id] = ShardService::NewStub(channels_[id]);
    active_replica_index_[id] = 0;
  }

  for (auto& [id, addrs] : shard_replica_addresses) {
    if (id >= num_shards || addrs.empty()) {
      continue;
    }
    auto& chans = replica_channels_[id];
    auto& stubs = replica_stubs_[id];
    chans.reserve(addrs.size());
    stubs.reserve(addrs.size());
    for (const auto& addr : addrs) {
      auto ch =
          grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), MakeChannelArgs());
      chans.push_back(std::move(ch));
      stubs.push_back(ShardService::NewStub(chans.back()));
    }
    active_replica_index_[id] = 0;
  }
  Status rs = Recover();
  if (!rs.ok()) {
    throw std::runtime_error("coordinator recover failed: " + rs.message());
  }
}

uint32_t Coordinator::RouteShard(std::string_view key) const {
  constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
  constexpr uint64_t kFnvPrime = 1099511628211ULL;
  uint64_t hash = kFnvOffset;
  for (unsigned char c : key) {
    hash ^= static_cast<uint64_t>(c);
    hash *= kFnvPrime;
  }
  return static_cast<uint32_t>(hash % std::max<uint32_t>(1u, num_shards_));
}

ShardService::Stub* Coordinator::GetStub(uint32_t shard_id) {
  auto rit = replica_stubs_.find(shard_id);
  if (rit != replica_stubs_.end() && !rit->second.empty()) {
    const size_t idx = active_replica_index_[shard_id];
    return rit->second[idx].get();
  }
  auto it = stubs_.find(shard_id);
  return it != stubs_.end() ? it->second.get() : nullptr;
}

void Coordinator::RotateStub(uint32_t shard_id) {
  auto rit = replica_stubs_.find(shard_id);
  if (rit == replica_stubs_.end() || rit->second.size() <= 1) {
    return;
  }
  size_t& idx = active_replica_index_[shard_id];
  idx = (idx + 1) % rit->second.size();
}

bool Coordinator::RpcExecuteOnShard(uint32_t shard_id, const ExecuteRequest& req,
                                    ExecuteResponse* resp) {
  ShardService::Stub* stub = GetStub(shard_id);
  if (!stub) {
    return false;
  }
  grpc::ClientContext ctx;
  ApplyDeadline(&ctx);
  return stub->Execute(&ctx, req, resp).ok();
}

Status Coordinator::ExecuteWithLeaderRetry(uint32_t shard_id, const ExecuteRequest& req,
                                         ExecuteResponse* resp, std::string* value_out) {
  for (size_t a = 0; a < kLeaderRetryAttempts; ++a) {
    if (!RpcExecuteOnShard(shard_id, req, resp)) {
      RotateStub(shard_id);
      std::this_thread::sleep_for(kLeaderRetrySleep);
      continue;
    }
    const Status st = FromExecuteResponse(true, resp, value_out);
    if (st.ok()) {
      return st;
    }
    if (!IsLeaderUnavailableMessage(st.message())) {
      return st;
    }
    RotateStub(shard_id);
    std::this_thread::sleep_for(kLeaderRetrySleep);
  }
  return Status::IOError("no raft leader available on shard");
}

bool Coordinator::PrepareOnShard(uint32_t shard_id, const PrepareRequest& req,
                                 PrepareResponse* resp) {
  const size_t attempts =
      replica_stubs_.count(shard_id) ? std::max<size_t>(1, replica_stubs_[shard_id].size()) : 1;
  for (size_t a = 0; a < attempts; ++a) {
    ShardService::Stub* stub = GetStub(shard_id);
    if (!stub) {
      return false;
    }
    grpc::ClientContext ctx;
    ApplyDeadline(&ctx);
    if (stub->Prepare(&ctx, req, resp).ok() && resp->vote_commit()) {
      return true;
    }
    if (!resp->error_message().empty() &&
        !IsLeaderUnavailableMessage(resp->error_message())) {
      return false;
    }
    RotateStub(shard_id);
  }
  return false;
}

bool Coordinator::CommitOnShard(uint32_t shard_id, const CommitRequest& req,
                                CommitResponse* resp) {
  const size_t attempts =
      replica_stubs_.count(shard_id) ? std::max<size_t>(1, replica_stubs_[shard_id].size()) : 1;
  for (size_t a = 0; a < attempts; ++a) {
    ShardService::Stub* stub = GetStub(shard_id);
    if (!stub) {
      return false;
    }
    grpc::ClientContext ctx;
    ApplyDeadline(&ctx);
    if (stub->Commit(&ctx, req, resp).ok() && resp->ok()) {
      return true;
    }
    if (!resp->error_message().empty() &&
        !IsLeaderUnavailableMessage(resp->error_message())) {
      return false;
    }
    RotateStub(shard_id);
  }
  return false;
}

bool Coordinator::AbortOnShard(uint32_t shard_id, const AbortRequest& req, AbortResponse* resp) {
  const size_t attempts =
      replica_stubs_.count(shard_id) ? std::max<size_t>(1, replica_stubs_[shard_id].size()) : 1;
  for (size_t a = 0; a < attempts; ++a) {
    ShardService::Stub* stub = GetStub(shard_id);
    if (!stub) {
      return false;
    }
    grpc::ClientContext ctx;
    ApplyDeadline(&ctx);
    if (stub->Abort(&ctx, req, resp).ok()) {
      return true;
    }
    RotateStub(shard_id);
  }
  return false;
}

bool Coordinator::QueryTxnStateOnShard(uint32_t shard_id, const TxnStateRequest& req,
                                       TxnStateResponse* resp) {
  const size_t attempts =
      replica_stubs_.count(shard_id) ? std::max<size_t>(1, replica_stubs_[shard_id].size()) : 1;
  for (size_t a = 0; a < attempts; ++a) {
    ShardService::Stub* stub = GetStub(shard_id);
    if (!stub) {
      return false;
    }
    grpc::ClientContext ctx;
    ApplyDeadline(&ctx);
    if (stub->QueryTxnState(&ctx, req, resp).ok() && resp->ok()) {
      return true;
    }
    RotateStub(shard_id);
  }
  return false;
}

Status Coordinator::EnsureShardParticipant(CoordinatorTxn& txn, uint32_t shard_id) {
  if (txn.participant_shards.count(shard_id)) {
    return Status::OK();
  }
  const auto replica_count =
      replica_stubs_.count(shard_id) ? replica_stubs_[shard_id].size() : size_t{1};
  ExecuteRequest req;
  req.set_raft_txn_id(txn.raft_txn_id);
  req.set_op(OP_BEGIN);
  req.set_snapshot_ts(txn.snapshot_ts);
  ExecuteResponse resp;
  const Status st = ExecuteWithLeaderRetry(shard_id, req, &resp);
  if (st.ok()) {
    txn.participant_shards.insert(shard_id);
  }
  return st;
}

uint64_t Coordinator::Begin() {
  std::scoped_lock lk(mu_);
  const uint64_t raft_txn_id = next_txn_id_++;
  const uint64_t snap = ts_oracle_.Now();
  auto txn = std::make_shared<CoordinatorTxn>();
  txn->raft_txn_id = raft_txn_id;
  txn->snapshot_ts = snap;
  txn->aborted = false;
  txns_[raft_txn_id] = std::move(txn);
  return raft_txn_id;
}

Status Coordinator::AppendCoordinatorRecordWrite(CoordinatorLogRecordType type, uint64_t txn_id,
                                                 const std::vector<uint32_t>& shards) {
  if (!coordinator_log_) {
    return Status::IOError("coordinator log unavailable");
  }
  CoordinatorLogRecord rec;
  rec.type = type;
  rec.txn_id = txn_id;
  rec.timestamp_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  rec.shards = shards;
  return coordinator_log_->AppendWrite(rec);
}

Status Coordinator::FlushCoordinatorLog() {
  if (!coordinator_log_) {
    return Status::IOError("coordinator log unavailable");
  }
  return coordinator_log_->Flush();
}

Status Coordinator::AppendCoordinatorRecord(CoordinatorLogRecordType type, uint64_t txn_id,
                                            const std::vector<uint32_t>& shards) {
  Status ws = AppendCoordinatorRecordWrite(type, txn_id, shards);
  if (!ws.ok()) {
    return ws;
  }
  return FlushCoordinatorLog();
}

TxnStateCode Coordinator::QueryShardTxnState(uint32_t shard_id, uint64_t txn_id) {
  TxnStateRequest req;
  req.set_raft_txn_id(txn_id);
  TxnStateResponse resp;
  if (!QueryTxnStateOnShard(shard_id, req, &resp)) {
    return TxnStateCode::TXN_UNKNOWN;
  }
  return resp.state();
}

Status Coordinator::DriveCommit(uint64_t txn_id, const std::vector<uint32_t>& shards) {
  const uint64_t commit_ts = ts_oracle_.Now();
  std::vector<std::future<bool>> commit_futs;
  commit_futs.reserve(shards.size());
  for (uint32_t sid : shards) {
    commit_futs.push_back(std::async(std::launch::async, [this, txn_id, sid, commit_ts]() {
      CommitRequest cr;
      cr.set_raft_txn_id(txn_id);
      cr.set_commit_ts(commit_ts);
      CommitResponse cresp;
      return CommitOnShard(sid, cr, &cresp);
    }));
  }
  for (auto& f : commit_futs) {
    if (!f.get()) {
      return Status::IOError("recovery commit failed");
    }
  }
  return Status::OK();
}

Status Coordinator::DriveAbort(uint64_t txn_id, const std::vector<uint32_t>& shards) {
  std::vector<std::future<void>> abort_futs;
  abort_futs.reserve(shards.size());
  for (uint32_t sid : shards) {
    abort_futs.push_back(std::async(std::launch::async, [this, txn_id, sid]() {
      AbortRequest ar;
      ar.set_raft_txn_id(txn_id);
      AbortResponse aresp;
      (void)AbortOnShard(sid, ar, &aresp);
    }));
  }
  for (auto& f : abort_futs) {
    f.wait();
  }
  return Status::OK();
}

Status Coordinator::Recover() {
  if (!coordinator_log_) {
    return Status::OK();
  }

  struct TxnRecoveryState {
    std::vector<uint32_t> shards;
    bool saw_preparing{false};
    bool saw_committing{false};
    bool saw_committed{false};
    bool saw_aborting{false};
    bool saw_aborted{false};
  };
  std::unordered_map<uint64_t, TxnRecoveryState> states;
  Status rs = coordinator_log_->Replay([&](const CoordinatorLogRecord& rec) {
    auto& st = states[rec.txn_id];
    if (rec.type == CoordinatorLogRecordType::PREPARING) {
      st.saw_preparing = true;
      st.shards = rec.shards;
    } else if (rec.type == CoordinatorLogRecordType::COMMITTING) {
      st.saw_committing = true;
    } else if (rec.type == CoordinatorLogRecordType::COMMITTED) {
      st.saw_committed = true;
    } else if (rec.type == CoordinatorLogRecordType::ABORTING) {
      st.saw_aborting = true;
    } else if (rec.type == CoordinatorLogRecordType::ABORTED) {
      st.saw_aborted = true;
    }
  });
  if (!rs.ok()) {
    return rs;
  }

  for (const auto& [txn_id, st] : states) {
    if (!st.saw_preparing || st.saw_committed || st.saw_aborted) {
      continue;
    }
    if (st.shards.empty()) {
      continue;
    }

    if (st.saw_committing) {
      Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTING, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      Status cs = DriveCommit(txn_id, st.shards);
      if (!cs.ok()) {
        return cs;
      }
      ls = AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTED, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      continue;
    }

    if (st.saw_aborting) {
      Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTING, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      Status as = DriveAbort(txn_id, st.shards);
      if (!as.ok()) {
        return as;
      }
      ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTED, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      continue;
    }

    bool any_aborted = false;
    bool any_committed = false;
    bool all_prepared = true;
    for (uint32_t sid : st.shards) {
      TxnStateCode ss = QueryShardTxnState(sid, txn_id);
      if (ss == TxnStateCode::TXN_ABORTED) {
        any_aborted = true;
      }
      if (ss == TxnStateCode::TXN_COMMITTED) {
        any_committed = true;
      }
      if (ss != TxnStateCode::TXN_PREPARED && ss != TxnStateCode::TXN_COMMITTED) {
        all_prepared = false;
      }
    }

    if (any_aborted) {
      Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTING, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      Status as = DriveAbort(txn_id, st.shards);
      if (!as.ok()) {
        return as;
      }
      ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTED, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      continue;
    }

    if (all_prepared || any_committed) {
      Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTING, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      Status cs = DriveCommit(txn_id, st.shards);
      if (!cs.ok()) {
        return cs;
      }
      ls = AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTED, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      continue;
    }

    Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTING, txn_id);
    if (!ls.ok()) {
      return ls;
    }
    Status as = DriveAbort(txn_id, st.shards);
    if (!as.ok()) {
      return as;
    }
    ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTED, txn_id);
    if (!ls.ok()) {
      return ls;
    }
  }

  return Status::OK();
}

Status Coordinator::Read(uint64_t txn_id, uint32_t table_id, std::string_view key,
                         std::string* value) {
  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  const uint32_t shard_id = RouteShard(key);
  Status es = EnsureShardParticipant(*txn, shard_id);
  if (!es.ok()) {
    return es;
  }

  ExecuteRequest req;
  req.set_raft_txn_id(txn->raft_txn_id);
  req.set_op(OP_READ);
  req.set_table_id(table_id);
  req.set_key(key.data(), key.size());
  ExecuteResponse resp;
  return ExecuteWithLeaderRetry(shard_id, req, &resp, value);
}

Status Coordinator::Write(uint64_t txn_id, uint32_t table_id, std::string_view key,
                          std::string_view value) {
  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  const uint32_t shard_id = RouteShard(key);
  Status es = EnsureShardParticipant(*txn, shard_id);
  if (!es.ok()) {
    return es;
  }

  ExecuteRequest req;
  req.set_raft_txn_id(txn->raft_txn_id);
  req.set_op(OP_WRITE);
  req.set_table_id(table_id);
  req.set_key(key.data(), key.size());
  req.set_value(value.data(), value.size());
  ExecuteResponse resp;
  return ExecuteWithLeaderRetry(shard_id, req, &resp);
}

Status Coordinator::Delete(uint64_t txn_id, uint32_t table_id, std::string_view key) {
  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  const uint32_t shard_id = RouteShard(key);
  Status es = EnsureShardParticipant(*txn, shard_id);
  if (!es.ok()) {
    return es;
  }

  ExecuteRequest req;
  req.set_raft_txn_id(txn->raft_txn_id);
  req.set_op(OP_DELETE);
  req.set_table_id(table_id);
  req.set_key(key.data(), key.size());
  ExecuteResponse resp;
  return ExecuteWithLeaderRetry(shard_id, req, &resp);
}

Status Coordinator::Scan(uint64_t txn_id, uint32_t table_id, std::string_view range_start_pk,
                         std::string_view range_end_exclusive, bool range_end_open,
                         std::vector<std::pair<std::string, std::string>>* rows_out) {
  rows_out->clear();

  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }

  for (uint32_t sid = 0; sid < num_shards_; ++sid) {
    Status es = EnsureShardParticipant(*txn, sid);
    if (!es.ok()) {
      return es;
    }
  }


  std::vector<std::future<std::pair<Status, std::vector<std::pair<std::string, std::string>>>>>
      scan_futs;
  scan_futs.reserve(num_shards_);
  for (uint32_t sid = 0; sid < num_shards_; ++sid) {
    scan_futs.push_back(std::async(std::launch::async, [=, this]() {
      std::vector<std::pair<std::string, std::string>> shard_rows;
      ExecuteRequest req;
      req.set_raft_txn_id(txn->raft_txn_id);
      req.set_op(OP_SCAN);
      req.set_table_id(table_id);
      req.set_range_start_pk(range_start_pk.data(), range_start_pk.size());
      req.set_range_end_exclusive(range_end_exclusive.data(), range_end_exclusive.size());
      req.set_range_end_open(range_end_open);
      ExecuteResponse resp;
      const Status st = ExecuteWithLeaderRetry(sid, req, &resp);
      if (!st.ok()) {
        return std::make_pair(st, std::move(shard_rows));
      }
      shard_rows.reserve(static_cast<size_t>(resp.scan_rows_size()));
      for (const ScanRow& row : resp.scan_rows()) {
        shard_rows.emplace_back(row.pk(), row.row_value());
      }
      return std::make_pair(Status::OK(), std::move(shard_rows));
    }));
  }

  for (auto& fut : scan_futs) {
    auto [st, shard_rows] = fut.get();
    if (!st.ok()) {
      return st;
    }
    rows_out->insert(rows_out->end(), std::make_move_iterator(shard_rows.begin()),
                     std::make_move_iterator(shard_rows.end()));
  }

  std::sort(rows_out->begin(), rows_out->end(),
            [](const std::pair<std::string, std::string>& a,
               const std::pair<std::string, std::string>& b) { return a.first < b.first; });

  return Status::OK();
}

bool Coordinator::RpcDistributedJoinOnShard(uint32_t shard_id, const DistributedJoinRequest& req,
                                            DistributedJoinResponse* resp) {
  ShardService::Stub* stub = GetStub(shard_id);
  if (!stub) {
    return false;
  }
  grpc::ClientContext ctx;
  ApplyDeadline(&ctx);
  return stub->DistributedJoin(&ctx, req, resp).ok();
}

Status Coordinator::DistributedJoinOnShard(uint32_t shard_id, const DistributedJoinRequest& req,
                                         DistributedJoinResponse* resp) {
  resp->Clear();

  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(req.raft_txn_id());
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  if (shard_id >= num_shards_) {
    return Status::Error(StatusCode::InvalidArgument, "unknown shard id");
  }

  Status es = EnsureShardParticipant(*txn, shard_id);
  if (!es.ok()) {
    return es;
  }

  DistributedJoinRequest shard_req = req;
  shard_req.set_raft_txn_id(txn->raft_txn_id);

  for (size_t a = 0; a < kLeaderRetryAttempts; ++a) {
    if (!RpcDistributedJoinOnShard(shard_id, shard_req, resp)) {
      RotateStub(shard_id);
      std::this_thread::sleep_for(kLeaderRetrySleep);
      continue;
    }
    if (resp->ok()) {
      return Status::OK();
    }
    if (!resp->error_message().empty() &&
        !IsLeaderUnavailableMessage(resp->error_message())) {
      return Status::Error(static_cast<StatusCode>(resp->error_code()), resp->error_message());
    }
    RotateStub(shard_id);
    std::this_thread::sleep_for(kLeaderRetrySleep);
  }
  return Status::IOError("no raft leader available on shard");
}

Status Coordinator::SingleShardCommit(uint64_t raft_txn_id, uint32_t shard_id,
                                     uint64_t commit_ts) {
  std::vector<uint32_t> parts{shard_id};
  Status ls = AppendCoordinatorRecordWrite(CoordinatorLogRecordType::PREPARING, raft_txn_id, parts);
  if (!ls.ok()) {
    return ls;
  }
  ls = FlushCoordinatorLog();
  if (!ls.ok()) {
    return ls;
  }
  PrepareRequest pr;
  pr.set_raft_txn_id(raft_txn_id);
  pr.set_commit_ts(commit_ts);
  PrepareResponse presp;
  if (!PrepareOnShard(shard_id, pr, &presp)) {
    return Status::IOError("Prepare RPC failed");
  }
  if (!presp.vote_commit()) {
    (void)AppendCoordinatorRecordWrite(CoordinatorLogRecordType::ABORTING, raft_txn_id);
    AbortRequest ar;
    ar.set_raft_txn_id(raft_txn_id);
    AbortResponse aresp;
    (void)AbortOnShard(shard_id, ar, &aresp);
    (void)AppendCoordinatorRecordWrite(CoordinatorLogRecordType::ABORTED, raft_txn_id);
    ls = FlushCoordinatorLog();
    if (!ls.ok()) {
      return ls;
    }
    return Status::Error(StatusCode::Conflict, presp.error_message());
  }

  ls = AppendCoordinatorRecordWrite(CoordinatorLogRecordType::COMMITTING, raft_txn_id);
  if (!ls.ok()) {
    return ls;
  }
  CommitRequest cr;
  cr.set_raft_txn_id(raft_txn_id);
  cr.set_commit_ts(commit_ts);
  CommitResponse cresp;
  if (!CommitOnShard(shard_id, cr, &cresp)) {
    return Status::IOError("Commit RPC failed");
  }
  ls = AppendCoordinatorRecordWrite(CoordinatorLogRecordType::COMMITTED, raft_txn_id);
  if (!ls.ok()) {
    return ls;
  }
  return FlushCoordinatorLog();
}

Status Coordinator::TwoPhaseCommit(uint64_t raft_txn_id, std::vector<uint32_t> parts,
                                   uint64_t commit_ts) {
  Status ls = AppendCoordinatorRecordWrite(CoordinatorLogRecordType::PREPARING, raft_txn_id, parts);
  if (!ls.ok()) {
    return ls;
  }
  ls = FlushCoordinatorLog();
  if (!ls.ok()) {
    return ls;
  }
  std::vector<std::future<std::pair<uint32_t, PrepareResponse>>> prep_futs;
  prep_futs.reserve(parts.size());
  for (uint32_t sid : parts) {
    prep_futs.push_back(std::async(std::launch::async, [this, raft_txn_id, sid, commit_ts]() {
      PrepareResponse presp;
      PrepareRequest pr;
      pr.set_raft_txn_id(raft_txn_id);
      pr.set_commit_ts(commit_ts);
      (void)PrepareOnShard(sid, pr, &presp);
      return std::pair<uint32_t, PrepareResponse>{sid, std::move(presp)};
    }));
  }

  bool all_vote_commit = true;
  for (auto& fu : prep_futs) {
    auto [_sid, presp] = fu.get();
    (void)_sid;
    if (!presp.vote_commit()) {
      all_vote_commit = false;
      break;
    }
  }

  if (!all_vote_commit) {
    (void)AppendCoordinatorRecordWrite(CoordinatorLogRecordType::ABORTING, raft_txn_id);
    std::vector<std::future<void>> abort_futs;
    for (uint32_t sid : parts) {
      abort_futs.push_back(std::async(std::launch::async, [this, raft_txn_id, sid]() {
        AbortRequest ar;
        ar.set_raft_txn_id(raft_txn_id);
        AbortResponse aresp;
        (void)AbortOnShard(sid, ar, &aresp);
      }));
    }
    for (auto& f : abort_futs) {
      f.wait();
    }
    (void)AppendCoordinatorRecordWrite(CoordinatorLogRecordType::ABORTED, raft_txn_id);
    ls = FlushCoordinatorLog();
    if (!ls.ok()) {
      return ls;
    }
    return Status::Error(StatusCode::Conflict, "2PC prepare failed");
  }

  ls = AppendCoordinatorRecordWrite(CoordinatorLogRecordType::COMMITTING, raft_txn_id);
  if (!ls.ok()) {
    return ls;
  }
  std::vector<std::future<bool>> commit_futs;
  for (uint32_t sid : parts) {
    commit_futs.push_back(std::async(std::launch::async, [this, raft_txn_id, sid, commit_ts]() {
      CommitRequest cr;
      cr.set_raft_txn_id(raft_txn_id);
      cr.set_commit_ts(commit_ts);
      CommitResponse cresp;
      return CommitOnShard(sid, cr, &cresp);
    }));
  }
  for (auto& f : commit_futs) {
    if (!f.get()) {
      return Status::IOError("2PC commit failed");
    }
  }
  ls = AppendCoordinatorRecordWrite(CoordinatorLogRecordType::COMMITTED, raft_txn_id);
  if (!ls.ok()) {
    return ls;
  }
  return FlushCoordinatorLog();
}

Status Coordinator::Commit(uint64_t txn_id) {
  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    std::scoped_lock lk(mu_);
    txns_.erase(txn_id);
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  if (txn->participant_shards.empty()) {
    std::scoped_lock lk(mu_);
    txns_.erase(txn_id);
    return Status::OK();
  }
  std::vector<uint32_t> parts(txn->participant_shards.begin(), txn->participant_shards.end());
  const uint64_t raft_txn_id = txn->raft_txn_id;
  const uint64_t commit_ts = ts_oracle_.Now();

  Status out;
  if (parts.size() == 1) {
    out = SingleShardCommit(raft_txn_id, parts.front(), commit_ts);
  } else {
    out = TwoPhaseCommit(raft_txn_id, std::move(parts), commit_ts);
  }
  {
    std::scoped_lock lk(mu_);
    txns_.erase(txn_id);
  }
  return out;
}

Status Coordinator::Abort(uint64_t txn_id) {
  std::vector<uint32_t> parts;
  uint64_t raft_tid = 0;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::OK();
    }
    raft_tid = tit->second->raft_txn_id;
    parts.assign(tit->second->participant_shards.begin(), tit->second->participant_shards.end());
    txns_.erase(tit);
  }

  if (!parts.empty()) {
    (void)AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTING, raft_tid);
  }
  std::vector<std::future<void>> futs;
  for (uint32_t sid : parts) {
    futs.push_back(std::async(std::launch::async, [this, sid, raft_tid]() {
      ShardService::Stub* stub = this->GetStub(sid);
      if (!stub) {
        return;
      }
      grpc::ClientContext ctx;
      this->ApplyDeadline(&ctx);
      AbortRequest ar;
      ar.set_raft_txn_id(raft_tid);
      AbortResponse aresp;
      (void)stub->Abort(&ctx, ar, &aresp).ok();
    }));
  }
  for (auto& f : futs) {
    f.wait();
  }
  if (!parts.empty()) {
    (void)AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTED, raft_tid);
  }
  return Status::OK();
}

}  // namespace txndb
