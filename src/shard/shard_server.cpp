#include "shard/shard_server.h"

#include "storage/status.h"

#include <filesystem>
#include <stdexcept>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace txndb {

namespace {

std::string JoinPath(std::string_view a, std::string_view b) {
  fs::path p(a);
  p /= fs::path(b);
  return p.string();
}

uint32_t ErrorCode(StatusCode c) { return static_cast<uint32_t>(c); }

}  // namespace

ShardServiceImpl::ShardServiceImpl(uint32_t shard_id, const std::string& data_dir, uint32_t num_replicas)
    : shard_id_(shard_id), data_dir_(data_dir) {
  std::error_code ec;
  fs::create_directories(data_dir_, ec);

  replicas_.reserve(num_replicas);
  std::ostringstream prefix;
  prefix << data_dir << "/shard" << shard_id << "_rep";

  for (uint32_t i = 0; i < num_replicas; ++i) {
    auto stack = std::make_unique<ReplicaStack>();
    const std::string base = prefix.str() + std::to_string(i);
    const std::string rock = JoinPath(base, "db");
    const std::string walp = JoinPath(base, "wal");
    fs::create_directories(rock, ec);

    if (!MVCCStore::Open(rock, &stack->store).ok()) {
      throw std::runtime_error("MVCCStore::Open failed");
    }
    if (!WAL::Open(walp, &stack->wal).ok()) {
      throw std::runtime_error("WAL::Open failed");
    }
    stack->lock_mgr = std::make_unique<LockManager>();
    stack->txn_mgr =
        std::make_unique<TxnManager>(stack->store.get(), stack->wal.get(), stack->lock_mgr.get());
    if (!stack->txn_mgr->Recover().ok()) {
      throw std::runtime_error("TxnManager::Recover failed");
    }
    stack->state_machine = std::make_unique<RaftStateMachine>(stack->txn_mgr.get());

    std::vector<uint32_t> peers;
    for (uint32_t j = 0; j < num_replicas; ++j) {
      if (j != i) {
        peers.push_back(j);
      }
    }

    stack->raft_node = std::make_unique<RaftNode>(i, std::move(peers), &transport_,
                                                  stack->state_machine->WrapApply());
    transport_.RegisterNode(i, stack->raft_node.get());

    replicas_.push_back(std::move(stack));
  }
}

ShardServiceImpl::~ShardServiceImpl() {
  Stop();
}

void ShardServiceImpl::Start() {
  if (started_) {
    return;
  }
  for (auto& r : replicas_) {
    r->raft_node->Start();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  started_ = true;
}

void ShardServiceImpl::Stop() {
  if (!started_) {
    return;
  }
  for (auto& r : replicas_) {
    if (r->raft_node) {
      r->raft_node->Stop();
    }
  }
  started_ = false;
}

ReplicaStack* ShardServiceImpl::FindLeader() {
  for (auto& r : replicas_) {
    if (r->raft_node && r->raft_node->GetRole() == RaftRole::LEADER) {
      return r.get();
    }
  }
  return nullptr;
}

bool ShardServiceImpl::ProposeAndWait(RaftEntryType type, std::string payload,
                                      std::chrono::milliseconds timeout) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    return false;
  }
  const uint64_t ix = L->raft_node->Propose(type, std::move(payload));
  if (ix == 0) {
    return false;
  }
  return L->raft_node->WaitForCommit(ix, timeout);
}

MVCCStore* ShardServiceImpl::GetReplicaStore(uint32_t replica_id) {
  if (replica_id >= replicas_.size()) {
    return nullptr;
  }
  return replicas_[replica_id]->store.get();
}

TxnManager* ShardServiceImpl::GetReplicaTxnMgr(uint32_t replica_id) {
  if (replica_id >= replicas_.size()) {
    return nullptr;
  }
  return replicas_[replica_id]->txn_mgr.get();
}

MVCCStore* ShardServiceImpl::GetLeaderStore() {
  ReplicaStack* L = FindLeader();
  return L ? L->store.get() : nullptr;
}

grpc::Status ShardServiceImpl::Execute(grpc::ServerContext* /*context*/, const ExecuteRequest* request,
                                       ExecuteResponse* response) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    response->set_ok(false);
    response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
    response->set_error_message("no raft leader available");
    return grpc::Status::OK;
  }

  switch (static_cast<int>(request->op())) {
    case 0: /* OP_BEGIN */ {
      const std::string payload = RaftStateMachine::PackTxnPayload(
          WAL::SerializeBeginPayload(request->snapshot_ts()), request->raft_txn_id());
      if (!ProposeAndWait(RaftEntryType::TXN_BEGIN, std::move(payload))) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(StatusCode::IOError));
        response->set_error_message("TXN_BEGIN not committed");
        return grpc::Status::OK;
      }
      response->set_ok(true);
      return grpc::Status::OK;
    }
    case 1: /* OP_READ */ {
      const uint64_t local = L->state_machine->GetLocalTxnId(request->raft_txn_id());
      if (local == 0) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
        response->set_error_message("txn not begun on shard");
        return grpc::Status::OK;
      }
      std::string val;
      const Status rs = L->txn_mgr->Read(local, request->table_id(), request->key(), &val);
      if (!rs.ok()) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(rs.code()));
        response->set_error_message(rs.message());
        return grpc::Status::OK;
      }
      response->set_ok(true);
      response->set_value(std::move(val));
      return grpc::Status::OK;
    }
    case 2: /* OP_WRITE */ {
      const std::string payload =
          RaftStateMachine::PackTxnPayload(WAL::SerializeWritePayload(request->table_id(), request->key(),
                                                                      request->value(), 0),
                                           request->raft_txn_id());
      if (!ProposeAndWait(RaftEntryType::TXN_WRITE, std::move(payload))) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(StatusCode::IOError));
        response->set_error_message("TXN_WRITE not committed");
        return grpc::Status::OK;
      }
      response->set_ok(true);
      return grpc::Status::OK;
    }
    case 3: /* OP_DELETE */ {
      const std::string payload = RaftStateMachine::PackTxnPayload(
          WAL::SerializeDeletePayload(request->table_id(), request->key(), 0),
          request->raft_txn_id());
      if (!ProposeAndWait(RaftEntryType::TXN_DELETE, std::move(payload))) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(StatusCode::IOError));
        response->set_error_message("TXN_DELETE not committed");
        return grpc::Status::OK;
      }
      response->set_ok(true);
      return grpc::Status::OK;
    }
    case 4: /* OP_SCAN */ {
      const uint64_t local = L->state_machine->GetLocalTxnId(request->raft_txn_id());
      if (local == 0) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
        response->set_error_message("txn not begun on shard");
        return grpc::Status::OK;
      }
      std::vector<std::pair<std::string, std::string>> rows;
      const Status rs = L->txn_mgr->Scan(local, request->table_id(), request->range_start_pk(),
                                         request->range_end_exclusive(),
                                         request->range_end_open(), &rows);
      if (!rs.ok()) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(rs.code()));
        response->set_error_message(rs.message());
        return grpc::Status::OK;
      }
      response->set_ok(true);
      for (auto& pr : rows) {
        ScanRow* sr = response->add_scan_rows();
        sr->set_pk(std::move(pr.first));
        sr->set_row_value(std::move(pr.second));
      }
      return grpc::Status::OK;
    }
    default:
      response->set_ok(false);
      response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
      response->set_error_message("unknown op");
      return grpc::Status::OK;
  }
}

grpc::Status ShardServiceImpl::Prepare(grpc::ServerContext* /*context*/, const PrepareRequest* request,
                                       PrepareResponse* response) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    response->set_vote_commit(false);
    response->set_error_message("no leader");
    return grpc::Status::OK;
  }
  const uint64_t tid = request->raft_txn_id();
  auto payload = RaftStateMachine::PackTxnPayload(WAL::SerializePreparePayload(request->commit_ts()), tid);
  if (!ProposeAndWait(RaftEntryType::TXN_PREPARE, std::move(payload))) {
    response->set_vote_commit(false);
    response->set_error_message("prepare not replicated");
    return grpc::Status::OK;
  }
  auto pr = L->state_machine->TakePrepareResult(tid);
  if (!pr.has_value()) {
    response->set_vote_commit(false);
    response->set_error_message("missing prepare result");
    return grpc::Status::OK;
  }
  response->set_vote_commit(pr->success);
  response->set_error_message(pr->error);
  return grpc::Status::OK;
}

grpc::Status ShardServiceImpl::Commit(grpc::ServerContext* /*context*/, const CommitRequest* request,
                                      CommitResponse* response) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    response->set_ok(false);
    response->set_error_message("no leader");
    return grpc::Status::OK;
  }
  auto payload = RaftStateMachine::PackTxnPayload(WAL::SerializeCommitPayload(request->commit_ts()),
                                                  request->raft_txn_id());
  response->set_ok(ProposeAndWait(RaftEntryType::TXN_COMMIT, std::move(payload)));
  return grpc::Status::OK;
}

grpc::Status ShardServiceImpl::Abort(grpc::ServerContext* /*context*/, const AbortRequest* request,
                                     AbortResponse* response) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    response->set_ok(true);  // idempotent teardown
    return grpc::Status::OK;
  }
  const auto payload = RaftStateMachine::PackAbortPayload(request->raft_txn_id());
  response->set_ok(ProposeAndWait(RaftEntryType::TXN_ABORT, std::string(payload)));
  return grpc::Status::OK;
}

ShardServer::ShardServer(uint32_t shard_id, const std::string& data_dir,
                         const std::string& listen_addr)
    : service_(shard_id, data_dir), listen_addr_(listen_addr) {}

ShardServer::~ShardServer() {
  Stop();
}

void ShardServer::Start() {
  service_.Start();
  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen_addr_, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);
  server_ = builder.BuildAndStart();
}

void ShardServer::Stop() {
  if (server_) {
    server_->Shutdown();
    server_->Wait();
    server_.reset();
  }
  service_.Stop();
}

void ShardServer::Wait() {
  if (server_) {
    server_->Wait();
  }
}

}  // namespace txndb
