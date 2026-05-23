#pragma once

#include "raft/raft_node.h"
#include "raft/raft_state_machine.h"
#include "raft/raft_transport.h"
#include "shard.grpc.pb.h"
#include "storage/mvcc_store.h"
#include "txn/lock_manager.h"
#include "txn/txn_manager.h"
#include "txn/wal.h"

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace txndb {

struct ReplicaStack {
  std::unique_ptr<MVCCStore> store;
  std::unique_ptr<WAL> wal;
  std::unique_ptr<LockManager> lock_mgr;
  std::unique_ptr<TxnManager> txn_mgr;
  std::unique_ptr<RaftStateMachine> state_machine;
  std::unique_ptr<RaftNode> raft_node;
};

class ShardServiceImpl final : public ShardService::Service {
public:
  struct PendingTxn {
    uint64_t raft_txn_id{0};
    uint64_t snapshot_ts{0};
    uint64_t local_txn_id{0};
    bool prepared_replicated{false};
  };

  ShardServiceImpl(uint32_t shard_id, const std::string& data_dir, uint32_t num_replicas = 3);
  ~ShardServiceImpl();

  void Start();
  void Stop();

  grpc::Status Execute(grpc::ServerContext* context, const ExecuteRequest* request,
                       ExecuteResponse* response) override;

  grpc::Status Prepare(grpc::ServerContext* context, const PrepareRequest* request,
                       PrepareResponse* response) override;

  grpc::Status Commit(grpc::ServerContext* context, const CommitRequest* request,
                      CommitResponse* response) override;

  grpc::Status Abort(grpc::ServerContext* context, const AbortRequest* request,
                     AbortResponse* response) override;
  grpc::Status QueryTxnState(grpc::ServerContext* context, const TxnStateRequest* request,
                             TxnStateResponse* response) override;

  MVCCStore* GetReplicaStore(uint32_t replica_id);
  TxnManager* GetReplicaTxnMgr(uint32_t replica_id);
  /// Leader's MVCC store (for tests).
  MVCCStore* GetLeaderStore();
  uint32_t GetLeaderReplicaId() const;
  uint32_t NumReplicas() const { return static_cast<uint32_t>(replicas_.size()); }
  void DisconnectReplicaForTest(uint32_t replica_id);
  void ReconnectReplicaForTest(uint32_t replica_id);

private:
  ReplicaStack* FindLeader();
  bool ProposeAndWait(RaftEntryType type, std::string payload,
                      std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));

  uint32_t shard_id_{0};
  std::string data_dir_;
  bool started_{false};

  InProcessTransport transport_;
  std::vector<std::unique_ptr<ReplicaStack>> replicas_;
  std::mutex pending_mu_;
  std::unordered_map<uint64_t, PendingTxn> pending_txns_;
  std::unordered_map<uint64_t, TxnStateCode> terminal_txn_states_;
};

class ShardServer {
public:
  ShardServer(uint32_t shard_id, const std::string& data_dir, const std::string& listen_addr,
              uint32_t num_replicas = 3);
  ~ShardServer();

  void Start();
  void Stop();
  void Wait();

  ShardServiceImpl* GetService() { return &service_; }

private:
  ShardServiceImpl service_;
  std::string listen_addr_;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace txndb
