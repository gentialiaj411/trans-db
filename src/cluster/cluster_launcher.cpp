#include "cluster/cluster_launcher.h"

#include "cluster/cluster_config.h"
#include "shard.grpc.pb.h"

#include <chrono>
#include <filesystem>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace txndb {

namespace {

std::string DefaultReplicaBinary() {
  const fs::path cwd = fs::current_path();
  const fs::path candidates[] = {
      cwd / "Release" / "trans_db_replica.exe",
      cwd / "trans_db_replica.exe",
      cwd / "Release" / "trans_db_replica",
      cwd / "trans_db_replica",
      cwd / "build" / "Release" / "trans_db_replica.exe",
      cwd / "build" / "Release" / "trans_db_replica",
      cwd.parent_path() / "build" / "Release" / "trans_db_replica.exe",
  };
  for (const auto& p : candidates) {
    if (fs::exists(p)) {
      return fs::absolute(p).string();
    }
  }
  return (cwd / "Release" / "trans_db_replica.exe").string();
}

bool ShardReplicaHasLeader(const std::string& addr) {
  auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
  auto stub = ShardService::NewStub(ch);
  grpc::ClientContext ctx;
  const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(2);
  ctx.set_deadline(deadline);
  ExecuteRequest req;
  req.set_raft_txn_id(0);
  req.set_op(OP_BEGIN);
  req.set_snapshot_ts(1);
  ExecuteResponse resp;
  if (!stub->Execute(&ctx, req, &resp).ok()) {
    return false;
  }
  return resp.ok();
}

void WaitForGrpcClusterReady(const ClusterTopology& topo, const std::string& host) {
  const auto replica_addrs = BuildShardReplicaAddresses(topo, host);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
  while (std::chrono::steady_clock::now() < deadline) {
    bool all_shards_ready = true;
    for (uint32_t s = 0; s < topo.num_shards; ++s) {
      const auto it = replica_addrs.find(s);
      if (it == replica_addrs.end() || it->second.empty()) {
        all_shards_ready = false;
        break;
      }
      bool shard_ready = false;
      for (const std::string& addr : it->second) {
        if (ShardReplicaHasLeader(addr)) {
          shard_ready = true;
          break;
        }
      }
      if (!shard_ready) {
        all_shards_ready = false;
        break;
      }
    }
    if (all_shards_ready) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

}  // namespace

ClusterLauncher::~ClusterLauncher() { Stop(); }

void ClusterLauncher::Start(ClusterTransport transport, const std::string& data_dir,
                             const ClusterTopology& topo, const std::string& host,
                             const std::string& replica_binary) {
  Stop();
  transport_ = transport;
  topo_ = topo;
  data_dir_ = data_dir;
  host_ = host;
  replica_binary_ = replica_binary.empty() ? DefaultReplicaBinary() : replica_binary;

  std::error_code ec;
  fs::create_directories(data_dir_, ec);

  if (transport_ == ClusterTransport::Grpc) {
    StartGrpcReplicas();
    WaitForGrpcClusterReady(topo_, host_);
  } else {
    StartInProcessShards();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  running_ = true;
}

void ClusterLauncher::StartInProcessShards() {
  inprocess_shards_.resize(topo_.num_shards);
  for (uint32_t s = 0; s < topo_.num_shards; ++s) {
    const std::string addr = HostPort(host_, ShardPort(topo_, s, 0));
    const std::string sdir = data_dir_ + "/shard" + std::to_string(s);
    auto server = std::make_unique<ShardServer>(s, sdir, addr, topo_.replicas_per_shard,
                                                ShardTransportMode::InProcess);
    server->Start();
    inprocess_shards_.push_back(std::move(server));
  }
}

void ClusterLauncher::StartGrpcReplicas() {
#ifndef _WIN32
  if (!fs::exists(replica_binary_)) {
    throw std::runtime_error("trans_db_replica binary not found: " + replica_binary_);
  }
#endif

  for (uint32_t s = 0; s < topo_.num_shards; ++s) {
    for (uint32_t r = 0; r < topo_.replicas_per_shard; ++r) {
      std::ostringstream peers;
      bool first = true;
      for (uint32_t p = 0; p < topo_.replicas_per_shard; ++p) {
        if (p == r) {
          continue;
        }
        if (!first) {
          peers << ",";
        }
        first = false;
        peers << p << "@" << HostPort(host_, RaftPort(topo_, s, p));
      }

      const std::string shard_listen = "0.0.0.0:" + std::to_string(ShardPort(topo_, s, r));
      const std::string raft_listen = "0.0.0.0:" + std::to_string(RaftPort(topo_, s, r));
      const std::string rep_dir =
          data_dir_ + "/shard" + std::to_string(s) + "_rep" + std::to_string(r);

      std::ostringstream args;
      args << " --shard " << s << " --replica " << r << " --data-dir \"" << rep_dir << "\""
           << " --shard-listen " << shard_listen << " --raft-listen " << raft_listen
           << " --raft-peers \"" << peers.str() << "\"";
      const char* gc_window = std::getenv("TRANS_DB_REPLICA_GROUP_COMMIT_WINDOW_US");
      if (gc_window == nullptr || gc_window[0] == '\0') {
        gc_window = std::getenv("TRANS_DB_GROUP_COMMIT_WINDOW_US");
      }
      if (gc_window != nullptr && gc_window[0] != '\0') {
        args << " --gc-window-us " << gc_window;
      }
      if (const char* gc_batch = std::getenv("TRANS_DB_GROUP_COMMIT_BATCH");
          gc_batch != nullptr && gc_batch[0] != '\0') {
        args << " --gc-batch " << gc_batch;
      }

#ifdef _WIN32
      STARTUPINFOA si{};
      si.cb = sizeof(si);
      PROCESS_INFORMATION pi{};
      std::string cmdline = "\"" + replica_binary_ + "\"" + args.str();
      std::vector<char> cmd_buf(cmdline.begin(), cmdline.end());
      cmd_buf.push_back('\0');
      const std::string work_dir = fs::path(replica_binary_).parent_path().string();
      if (!CreateProcessA(replica_binary_.c_str(), cmd_buf.data(), nullptr, nullptr, FALSE,
                          CREATE_NO_WINDOW, nullptr, work_dir.empty() ? nullptr : work_dir.c_str(),
                          &si, &pi)) {
        throw std::runtime_error("CreateProcess failed for trans_db_replica: " +
                                 replica_binary_);
      }
      CloseHandle(pi.hThread);
      ReplicaProc h;
      h.shard_id = s;
      h.replica_id = r;
      h.process = pi.hProcess;
      children_.push_back(h);
#else
      const pid_t pid = fork();
      if (pid == 0) {
        execlp(replica_binary_.c_str(), replica_binary_.c_str(), "--shard",
               std::to_string(s).c_str(), "--replica", std::to_string(r).c_str(), "--data-dir",
               rep_dir.c_str(), "--shard-listen", shard_listen.c_str(), "--raft-listen",
               raft_listen.c_str(), "--raft-peers", peers.str().c_str(), nullptr);
        _exit(127);
      }
      if (pid < 0) {
        throw std::runtime_error("fork failed for trans_db_replica");
      }
      ReplicaProc h;
      h.shard_id = s;
      h.replica_id = r;
      h.pid = pid;
      children_.push_back(h);
#endif
    }
  }
}

void ClusterLauncher::Stop() {
  if (!running_) {
    return;
  }

  for (auto& s : inprocess_shards_) {
    if (s) {
      s->Stop();
    }
  }
  inprocess_shards_.clear();

  for (auto& child : children_) {
#ifdef _WIN32
    if (child.process) {
      TerminateProcess(static_cast<HANDLE>(child.process), 0);
      WaitForSingleObject(static_cast<HANDLE>(child.process), 5000);
      CloseHandle(static_cast<HANDLE>(child.process));
      child.process = nullptr;
    }
#else
    if (child.pid > 0) {
      kill(child.pid, SIGTERM);
      int status = 0;
      waitpid(child.pid, &status, 0);
      child.pid = -1;
    }
#endif
  }
  children_.clear();
  running_ = false;
}

std::unordered_map<uint32_t, std::string> ClusterLauncher::shard_entry_addresses() const {
  return BuildShardEntryAddresses(topo_, host_);
}

std::unordered_map<uint32_t, std::vector<std::string>> ClusterLauncher::shard_replica_addresses()
    const {
  return BuildShardReplicaAddresses(topo_, host_);
}

ShardServer* ClusterLauncher::inprocess_shard(uint32_t shard_id) const {
  if (shard_id >= inprocess_shards_.size()) {
    return nullptr;
  }
  return inprocess_shards_[shard_id].get();
}

void ClusterLauncher::PartitionPeer(uint32_t shard_id, uint32_t peer_replica_id, bool partitioned) {
  if (transport_ != ClusterTransport::Grpc) {
    if (shard_id < inprocess_shards_.size() && inprocess_shards_[shard_id]) {
      inprocess_shards_[shard_id]->GetService()->SetRaftPeerPartitionForTest(peer_replica_id,
                                                                            partitioned);
    }
    return;
  }

  const auto addrs = BuildShardReplicaAddresses(topo_, host_);
  const auto it = addrs.find(shard_id);
  if (it == addrs.end()) {
    return;
  }
  for (const auto& addr : it->second) {
    auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub = ShardService::NewStub(ch);
    grpc::ClientContext ctx;
    SetRaftPeerPartitionRequest req;
    req.set_peer_replica_id(peer_replica_id);
    req.set_partitioned(partitioned);
    SetRaftPeerPartitionResponse resp;
    (void)stub->SetRaftPeerPartition(&ctx, req, &resp);
  }
}

void ClusterLauncher::StopShard(uint32_t shard_id) {
  if (transport_ == ClusterTransport::InProcess) {
    if (shard_id < inprocess_shards_.size() && inprocess_shards_[shard_id]) {
      inprocess_shards_[shard_id]->Stop();
      inprocess_shards_[shard_id].reset();
    }
    return;
  }

  for (auto it = children_.begin(); it != children_.end();) {
    if (it->shard_id != shard_id) {
      ++it;
      continue;
    }
#ifdef _WIN32
    if (it->process) {
      TerminateProcess(static_cast<HANDLE>(it->process), 0);
      WaitForSingleObject(static_cast<HANDLE>(it->process), 5000);
      CloseHandle(static_cast<HANDLE>(it->process));
    }
#else
    if (it->pid > 0) {
      kill(it->pid, SIGTERM);
      int status = 0;
      waitpid(it->pid, &status, 0);
    }
#endif
    it = children_.erase(it);
  }
}

void ClusterLauncher::StartShard(uint32_t shard_id) {
  if (transport_ == ClusterTransport::InProcess) {
    if (shard_id < inprocess_shards_.size() && !inprocess_shards_[shard_id]) {
      const std::string addr = HostPort(host_, ShardPort(topo_, shard_id, 0));
      const std::string sdir = data_dir_ + "/shard" + std::to_string(shard_id);
      auto server = std::make_unique<ShardServer>(shard_id, sdir, addr, topo_.replicas_per_shard,
                                                  ShardTransportMode::InProcess);
      server->Start();
      inprocess_shards_[shard_id] = std::move(server);
    }
    return;
  }

  for (uint32_t r = 0; r < topo_.replicas_per_shard; ++r) {
    std::ostringstream peers;
    bool first = true;
    for (uint32_t p = 0; p < topo_.replicas_per_shard; ++p) {
      if (p == r) {
        continue;
      }
      if (!first) {
        peers << ",";
      }
      first = false;
      peers << p << "@" << HostPort(host_, RaftPort(topo_, shard_id, p));
    }

    const std::string shard_listen = "0.0.0.0:" + std::to_string(ShardPort(topo_, shard_id, r));
    const std::string raft_listen = "0.0.0.0:" + std::to_string(RaftPort(topo_, shard_id, r));
    const std::string rep_dir =
        data_dir_ + "/shard" + std::to_string(shard_id) + "_rep" + std::to_string(r);

    std::ostringstream args;
    args << " --shard " << shard_id << " --replica " << r << " --data-dir \"" << rep_dir << "\""
         << " --shard-listen " << shard_listen << " --raft-listen " << raft_listen
         << " --raft-peers \"" << peers.str() << "\"";

#ifdef _WIN32
    STARTUPINFOA si{};
    si.cb = sizeof(si);
    PROCESS_INFORMATION pi{};
    std::string cmdline = "\"" + replica_binary_ + "\"" + args.str();
    std::vector<char> cmd_buf(cmdline.begin(), cmdline.end());
    cmd_buf.push_back('\0');
    const std::string work_dir = fs::path(replica_binary_).parent_path().string();
    if (!CreateProcessA(replica_binary_.c_str(), cmd_buf.data(), nullptr, nullptr, FALSE,
                        CREATE_NO_WINDOW, nullptr, work_dir.empty() ? nullptr : work_dir.c_str(),
                        &si, &pi)) {
      throw std::runtime_error("CreateProcess failed for trans_db_replica");
    }
    CloseHandle(pi.hThread);
    ReplicaProc h;
    h.shard_id = shard_id;
    h.replica_id = r;
    h.process = pi.hProcess;
    children_.push_back(h);
#else
    const pid_t pid = fork();
    if (pid == 0) {
      execlp(replica_binary_.c_str(), replica_binary_.c_str(), "--shard",
             std::to_string(shard_id).c_str(), "--replica", std::to_string(r).c_str(), "--data-dir",
             rep_dir.c_str(), "--shard-listen", shard_listen.c_str(), "--raft-listen",
             raft_listen.c_str(), "--raft-peers", peers.str().c_str(), nullptr);
      _exit(127);
    }
    if (pid < 0) {
      throw std::runtime_error("fork failed for trans_db_replica");
    }
    ReplicaProc h;
    h.shard_id = shard_id;
    h.replica_id = r;
    h.pid = pid;
    children_.push_back(h);
#endif
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(800));
}

}  // namespace txndb
