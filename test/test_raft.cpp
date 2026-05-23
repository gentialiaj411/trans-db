#include <gtest/gtest.h>

#include "raft/raft_state_machine.h"
#include "raft/raft_transport.h"
#include "raft/raft_node.h"
#include "storage/mvcc_store.h"
#include "txn/wal.h"
#include "txn/lock_manager.h"
#include "txn/txn_manager.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using namespace txndb;
using RaftSteadyClk = std::chrono::steady_clock;

static std::string UniquePath(std::string_view suffix) {
  auto base = fs::temp_directory_path() / "trans_db_raft_test";
  std::error_code ec;
  fs::create_directories(base, ec);
  return (base / (std::string(suffix) + "_" + std::to_string(std::rand()))).string();
}

static void Nuke(const std::string& p) {
  std::error_code ec;
  fs::remove_all(p, ec);
}

static RaftNode* WaitForLeader(std::vector<RaftNode*>& nodes, std::chrono::milliseconds timeout) {
  const RaftSteadyClk::time_point deadline = RaftSteadyClk::now() + timeout;
  while (RaftSteadyClk::now() < deadline) {
    for (RaftNode* n : nodes) {
      if (n->GetRole() == RaftRole::LEADER) {
        return n;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  return nullptr;
}

static RaftNode* AnyLeaderExcept(std::vector<RaftNode*>& nodes, uint32_t disconnected_id,
                                 std::chrono::milliseconds timeout) {
  const RaftSteadyClk::time_point deadline = RaftSteadyClk::now() + timeout;
  while (RaftSteadyClk::now() < deadline) {
    for (RaftNode* n : nodes) {
      if (n->GetNodeId() == disconnected_id) {
        continue;
      }
      if (n->GetRole() == RaftRole::LEADER) {
        return n;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  return nullptr;
}

static uint64_t FirstLogIndexSkippingNoop(RaftLog const& lg) {
  uint64_t i = 1;
  const uint64_t last = lg.LastIndex();
  while (i <= last) {
    auto e = lg.Get(i);
    if (!e) {
      break;
    }
    if (e->type != RaftEntryType::NOOP) {
      return i;
    }
    ++i;
  }
  return 0;
}

struct Cluster {
  InProcessTransport transport;
  std::vector<std::string> store_paths;
  std::vector<std::string> wal_paths;
  std::vector<std::unique_ptr<MVCCStore>> stores;
  std::vector<std::unique_ptr<WAL>> wals;
  std::vector<std::unique_ptr<LockManager>> locks;
  std::vector<std::unique_ptr<TxnManager>> txn_mgr;
  std::vector<std::unique_ptr<RaftStateMachine>> sms;
  std::vector<std::unique_ptr<RaftNode>> nodes;
  std::vector<RaftNode*> ptrs;

  ~Cluster() {
    for (auto& n : nodes) {
      if (n) {
        n->Stop();
      }
    }
    for (auto& p : store_paths) {
      Nuke(p);
    }
    for (auto& p : wal_paths) {
      Nuke(p);
    }
  }

  void Init() {
    for (uint32_t id : {0u, 1u, 2u}) {
      store_paths.push_back(UniquePath("rst"));
      wal_paths.push_back(UniquePath("walr"));
      Nuke(store_paths.back());
      Nuke(wal_paths.back());

      stores.emplace_back();
      wals.emplace_back();
      locks.push_back(std::make_unique<LockManager>());
      ASSERT_TRUE(MVCCStore::Open(store_paths.back(), &stores.back()).ok());
      ASSERT_TRUE(WAL::Open(wal_paths.back(), &wals.back()).ok());
      txn_mgr.push_back(std::make_unique<TxnManager>(stores.back().get(), wals.back().get(),
                                                       locks.back().get()));
      sms.push_back(std::make_unique<RaftStateMachine>(txn_mgr.back().get()));
    }

    for (uint32_t id : {0u, 1u, 2u}) {
      std::vector<uint32_t> peers;
      for (uint32_t o : {0u, 1u, 2u}) {
        if (o != id) {
          peers.push_back(o);
        }
      }
      RaftNode* self = nodes.emplace_back(std::make_unique<RaftNode>(
                               id, std::move(peers), &transport, sms[id]->WrapApply()))
                         .get();
      transport.RegisterNode(id, self);
    }
    ptrs = {nodes[0].get(), nodes[1].get(), nodes[2].get()};
    for (auto* n : ptrs) {
      n->Start();
    }
  }
};

TEST(RaftCluster, LeaderElection) {
  Cluster c;
  c.Init();
  RaftNode* lead = WaitForLeader(c.ptrs, std::chrono::seconds(2));
  ASSERT_NE(lead, nullptr);
  uint64_t lt = lead->GetCurrentTerm();
  for (RaftNode* n : c.ptrs) {
    ASSERT_EQ(lt, n->GetCurrentTerm());
    RaftRole r = n->GetRole();
    ASSERT_TRUE(r == RaftRole::LEADER || r == RaftRole::FOLLOWER);
  }
  EXPECT_EQ(static_cast<int>(std::count_if(c.ptrs.begin(), c.ptrs.end(),
                                           [](RaftNode* n) {
                                             return n->GetRole() == RaftRole::LEADER;
                                           })),
            1);
}

TEST(RaftCluster, LeaderElectionAfterDisconnect) {
  Cluster c;
  c.Init();
  RaftNode* lead = WaitForLeader(c.ptrs, std::chrono::seconds(3));
  ASSERT_NE(lead, nullptr);
  uint64_t old_term = lead->GetCurrentTerm();
  uint32_t dead = lead->GetNodeId();
  c.transport.DisconnectNode(dead);
  std::this_thread::sleep_for(std::chrono::milliseconds(550));
  RaftNode* n2 = AnyLeaderExcept(c.ptrs, dead, std::chrono::seconds(3));
  ASSERT_NE(n2, nullptr);
  ASSERT_GT(n2->GetCurrentTerm(), old_term);
}

TEST(RaftCluster, ProposeAndCommit) {
  Cluster c;
  c.Init();
  RaftNode* lead = WaitForLeader(c.ptrs, std::chrono::seconds(3));
  ASSERT_NE(lead, nullptr);
  uint64_t r = 777;
  auto p_begin = RaftStateMachine::PackTxnPayload(WAL::SerializeBeginPayload(80), r);
  uint64_t ix1 = lead->Propose(RaftEntryType::TXN_BEGIN, std::move(p_begin));
  EXPECT_GT(ix1, 0u);

  auto p_wr = RaftStateMachine::PackTxnPayload(
      WAL::SerializeWritePayload(1, "k", "v", 0), r);

  uint64_t ix2 = lead->Propose(RaftEntryType::TXN_WRITE, std::move(p_wr));

  auto p_prep = RaftStateMachine::PackTxnPayload(WAL::SerializePreparePayload(90), r);
  uint64_t ix3 = lead->Propose(RaftEntryType::TXN_PREPARE, std::move(p_prep));

  auto p_cm = RaftStateMachine::PackTxnPayload(WAL::SerializeCommitPayload(90), r);
  uint64_t ix4 = lead->Propose(RaftEntryType::TXN_COMMIT, std::move(p_cm));

  ASSERT_GT(ix2, ix1);
  ASSERT_GT(ix3, ix2);
  ASSERT_GT(ix4, ix3);

  for (RaftNode* n : c.ptrs) {
    ASSERT_TRUE(n->WaitForCommit(ix4, std::chrono::seconds(5)));
  }

  for (MVCCStore* st : {c.stores[0].get(), c.stores[1].get(), c.stores[2].get()}) {
    std::string v;
    uint64_t ts = 0;
    ASSERT_TRUE(st->Get(1, "k", 100, &v, &ts).ok());
    EXPECT_EQ(v, "v");
    EXPECT_EQ(ts, 90u);
  }
}

TEST(RaftCluster, ProposeOnFollowerFails) {
  Cluster c;
  c.Init();
  RaftNode* lead = WaitForLeader(c.ptrs, std::chrono::seconds(3));
  ASSERT_NE(lead, nullptr);
  RaftNode* fol = nullptr;
  for (RaftNode* n : c.ptrs) {
    if (n != lead) {
      fol = n;
      break;
    }
  }
  ASSERT_NE(fol, nullptr);
  EXPECT_EQ(fol->Propose(RaftEntryType::TXN_BEGIN, RaftStateMachine::PackTxnPayload(
                                                      WAL::SerializeBeginPayload(1), 42)),
            0u);
}

TEST(RaftCluster, LogReplicationToFollowers) {
  Cluster c;
  c.Init();
  RaftNode* lead = WaitForLeader(c.ptrs, std::chrono::seconds(3));
  ASSERT_NE(lead, nullptr);
  uint64_t last_ix = lead->GetLog().LastIndex();

  uint64_t r = 10;
  for (int i = 0; i < 10; ++i, ++r) {
    auto pl = RaftStateMachine::PackTxnPayload(WAL::SerializeBeginPayload(5 + static_cast<unsigned>(i)), r);
    uint64_t ix = lead->Propose(RaftEntryType::TXN_BEGIN, std::move(pl));
    ASSERT_GT(ix, last_ix);
    ASSERT_TRUE(lead->WaitForCommit(ix, std::chrono::seconds(3)));
    for (RaftNode* n : c.ptrs) {
      ASSERT_TRUE(n->WaitForCommit(ix, std::chrono::seconds(5)));
    }
    last_ix = ix;
    std::this_thread::sleep_for(std::chrono::milliseconds(35));
  }

  uint64_t s0 = FirstLogIndexSkippingNoop(lead->GetLog());

  ASSERT_GT(s0, 0u);
  for (int i : {1, 2}) {
    const auto vr = lead->GetLog().GetRange(s0, last_ix);
    const auto vn = c.ptrs[i]->GetLog().GetRange(s0, last_ix);
    ASSERT_EQ(vr.size(), vn.size());
    for (size_t j = 0; j < vr.size(); ++j) {
      EXPECT_EQ(vr[j].term, vn[j].term);
      EXPECT_EQ(vr[j].type, vn[j].type);
      EXPECT_EQ(vr[j].payload, vn[j].payload);
    }
  }
}

TEST(RaftCluster, LeaderFailoverPreservesCommitted) {
  Cluster c;
  c.Init();
  RaftNode* lead = WaitForLeader(c.ptrs, std::chrono::seconds(3));
  ASSERT_NE(lead, nullptr);
  uint32_t old_leader = lead->GetNodeId();
  uint64_t r_base = 200;
  auto propose_n = [&](int n_ent, RaftNode* L, uint64_t& r_tid) -> uint64_t {
    uint64_t last_ix = 0;
    for (int i = 0; i < n_ent; ++i, ++r_tid) {
      auto pl = RaftStateMachine::PackTxnPayload(WAL::SerializeBeginPayload(3), r_tid);
      last_ix = L->Propose(RaftEntryType::TXN_BEGIN, std::move(pl));
      if (last_ix == 0u) {
        return 0u;
      }
    }
    return last_ix;
  };

  uint64_t raft_tid = r_base;
  uint64_t end1 = propose_n(5, lead, raft_tid);
  ASSERT_GT(end1, 0u);

  for (RaftNode* n : c.ptrs) {
    ASSERT_TRUE(n->WaitForCommit(end1, std::chrono::seconds(6)));
  }

  uint64_t old_term = lead->GetCurrentTerm();
  c.transport.DisconnectNode(old_leader);
  std::this_thread::sleep_for(std::chrono::milliseconds(550));

  RaftNode* nl = AnyLeaderExcept(c.ptrs, old_leader, std::chrono::seconds(4));
  ASSERT_NE(nl, nullptr);
  ASSERT_GE(nl->GetCurrentTerm(), old_term);

  uint64_t end2 = propose_n(5, nl, raft_tid);
  ASSERT_GT(end2, 0u);

  RaftNode* other_survivor = nullptr;
  for (RaftNode* n : c.ptrs) {
    if (n->GetNodeId() != old_leader && n->GetNodeId() != nl->GetNodeId()) {
      other_survivor = n;
      break;
    }
  }

  ASSERT_TRUE(nl->WaitForCommit(end2, std::chrono::seconds(6)));
  if (other_survivor != nullptr) {
    ASSERT_TRUE(other_survivor->WaitForCommit(end2, std::chrono::seconds(6)));
    uint64_t s1 = FirstLogIndexSkippingNoop(nl->GetLog());
    uint64_t s2 = FirstLogIndexSkippingNoop(other_survivor->GetLog());
    uint64_t s = std::min(s1, s2);
    const auto lg1 = nl->GetLog().GetRange(s, end2);
    const auto lg2 = other_survivor->GetLog().GetRange(s, end2);
    ASSERT_EQ(lg1.size(), lg2.size());
    for (size_t i = 0; i < lg1.size(); ++i) {
      EXPECT_EQ(lg1[i].type, lg2[i].type);
      EXPECT_EQ(lg1[i].payload, lg2[i].payload);
      EXPECT_EQ(lg1[i].term, lg2[i].term);
      EXPECT_EQ(lg1[i].index, lg2[i].index);
    }
  }
}

TEST(RaftCluster, DisconnectedNodeCatchesUp) {
  Cluster c;
  c.Init();
  RaftNode* lead = WaitForLeader(c.ptrs, std::chrono::seconds(3));
  ASSERT_NE(lead, nullptr);

  auto propose_batch = [&](RaftNode* L, uint64_t raft_tid_begin, int count) -> uint64_t {
    uint64_t last_ix = 0;
    uint64_t r = raft_tid_begin;
    for (int i = 0; i < count; ++i, ++r) {
      auto pl =
          RaftStateMachine::PackTxnPayload(WAL::SerializeBeginPayload(7 + static_cast<unsigned>(i)), r);
      last_ix = L->Propose(RaftEntryType::TXN_BEGIN, std::move(pl));
      if (last_ix == 0u) {
        return 0u;
      }
    }
    return last_ix;
  };

  uint64_t ix5 = propose_batch(lead, 300, 5);
  ASSERT_GT(ix5, 0u);
  for (RaftNode* n : c.ptrs) {
    ASSERT_TRUE(n->WaitForCommit(ix5, std::chrono::seconds(5)));
  }

  c.transport.DisconnectNode(2);
  std::this_thread::sleep_for(std::chrono::milliseconds(550));

  RaftNode* newl = AnyLeaderExcept(c.ptrs, 2, std::chrono::seconds(5));
  ASSERT_NE(newl, nullptr);

  uint64_t ix10 = propose_batch(newl, 310, 5);
  ASSERT_GT(ix10, 0u);

  for (RaftNode* n : c.ptrs) {
    if (n->GetNodeId() == 2u) {
      continue;
    }
    ASSERT_TRUE(n->WaitForCommit(ix10, std::chrono::seconds(6)));
  }

  c.transport.ReconnectNode(2, c.nodes[2].get());
  std::this_thread::sleep_for(std::chrono::milliseconds(550));
  ASSERT_TRUE(c.nodes[2]->WaitForCommit(ix10, std::chrono::seconds(8)));

  RaftNode* verify = WaitForLeader(c.ptrs, std::chrono::seconds(4));
  ASSERT_NE(verify, nullptr);
  ASSERT_EQ(c.nodes[2]->GetCommitIndex(), verify->GetCommitIndex());
  ASSERT_EQ(c.nodes[2]->GetLog().LastIndex(), verify->GetLog().LastIndex());

  uint64_t s1 = FirstLogIndexSkippingNoop(verify->GetLog());
  uint64_t s2 = FirstLogIndexSkippingNoop(c.nodes[2]->GetLog());
  uint64_t s = std::min(s1, s2);
  const auto lg1 = verify->GetLog().GetRange(s, ix10);
  const auto lg2 = c.nodes[2]->GetLog().GetRange(s, ix10);
  ASSERT_EQ(lg1.size(), lg2.size());
  for (size_t i = 0; i < lg1.size(); ++i) {
    EXPECT_EQ(lg1[i].type, lg2[i].type);
    EXPECT_EQ(lg1[i].payload, lg2[i].payload);
    EXPECT_EQ(lg1[i].term, lg2[i].term);
    EXPECT_EQ(lg1[i].index, lg2[i].index);
  }
}

TEST(RaftCluster, ConcurrentProposals) {
  Cluster c;
  c.Init();
  RaftNode* lead = WaitForLeader(c.ptrs, std::chrono::seconds(4));
  ASSERT_NE(lead, nullptr);

  std::vector<uint64_t> indices(5);
  std::vector<std::thread> threads;
  for (uint32_t tid = 0; tid < 5; ++tid) {
    threads.emplace_back([&c, lead, tid, &indices]() {
      const uint64_t raft_id = 901u + static_cast<uint64_t>(tid);
      auto pl = RaftStateMachine::PackTxnPayload(WAL::SerializeBeginPayload(11 + tid), raft_id);

      const uint64_t ix = lead->Propose(RaftEntryType::TXN_BEGIN, std::move(pl));
      indices[tid] = ix;
      EXPECT_GT(ix, 0u);
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  uint64_t max_ix = indices[0];
  for (uint64_t ix : indices) {
    ASSERT_GT(ix, 0u);
    max_ix = std::max(max_ix, ix);
  }
  for (RaftNode* n : c.ptrs) {
    ASSERT_TRUE(n->WaitForCommit(max_ix, std::chrono::seconds(15)));
  }
  std::sort(indices.begin(), indices.end());
  for (size_t i = 1; i < indices.size(); ++i) {
    EXPECT_EQ(indices[i], indices[i - 1] + 1);
  }
}

TEST(RaftCluster, ElectionSafetyNoTwoLeaders) {
  Cluster c;
  c.Init();

  ASSERT_NE(WaitForLeader(c.ptrs, std::chrono::seconds(5)), nullptr);

  constexpr int polls = 100;
  constexpr auto sleep_total = std::chrono::milliseconds(2000);

  for (int step = 0; step < polls; ++step) {
    std::unordered_map<uint64_t, int> leaders_at_term;

    for (int i = 0; i < 3; ++i) {
      const uint64_t term = c.ptrs[i]->GetCurrentTerm();

      if (c.ptrs[i]->GetRole() == RaftRole::LEADER) {
        leaders_at_term[term]++;
      }
    }

    for (const auto& p : leaders_at_term) {
      ASSERT_LE(p.second, 1) << "term=" << p.first << " leaders=" << p.second;
    }

    std::this_thread::sleep_for(sleep_total / polls);
  }
}

TEST(RaftCluster, FullTransactionViaRaft) {
  Cluster c;
  c.Init();
  RaftNode* lead = WaitForLeader(c.ptrs, std::chrono::seconds(5));
  ASSERT_NE(lead, nullptr);
  constexpr uint64_t raft_tid = 100;
  constexpr uint64_t snap = 10;
  constexpr uint32_t tbl = 1;
  constexpr uint64_t cts = 20;

  std::vector<uint64_t> ixs;

  auto push = [&](RaftEntryType t, std::string p) -> uint64_t {
    uint64_t ix = lead->Propose(t, std::move(p));
    EXPECT_GT(ix, 0u);
    ixs.push_back(ix);
    return ix;
  };

  push(RaftEntryType::TXN_BEGIN,
       RaftStateMachine::PackTxnPayload(WAL::SerializeBeginPayload(snap), raft_tid));
  push(RaftEntryType::TXN_WRITE,
       RaftStateMachine::PackTxnPayload(WAL::SerializeWritePayload(tbl, "balance", "500", 0),
                                        raft_tid));
  push(RaftEntryType::TXN_PREPARE, RaftStateMachine::PackTxnPayload(WAL::SerializePreparePayload(cts),
                                                                    raft_tid));
  push(RaftEntryType::TXN_COMMIT, RaftStateMachine::PackTxnPayload(WAL::SerializeCommitPayload(cts),
                                                                   raft_tid));

  uint64_t last = ixs.back();
  for (RaftNode* n : c.ptrs) {
    ASSERT_TRUE(n->WaitForCommit(last, std::chrono::seconds(8)));
  }
  for (auto& st : c.stores) {
    std::string v;
    uint64_t ts = 0;
    ASSERT_TRUE(st->Get(tbl, "balance", cts, &v, &ts).ok()) << ts;
    EXPECT_EQ(v, "500");
    EXPECT_EQ(ts, cts);
  }
}

TEST(RaftPersistence, LogSurvivesRestart) {
  const std::string dir = UniquePath("raft_log_persist");
  Nuke(dir);
  std::error_code ec;
  fs::create_directories(dir, ec);
  const std::string log_path = (fs::path(dir) / "raft_log").string();
  {
    RaftLog log(log_path);
    for (uint64_t i = 0; i < 5; ++i) {
      const uint64_t idx = log.Append(3, RaftEntryType::TXN_BEGIN, "p" + std::to_string(i));
      ASSERT_EQ(idx, i + 1);
    }
    ASSERT_EQ(log.LastIndex(), 5u);
  }
  {
    RaftLog reloaded(log_path);
    ASSERT_EQ(reloaded.LastIndex(), 5u);
    for (uint64_t i = 1; i <= 5; ++i) {
      auto e = reloaded.Get(i);
      ASSERT_TRUE(e.has_value());
      EXPECT_EQ(e->index, i);
      EXPECT_EQ(e->term, 3u);
      EXPECT_EQ(e->type, RaftEntryType::TXN_BEGIN);
      EXPECT_EQ(e->payload, "p" + std::to_string(i - 1));
    }
  }
  Nuke(dir);
}

TEST(RaftPersistence, MetaSurvivesRestart) {
  const std::string dir = UniquePath("raft_meta_term");
  Nuke(dir);
  InProcessTransport t;
  {
    RaftNode node(1, {2, 3}, &t, nullptr, dir);
    RequestVoteRequest req;
    req.term = 9;
    req.candidate_id = 2;
    req.last_log_index = 0;
    req.last_log_term = 0;
    auto resp = node.HandleRequestVote(req);
    ASSERT_TRUE(resp.vote_granted);
    ASSERT_EQ(node.GetCurrentTerm(), 9u);
  }
  {
    RaftNode node(1, {2, 3}, &t, nullptr, dir);
    EXPECT_EQ(node.GetCurrentTerm(), 9u);
  }
  Nuke(dir);
}

TEST(RaftPersistence, VotedForSurvivesRestart) {
  const std::string dir = UniquePath("raft_meta_vote");
  Nuke(dir);
  InProcessTransport t;
  {
    RaftNode node(1, {2, 3}, &t, nullptr, dir);
    RequestVoteRequest req;
    req.term = 11;
    req.candidate_id = 2;
    req.last_log_index = 0;
    req.last_log_term = 0;
    auto resp = node.HandleRequestVote(req);
    ASSERT_TRUE(resp.vote_granted);
  }
  {
    RaftNode node(1, {2, 3}, &t, nullptr, dir);
    EXPECT_EQ(node.GetCurrentTerm(), 11u);
    RequestVoteRequest req2;
    req2.term = 11;
    req2.candidate_id = 3;
    req2.last_log_index = 0;
    req2.last_log_term = 0;
    auto resp2 = node.HandleRequestVote(req2);
    EXPECT_FALSE(resp2.vote_granted);
    EXPECT_EQ(resp2.term, 11u);
  }
  Nuke(dir);
}

TEST(RaftPersistence, TruncateFromSurvivesRestart) {
  const std::string dir = UniquePath("raft_log_truncate_restart");
  Nuke(dir);
  std::error_code ec;
  fs::create_directories(dir, ec);
  const std::string log_path = (fs::path(dir) / "raft_log").string();
  {
    RaftLog log(log_path);
    for (uint64_t i = 0; i < 5; ++i) {
      const uint64_t idx = log.Append(1, RaftEntryType::TXN_BEGIN, "p" + std::to_string(i));
      ASSERT_EQ(idx, i + 1);
    }
    log.TruncateFrom(3);
  }
  {
    RaftLog reloaded(log_path);
    ASSERT_EQ(reloaded.LastIndex(), 2u);
    EXPECT_FALSE(reloaded.Get(3).has_value());
  }
  Nuke(dir);
}

TEST(RaftPersistence, ConflictTruncationSurvivesRestart) {
  const std::string dir = UniquePath("raft_log_conflict_restart");
  Nuke(dir);
  std::error_code ec;
  fs::create_directories(dir, ec);
  const std::string log_path = (fs::path(dir) / "raft_log").string();
  {
    RaftLog log(log_path);
    ASSERT_EQ(log.Append(1, RaftEntryType::TXN_BEGIN, "a"), 1u);
    ASSERT_EQ(log.Append(1, RaftEntryType::TXN_BEGIN, "b"), 2u);
    ASSERT_EQ(log.Append(1, RaftEntryType::TXN_BEGIN, "c1"), 3u);

    std::vector<RaftLogEntry> incoming;
    incoming.push_back(RaftLogEntry{3, 2, RaftEntryType::TXN_BEGIN, "c2"});
    incoming.push_back(RaftLogEntry{4, 2, RaftEntryType::TXN_BEGIN, "d"});
    log.AppendEntries(incoming);
  }
  {
    RaftLog reloaded(log_path);
    ASSERT_EQ(reloaded.LastIndex(), 4u);
    ASSERT_EQ(reloaded.TermAt(3), 2u);
    auto e3 = reloaded.Get(3);
    ASSERT_TRUE(e3.has_value());
    ASSERT_EQ(e3->payload, "c2");
    ASSERT_EQ(e3->term, 2u);
  }
  Nuke(dir);
}

namespace {

struct Cluster5 {
  InProcessTransport transport;
  std::vector<std::string> store_paths;
  std::vector<std::string> wal_paths;
  std::vector<std::unique_ptr<MVCCStore>> stores;
  std::vector<std::unique_ptr<WAL>> wals;
  std::vector<std::unique_ptr<LockManager>> locks;
  std::vector<std::unique_ptr<TxnManager>> txn_mgr;
  std::vector<std::unique_ptr<RaftStateMachine>> sms;
  std::vector<std::unique_ptr<RaftNode>> nodes;
  std::vector<RaftNode*> ptrs;

  ~Cluster5() {
    for (auto& n : nodes) {
      if (n) {
        n->Stop();
      }
    }
    for (auto& p : store_paths) {
      Nuke(p);
    }
    for (auto& p : wal_paths) {
      Nuke(p);
    }
  }

  void Init() {
    for (uint32_t id = 0; id < 5; ++id) {
      store_paths.push_back(UniquePath("f5st"));
      wal_paths.push_back(UniquePath("f5wal"));
      Nuke(store_paths.back());
      Nuke(wal_paths.back());
      stores.emplace_back();
      wals.emplace_back();
      locks.push_back(std::make_unique<LockManager>());
      ASSERT_TRUE(MVCCStore::Open(store_paths.back(), &stores.back()).ok());
      ASSERT_TRUE(WAL::Open(wal_paths.back(), &wals.back()).ok());
      txn_mgr.push_back(std::make_unique<TxnManager>(stores.back().get(), wals.back().get(),
                                                     locks.back().get()));
      sms.push_back(std::make_unique<RaftStateMachine>(txn_mgr.back().get()));
    }
    for (uint32_t id = 0; id < 5; ++id) {
      std::vector<uint32_t> peers;
      for (uint32_t o = 0; o < 5; ++o) {
        if (o != id) {
          peers.push_back(o);
        }
      }
      RaftNode* self = nodes.emplace_back(std::make_unique<RaftNode>(
                               id, std::move(peers), &transport, sms[id]->WrapApply()))
                         .get();
      transport.RegisterNode(id, self);
    }
    for (auto& n : nodes) {
      ptrs.push_back(n.get());
      n->Start();
    }
  }

  RaftNode* WaitLeader(std::chrono::milliseconds timeout = std::chrono::seconds(4)) {
    return WaitForLeader(ptrs, timeout);
  }

  bool CommitKV(RaftNode* leader, uint64_t raft_tid, std::string key, std::string value,
                uint64_t commit_ts) {
    if (!leader) {
      return false;
    }
    auto b = RaftStateMachine::PackTxnPayload(WAL::SerializeBeginPayload(1), raft_tid);
    uint64_t i1 = leader->Propose(RaftEntryType::TXN_BEGIN, std::move(b));
    if (i1 == 0 || !leader->WaitForCommit(i1, std::chrono::seconds(3))) {
      return false;
    }
    auto w = RaftStateMachine::PackTxnPayload(WAL::SerializeWritePayload(1, key, value, 0), raft_tid);
    uint64_t i2 = leader->Propose(RaftEntryType::TXN_WRITE, std::move(w));
    if (i2 == 0 || !leader->WaitForCommit(i2, std::chrono::seconds(3))) {
      return false;
    }
    auto p = RaftStateMachine::PackTxnPayload(WAL::SerializePreparePayload(commit_ts), raft_tid);
    uint64_t i3 = leader->Propose(RaftEntryType::TXN_PREPARE, std::move(p));
    if (i3 == 0 || !leader->WaitForCommit(i3, std::chrono::seconds(3))) {
      return false;
    }
    auto c = RaftStateMachine::PackTxnPayload(WAL::SerializeCommitPayload(commit_ts), raft_tid);
    uint64_t i4 = leader->Propose(RaftEntryType::TXN_COMMIT, std::move(c));
    if (i4 == 0) {
      return false;
    }
    return leader->WaitForCommit(i4, std::chrono::seconds(4));
  }

  bool KeyVisibleEverywhere(const std::string& key, const std::string& expected, uint64_t snap) {
    for (auto& st : stores) {
      std::string v;
      uint64_t ts = 0;
      if (!st->Get(1, key, snap, &v, &ts).ok() || v != expected) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace

TEST(FaultTest, NetworkPartitionDuringTransaction) {
  Cluster5 c;
  c.Init();
  RaftNode* leader = c.WaitLeader();
  ASSERT_NE(leader, nullptr);
  const uint32_t old_leader = leader->GetNodeId();
  c.transport.DisconnectNode(old_leader);
  std::this_thread::sleep_for(std::chrono::milliseconds(700));

  auto begin_only = RaftStateMachine::PackTxnPayload(WAL::SerializeBeginPayload(1), 40001);
  uint64_t idx = leader->Propose(RaftEntryType::TXN_BEGIN, std::move(begin_only));
  if (idx != 0) {
    EXPECT_FALSE(leader->WaitForCommit(idx, std::chrono::milliseconds(900)));
  }

  RaftNode* new_leader = AnyLeaderExcept(c.ptrs, old_leader, std::chrono::seconds(4));
  ASSERT_NE(new_leader, nullptr);
  ASSERT_TRUE(c.CommitKV(new_leader, 40002, "np_key", "np_val", 10));

  c.transport.ReconnectNode(old_leader, c.nodes[old_leader].get());
  std::this_thread::sleep_for(std::chrono::milliseconds(900));
  EXPECT_TRUE(c.KeyVisibleEverywhere("np_key", "np_val", UINT64_MAX / 4));
}

TEST(FaultTest, LeaderFailoverUnderSustainedWrites) {
  Cluster5 c;
  c.Init();
  RaftNode* leader = c.WaitLeader();
  ASSERT_NE(leader, nullptr);
  const uint32_t old_leader = leader->GetNodeId();

  std::atomic<bool> injected{false};
  std::atomic<uint64_t> committed{0};
  std::vector<std::chrono::steady_clock::time_point> success_times;
  std::mutex success_mu;

  std::vector<std::thread> workers;
  for (int t = 0; t < 4; ++t) {
    workers.emplace_back([&, t]() {
      for (int i = 0; i < 250; ++i) {
        RaftNode* cur = c.WaitLeader(std::chrono::milliseconds(1500));
        if (!cur) {
          continue;
        }
        const uint64_t tid = 50000 + static_cast<uint64_t>(t * 1000 + i);
        const uint64_t cts = 100 + tid;
        const std::string k = "lf_key_" + std::to_string(t) + "_" + std::to_string(i);
        const std::string v = "lf_val_" + std::to_string(t) + "_" + std::to_string(i);
        if (c.CommitKV(cur, tid, k, v, cts)) {
          committed.fetch_add(1);
          std::scoped_lock lk(success_mu);
          success_times.push_back(std::chrono::steady_clock::now());
        }
        if (!injected.load() && committed.load() > 120) {
          injected.store(true);
          c.transport.DisconnectNode(old_leader);
        }
      }
    });
  }
  for (auto& w : workers) {
    w.join();
  }

  RaftNode* new_leader = AnyLeaderExcept(c.ptrs, old_leader, std::chrono::seconds(5));
  ASSERT_NE(new_leader, nullptr);
  EXPECT_GT(committed.load(), 500u);

  uint64_t worst_pause_ms = 0;
  {
    std::scoped_lock lk(success_mu);
    std::sort(success_times.begin(), success_times.end());
    for (size_t i = 1; i < success_times.size(); ++i) {
      const auto gap = std::chrono::duration_cast<std::chrono::milliseconds>(
          success_times[i] - success_times[i - 1]).count();
      if (static_cast<uint64_t>(gap) > worst_pause_ms) {
        worst_pause_ms = static_cast<uint64_t>(gap);
      }
    }
  }
  std::cout << "[LeaderFailoverUnderSustainedWrites] worst_pause_ms=" << worst_pause_ms << "\n";
  EXPECT_LT(worst_pause_ms, 10000u);
}

TEST(FaultTest, SplitBrainPrevention) {
  Cluster5 c;
  c.Init();
  ASSERT_NE(c.WaitLeader(), nullptr);
  c.transport.DisconnectNode(0);
  c.transport.DisconnectNode(1);
  std::this_thread::sleep_for(std::chrono::milliseconds(800));

  RaftNode* majority_leader = nullptr;
  for (RaftNode* n : c.ptrs) {
    if (n->GetNodeId() >= 2 && n->GetRole() == RaftRole::LEADER) {
      majority_leader = n;
      break;
    }
  }
  ASSERT_NE(majority_leader, nullptr);

  uint64_t min_ix = c.nodes[0]->Propose(
      RaftEntryType::TXN_BEGIN,
      RaftStateMachine::PackTxnPayload(WAL::SerializeBeginPayload(1), 61001));
  EXPECT_EQ(min_ix, 0u);

  ASSERT_TRUE(c.CommitKV(majority_leader, 61002, "sb_key", "sb_val", 17));
  c.transport.ReconnectNode(0, c.nodes[0].get());
  c.transport.ReconnectNode(1, c.nodes[1].get());
  std::this_thread::sleep_for(std::chrono::milliseconds(1200));

  RaftNode* final_leader = c.WaitLeader(std::chrono::seconds(5));
  ASSERT_NE(final_leader, nullptr);
  EXPECT_EQ(c.nodes[0]->GetLog().LastIndex(), final_leader->GetLog().LastIndex());
  EXPECT_EQ(c.nodes[1]->GetLog().LastIndex(), final_leader->GetLog().LastIndex());
}
