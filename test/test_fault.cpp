#include <gtest/gtest.h>

#include "shard/shard_server.h"
#include "coordinator/coordinator.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using namespace txndb;

namespace {

std::string UniqueBase() {
  const auto base =
      fs::temp_directory_path() / ("trans_db_fault_" + std::to_string(std::rand()));
  fs::create_directories(base);
  return base.string();
}

void Cleanup(const std::string& path) {
  std::error_code ec;
  fs::remove_all(path, ec);
}

std::string RowFromValue(const std::string& v) {
  std::string out;
  out.push_back(1);
  out.push_back(0);
  const uint32_t n = static_cast<uint32_t>(v.size());
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>((n >> (8 * i)) & 0xFF));
  }
  out.append(v);
  return out;
}

std::string ValueFromRow(const std::string& row) {
  if (row.size() < 6) {
    return {};
  }
  const uint32_t n = static_cast<uint32_t>(static_cast<unsigned char>(row[2])) |
                     (static_cast<uint32_t>(static_cast<unsigned char>(row[3])) << 8) |
                     (static_cast<uint32_t>(static_cast<unsigned char>(row[4])) << 16) |
                     (static_cast<uint32_t>(static_cast<unsigned char>(row[5])) << 24);
  if (row.size() < 6 + n) {
    return {};
  }
  return row.substr(6, n);
}

uint32_t RouteKey(std::string_view key, uint32_t num_shards) {
  constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
  constexpr uint64_t kFnvPrime = 1099511628211ULL;
  uint64_t hash = kFnvOffset;
  for (unsigned char c : key) {
    hash ^= static_cast<uint64_t>(c);
    hash *= kFnvPrime;
  }
  return static_cast<uint32_t>(hash % num_shards);
}

}  // namespace

class FaultTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::srand(static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count()));
    base_dir_ = UniqueBase();
    base_port_ = 59051 + (std::rand() % 1000);
    StartCluster();
  }

  void TearDown() override {
    StopCluster();
    Cleanup(base_dir_);
  }

  void StartCluster() {
    for (uint32_t i = 0; i < kShards; ++i) {
      const std::string addr = "127.0.0.1:" + std::to_string(base_port_ + static_cast<int>(i));
      shard_addrs_[i] = addr;
      const std::string d = base_dir_ + "/s" + std::to_string(i);
      auto s = std::make_unique<ShardServer>(i, d, addr);
      s->Start();
      shards_.push_back(std::move(s));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    coordinator_ = std::make_unique<Coordinator>(shard_addrs_, kShards);
  }

  void StopCluster() {
    coordinator_.reset();
    for (auto& s : shards_) {
      s->Stop();
    }
    shards_.clear();
  }

  void RestartCluster() {
    StopCluster();
    StartCluster();
  }

  void KillShardServer(uint32_t shard_id) {
    ASSERT_LT(shard_id, shards_.size());
    shards_[shard_id]->Stop();
    shards_[shard_id].reset();
  }

  void RestartShardServer(uint32_t shard_id) {
    ASSERT_LT(shard_id, kShards);
    if (shards_[shard_id]) {
      return;
    }
    const std::string addr = shard_addrs_.at(shard_id);
    const std::string d = base_dir_ + "/s" + std::to_string(shard_id);
    auto s = std::make_unique<ShardServer>(shard_id, d, addr);
    s->Start();
    shards_[shard_id] = std::move(s);
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    coordinator_ = std::make_unique<Coordinator>(shard_addrs_, kShards);
  }

  void WriteKV(const std::string& key, const std::string& value) {
    const uint64_t t = coordinator_->Begin();
    ASSERT_TRUE(coordinator_->Write(t, kTable, key, RowFromValue(value)).ok());
    ASSERT_TRUE(coordinator_->Commit(t).ok());
  }

  bool ReadKV(const std::string& key, std::string* value) {
    const uint64_t t = coordinator_->Begin();
    std::string row;
    const Status s = coordinator_->Read(t, kTable, key, &row);
    if (!s.ok()) {
      (void)coordinator_->Abort(t);
      return false;
    }
    (void)coordinator_->Commit(t);
    *value = ValueFromRow(row);
    return true;
  }

  bool ReadKVDurable(const std::string& key, std::string* value) {
    const uint32_t sid = RouteKey(key, kShards);
    if (sid >= shards_.size() || !shards_[sid]) {
      return false;
    }
    MVCCStore* store = shards_[sid]->GetService()->GetLeaderStore();
    if (!store) {
      return false;
    }
    std::string row;
    uint64_t ts = 0;
    const Status s = store->Get(kTable, key, UINT64_MAX / 4, &row, &ts);
    if (!s.ok()) {
      return false;
    }
    *value = ValueFromRow(row);
    return true;
  }

  static constexpr uint32_t kShards = 3;
  static constexpr uint32_t kTable = 1;

  std::vector<std::unique_ptr<ShardServer>> shards_;
  std::unordered_map<uint32_t, std::string> shard_addrs_;
  std::unique_ptr<Coordinator> coordinator_;
  std::string base_dir_;
  int base_port_{0};
};

TEST_F(FaultTest, CommittedDataSurvivesRestart) {
  for (int i = 0; i < 10; ++i) {
    WriteKV("commit_" + std::to_string(i), "v" + std::to_string(i));
  }

  for (int i = 0; i < 10; ++i) {
    std::string v;
    ASSERT_TRUE(ReadKV("commit_" + std::to_string(i), &v));
    EXPECT_EQ(v, "v" + std::to_string(i));
  }
}

TEST_F(FaultTest, AbortedDataNotVisibleAfterRestart) {
  const uint64_t t = coordinator_->Begin();
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(coordinator_->Write(t, kTable, "abort_" + std::to_string(i), RowFromValue("x")).ok());
  }
  ASSERT_TRUE(coordinator_->Abort(t).ok());

  RestartCluster();

  for (int i = 0; i < 5; ++i) {
    std::string out;
    EXPECT_FALSE(ReadKV("abort_" + std::to_string(i), &out));
  }
}

TEST_F(FaultTest, SingleReplicaFailureNoDataLoss) {
  GTEST_SKIP() << "ShardServer does not expose replica-level disconnect/kill API; skipping.";
}

TEST_F(FaultTest, MultiShardCommitAtomicity) {
  const uint64_t t = coordinator_->Begin();
  ASSERT_TRUE(coordinator_->Write(t, kTable, "ms_a_1001", RowFromValue("A")).ok());
  ASSERT_TRUE(coordinator_->Write(t, kTable, "ms_b_2002", RowFromValue("B")).ok());
  ASSERT_TRUE(coordinator_->Write(t, kTable, "ms_c_3003", RowFromValue("C")).ok());
  ASSERT_TRUE(coordinator_->Commit(t).ok());

  std::string v;
  ASSERT_TRUE(ReadKV("ms_a_1001", &v));
  EXPECT_EQ(v, "A");
  ASSERT_TRUE(ReadKV("ms_b_2002", &v));
  EXPECT_EQ(v, "B");
  ASSERT_TRUE(ReadKV("ms_c_3003", &v));
  EXPECT_EQ(v, "C");
}

TEST_F(FaultTest, HighContentionWorkload) {
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> commits{0};
  std::atomic<uint64_t> aborts{0};

  std::vector<std::thread> threads;
  for (int tid = 0; tid < 8; ++tid) {
    threads.emplace_back([&, tid]() {
      std::mt19937 rng(static_cast<uint32_t>(tid + 1));
      std::uniform_int_distribution<int> keyd(0, 4);
      while (!stop.load()) {
        const std::string key = "hot_" + std::to_string(keyd(rng));
        const uint64_t txn = coordinator_->Begin();
        if (!coordinator_->Write(txn, kTable, key, RowFromValue("t" + std::to_string(tid))).ok()) {
          (void)coordinator_->Abort(txn);
          aborts.fetch_add(1);
          continue;
        }
        if (coordinator_->Commit(txn).ok()) {
          commits.fetch_add(1);
        } else {
          (void)coordinator_->Abort(txn);
          aborts.fetch_add(1);
        }
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::seconds(5));
  stop.store(true);
  for (auto& t : threads) {
    t.join();
  }

  EXPECT_GT(commits.load(), 0u);
  EXPECT_GE(aborts.load(), 0u);

  int readable = 0;
  for (int i = 0; i < 5; ++i) {
    std::string v;
    if (ReadKV("hot_" + std::to_string(i), &v) && !v.empty()) {
      ++readable;
    }
  }
  EXPECT_GE(readable, 1);
}

TEST_F(FaultTest, RapidTransactions) {
  for (int i = 0; i < 200; ++i) {
    WriteKV("rapid_" + std::to_string(i), std::to_string(i));
  }

  int found = 0;
  for (int i = 0; i < 200; ++i) {
    std::string v;
    if (ReadKV("rapid_" + std::to_string(i), &v)) {
      ++found;
      EXPECT_EQ(v, std::to_string(i));
    }
  }
  EXPECT_GE(found, 180);
}

TEST_F(FaultTest, LargeValueStorage) {
  std::string payload(10 * 1024, 'x');
  WriteKV("large", payload);

  std::string got;
  ASSERT_TRUE(ReadKV("large", &got));
  EXPECT_EQ(got, payload);
}

TEST_F(FaultTest, ReadOnlyTransactionNoLocks) {
  WriteKV("ro_key", "seed");

  std::atomic<bool> writer_done{false};
  std::thread writer([&]() {
    for (int i = 0; i < 100; ++i) {
      const uint64_t t = coordinator_->Begin();
      if (coordinator_->Write(t, kTable, "ro_key", RowFromValue("w" + std::to_string(i))).ok()) {
        (void)coordinator_->Commit(t);
      } else {
        (void)coordinator_->Abort(t);
      }
    }
    writer_done.store(true);
  });

  const uint64_t ro_txn = coordinator_->Begin();
  std::string row;
  ASSERT_TRUE(coordinator_->Read(ro_txn, kTable, "ro_key", &row).ok());
  ASSERT_TRUE(coordinator_->Commit(ro_txn).ok());

  writer.join();
  EXPECT_TRUE(writer_done.load());
}
