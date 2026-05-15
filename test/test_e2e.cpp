#include <gtest/gtest.h>

// Include shard/server stack before coordinator so txn/wal.h is parsed before gRPC pulls
// Win32 macros (e.g. BEGIN/DELETE/CONFLICT names) that collide with our enums / proto names.
#include "shard/shard_server.h"
#include "storage/mvcc_store.h"

#include "coordinator/coordinator.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace fs = std::filesystem;
using namespace txndb;

static std::string UniqueBase() {
  auto base =
      fs::temp_directory_path() / ("trans_db_e2e_" + std::to_string(std::rand()));
  fs::create_directories(base);
  return base.string();
}

static void Nuke(const std::string& p) {
  std::error_code ec;
  fs::remove_all(p, ec);
}

static uint32_t RouteKey(std::string_view key, uint32_t num_shards) {
  constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
  constexpr uint64_t kFnvPrime = 1099511628211ULL;
  uint64_t hash = kFnvOffset;
  for (unsigned char c : key) {
    hash ^= static_cast<uint64_t>(c);
    hash *= kFnvPrime;
  }
  return static_cast<uint32_t>(hash % num_shards);
}

static bool MvccReadable(MVCCStore* store, uint32_t table, std::string_view key, uint64_t snap) {
  if (!store) {
    return false;
  }
  std::string v;
  uint64_t ts = 0;
  return store->Get(table, key, snap, &v, &ts).ok();
}

class E2ETest : public ::testing::Test {
protected:
  void SetUp() override {
    std::srand(static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count()));
    base_dir_ = UniqueBase();
    const int base_port = 55451 + (std::rand() % 2000);
    std::unordered_map<uint32_t, std::string> addrs;

    for (uint32_t i = 0; i < kShards; ++i) {
      const std::string addr = "127.0.0.1:" + std::to_string(base_port + static_cast<int>(i));
      addrs[i] = addr;
      const std::string d = base_dir_ + "/s" + std::to_string(i);
      shard_servers_.push_back(std::make_unique<ShardServer>(i, d, addr));
      shard_servers_.back()->Start();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(400));

    coordinator_ = std::make_unique<Coordinator>(addrs, kShards);
  }

  void TearDown() override {
    coordinator_.reset();
    shard_servers_.clear();
    Nuke(base_dir_);
  }

  Coordinator* Coord() const { return coordinator_.get(); }

  MVCCStore* LeaderStoreForShard(uint32_t shard_idx) const {
    if (shard_idx >= shard_servers_.size()) {
      return nullptr;
    }
    return shard_servers_[shard_idx]->GetService()->GetLeaderStore();
  }

  static constexpr uint32_t kShards = 3;
  static constexpr uint32_t kTable = 1;

  std::vector<std::unique_ptr<ShardServer>> shard_servers_;
  std::unique_ptr<Coordinator> coordinator_;
  std::string base_dir_;
};

TEST_F(E2ETest, SingleShardWriteRead) {
  uint64_t t = Coord()->Begin();
  ASSERT_TRUE(Coord()->Write(t, kTable, "alice", "v1").ok());
  ASSERT_TRUE(Coord()->Commit(t).ok());

  uint64_t t2 = Coord()->Begin();
  std::string v;
  ASSERT_TRUE(Coord()->Read(t2, kTable, "alice", &v).ok());
  EXPECT_EQ(v, "v1");
}

TEST_F(E2ETest, MultiShardTransaction) {
  std::string ka, kb;
  for (unsigned i = 0; i < 50000; ++i) {
    ka = "msk" + std::to_string(i);
    kb = "msk_alt" + std::to_string(i);
    if (RouteKey(ka, kShards) != RouteKey(kb, kShards)) {
      break;
    }
  }
  ASSERT_NE(RouteKey(ka, kShards), RouteKey(kb, kShards));

  uint64_t tx = Coord()->Begin();
  ASSERT_TRUE(Coord()->Write(tx, kTable, ka, "xa").ok());
  ASSERT_TRUE(Coord()->Write(tx, kTable, kb, "xb").ok());
  ASSERT_TRUE(Coord()->Commit(tx).ok());

  uint64_t rd = Coord()->Begin();
  std::string va, vb;
  ASSERT_TRUE(Coord()->Read(rd, kTable, ka, &va).ok());
  ASSERT_TRUE(Coord()->Read(rd, kTable, kb, &vb).ok());
  EXPECT_EQ(va, "xa");
  EXPECT_EQ(vb, "xb");
}

TEST_F(E2ETest, SingleShardOptimization) {
  uint64_t t = Coord()->Begin();
  const std::string key = "single_opt_key";
  const uint32_t owner = RouteKey(key, kShards);
  ASSERT_TRUE(Coord()->Write(t, kTable, key, "solo").ok());
  ASSERT_TRUE(Coord()->Commit(t).ok());

  for (uint32_t s = 0; s < kShards; ++s) {
    MVCCStore* st = LeaderStoreForShard(s);
    ASSERT_NE(st, nullptr);
    if (s == owner) {
      EXPECT_TRUE(MvccReadable(st, kTable, key, 999999999u));
    } else {
      EXPECT_FALSE(MvccReadable(st, kTable, key, 999999999u));
    }
  }
}

TEST_F(E2ETest, MultiShardAtomicity) {
  std::string ka, kb;
  bool ok = false;
  for (unsigned i = 0; i < 50000 && !ok; ++i) {
    ka = "atom_a" + std::to_string(i);
    kb = "atom_b" + std::to_string(i);
    ok = (RouteKey(ka, kShards) != RouteKey(kb, kShards));
  }
  ASSERT_TRUE(ok);

  uint64_t interloper = Coord()->Begin();
  ASSERT_TRUE(Coord()->Write(interloper, kTable, kb, "pre").ok());
  ASSERT_TRUE(Coord()->Commit(interloper).ok());

  uint64_t t1 = Coord()->Begin();
  std::string vignore;
  ASSERT_TRUE(Coord()->Read(t1, kTable, kb, &vignore).ok());

  uint64_t bump = Coord()->Begin();
  ASSERT_TRUE(Coord()->Write(bump, kTable, kb, "bump_after_read").ok());
  ASSERT_TRUE(Coord()->Commit(bump).ok());

  ASSERT_TRUE(Coord()->Write(t1, kTable, ka, "should_abort").ok());
  Status c = Coord()->Commit(t1);
  EXPECT_FALSE(c.ok());

  constexpr uint64_t kSnapProbe = UINT64_MAX / 4;
  for (uint32_t s = 0; s < kShards; ++s) {
    MVCCStore* st = LeaderStoreForShard(s);
    ASSERT_FALSE(MvccReadable(st, kTable, ka, kSnapProbe));
    if (RouteKey(kb, kShards) == s) {
      EXPECT_TRUE(MvccReadable(st, kTable, kb, kSnapProbe));
    }
  }
}

TEST_F(E2ETest, AbortCleansUp) {
  uint64_t t = Coord()->Begin();
  std::vector<std::string> keys{"abt0", "abt1", "abt2"};
  ASSERT_TRUE(Coord()->Write(t, kTable, keys[0], "a").ok());
  ASSERT_TRUE(Coord()->Write(t, kTable, keys[1], "b").ok());
  ASSERT_TRUE(Coord()->Write(t, kTable, keys[2], "c").ok());
  ASSERT_TRUE(Coord()->Abort(t).ok());

  uint64_t chk = Coord()->Begin();
  for (const auto& k : keys) {
    std::string v;
    Status r = Coord()->Read(chk, kTable, k, &v);
    EXPECT_FALSE(r.ok());
  }
}

TEST_F(E2ETest, ConcurrentTransactions) {
  std::vector<std::future<bool>> fus;
  for (int i = 0; i < 10; ++i) {
    fus.push_back(std::async(std::launch::async, [this, i]() {
      uint64_t t = Coord()->Begin();
      const std::string k = "con" + std::to_string(i);
      if (!Coord()->Write(t, kTable, k, std::to_string(i)).ok()) {
        return false;
      }
      return Coord()->Commit(t).ok();
    }));
  }
  for (auto& f : fus) {
    ASSERT_TRUE(f.get());
  }
  uint64_t readAll = Coord()->Begin();
  for (int i = 0; i < 10; ++i) {
    std::string v;
    const std::string k = "con" + std::to_string(i);
    ASSERT_TRUE(Coord()->Read(readAll, kTable, k, &v).ok());
    EXPECT_EQ(v, std::to_string(i));
  }
}

TEST_F(E2ETest, ReadYourOwnWrites) {
  uint64_t t = Coord()->Begin();
  ASSERT_TRUE(Coord()->Write(t, kTable, "ryow", "x").ok());
  std::string v;
  ASSERT_TRUE(Coord()->Read(t, kTable, "ryow", &v).ok());
  EXPECT_EQ(v, "x");
  ASSERT_TRUE(Coord()->Commit(t).ok());
}

TEST_F(E2ETest, ConflictDetection) {
  const std::string kx = "conflict_detect_x";

  uint64_t seed = Coord()->Begin();
  ASSERT_TRUE(Coord()->Write(seed, kTable, kx, "seed").ok());
  ASSERT_TRUE(Coord()->Commit(seed).ok());

  uint64_t a = Coord()->Begin();
  std::string v0;
  ASSERT_TRUE(Coord()->Read(a, kTable, kx, &v0).ok());

  uint64_t b = Coord()->Begin();
  ASSERT_TRUE(Coord()->Write(b, kTable, kx, "bob").ok());
  ASSERT_TRUE(Coord()->Commit(b).ok());

  ASSERT_TRUE(Coord()->Write(a, kTable, "other_key_z", "z").ok());
  Status c = Coord()->Commit(a);
  EXPECT_FALSE(c.ok());
}

TEST_F(E2ETest, MultipleShardReadsAndWrites) {
  std::string keys_for_shard[3];
  bool picked[3] = {false, false, false};
  unsigned cand = 0;
  while (!(picked[0] && picked[1] && picked[2])) {
    ++cand;
    ASSERT_LT(cand, 200000u);
    std::string k = "mssvx" + std::to_string(cand);
    const uint32_t s = RouteKey(k, kShards);
    if (!picked[s]) {
      picked[s] = true;
      keys_for_shard[s] = k;
    }
  }
  const std::vector<std::string> keys = {keys_for_shard[0], keys_for_shard[1], keys_for_shard[2],
                                        "extra_mssw_a", "extra_mssw_b"};

  uint64_t t = Coord()->Begin();
  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_TRUE(Coord()->Write(t, kTable, keys[i], std::string(1, 'a' + static_cast<char>(i))).ok());
  }
  for (size_t i = 0; i < keys.size(); ++i) {
    std::string v;
    ASSERT_TRUE(Coord()->Read(t, kTable, keys[i], &v).ok());
    EXPECT_EQ(v, std::string(1, 'a' + static_cast<char>(i)));
  }
  ASSERT_TRUE(Coord()->Commit(t).ok());

  uint64_t chk = Coord()->Begin();
  for (size_t i = 0; i < keys.size(); ++i) {
    std::string v;
    ASSERT_TRUE(Coord()->Read(chk, kTable, keys[i], &v).ok());
    EXPECT_EQ(v, std::string(1, 'a' + static_cast<char>(i)));
  }
}

TEST_F(E2ETest, TimestampOrdering) {
  uint64_t a = Coord()->Begin();
  ASSERT_TRUE(Coord()->Write(a, kTable, "tsk", "old").ok());
  ASSERT_TRUE(Coord()->Commit(a).ok());

  uint64_t b = Coord()->Begin();
  std::string v;
  ASSERT_TRUE(Coord()->Read(b, kTable, "tsk", &v).ok());
  EXPECT_EQ(v, "old");

  ASSERT_TRUE(Coord()->Write(b, kTable, "tsk", "new").ok());
  ASSERT_TRUE(Coord()->Commit(b).ok());

  uint64_t c = Coord()->Begin();
  ASSERT_TRUE(Coord()->Read(c, kTable, "tsk", &v).ok());
  EXPECT_EQ(v, "new");
}

TEST_F(E2ETest, CoordinatorRangeScanFansOutShards) {
  auto committed_put = [&](const std::string& pk, const std::string& val) {
    uint64_t t = Coord()->Begin();
    ASSERT_TRUE(Coord()->Write(t, kTable, pk, val).ok());
    ASSERT_TRUE(Coord()->Commit(t).ok());
  };

  committed_put("zr_a", "1");
  committed_put("zr_m", "2");
  committed_put("zr_z", "3");

  uint64_t txn = Coord()->Begin();
  std::vector<std::pair<std::string, std::string>> rows;
  Status s = Coord()->Scan(txn, kTable, "zr_", "zs", /*range_end_open=*/false, &rows);
  ASSERT_TRUE(s.ok());

  ASSERT_EQ(rows.size(), 3u);
  EXPECT_EQ(rows[0].first, "zr_a");
  EXPECT_EQ(rows[1].first, "zr_m");
  EXPECT_EQ(rows[2].first, "zr_z");
  ASSERT_TRUE(Coord()->Commit(txn).ok());
}
