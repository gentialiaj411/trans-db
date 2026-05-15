#include <gtest/gtest.h>

#include "txn/lock_manager.h"
#include "txn/txn_manager.h"
#include "txn/wal.h"

#include "storage/mvcc_store.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace txndb;

static std::string UniquePath(std::string_view suffix) {
  auto base = fs::temp_directory_path() / "trans_db_txn_test";
  std::error_code ec;
  fs::create_directories(base, ec);
  return (base / (std::string(suffix) + "_" + std::to_string(std::rand()))).string();
}

static void Nuke(const std::string& p) {
  std::error_code ec;
  fs::remove_all(p, ec);
}

// -----------------------------------------------------------------------------
// Lock manager
// -----------------------------------------------------------------------------

TEST(LockManager, BasicLockUnlock) {
  LockManager lm;
  EXPECT_TRUE(lm.Acquire(1, "x", 10).ok());
  lm.Release(1, "x", 10);
  EXPECT_TRUE(lm.Acquire(1, "x", 22).ok());
  lm.Release(1, "x", 22);
}

TEST(LockManager, ConflictingLockTimesOut) {
  LockManager lm_short(std::chrono::milliseconds(50));
  ASSERT_TRUE(lm_short.Acquire(1, "k", 1).ok());
  EXPECT_EQ(lm_short.Acquire(1, "k", 2).code(), StatusCode::TimedOut);
  lm_short.ReleaseAll(1);
}

TEST(LockManager, LockReleasedWakesWaiter) {
  LockManager lm;
  ASSERT_TRUE(lm.Acquire(3, "q", 1).ok());

  std::mutex m;
  std::condition_variable cv;
  bool waiter_done = false;
  Status waiter_status;

  std::thread waiter([&] {
    waiter_status = lm.Acquire(3, "q", 2, std::chrono::seconds(60));
    {
      std::scoped_lock lk(m);
      waiter_done = true;
    }
    cv.notify_one();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(80));
  lm.Release(3, "q", 1);

  {
    std::unique_lock lk(m);
    cv.wait_for(lk, std::chrono::seconds(60), [&] { return waiter_done; });
    ASSERT_TRUE(waiter_done);
  }
  waiter.join();

  ASSERT_TRUE(waiter_status.ok());
  lm.ReleaseAll(2);
}

TEST(LockManager, ReleaseAllFreesEverything) {
  LockManager lm;
  ASSERT_TRUE(lm.Acquire(2, "a", 9).ok());
  ASSERT_TRUE(lm.Acquire(2, "b", 9).ok());
  ASSERT_TRUE(lm.Acquire(3, "c", 9).ok());
  lm.ReleaseAll(9);
  EXPECT_TRUE(lm.Acquire(2, "a", 88).ok());
  EXPECT_TRUE(lm.Acquire(2, "b", 88).ok());
  EXPECT_TRUE(lm.Acquire(3, "c", 88).ok());
  lm.ReleaseAll(88);
}

TEST(LockManager, ReentrantLock) {
  LockManager lm;
  ASSERT_TRUE(lm.Acquire(4, "r", 5).ok());
  ASSERT_TRUE(lm.Acquire(4, "r", 5).ok());
  lm.Release(4, "r", 5);
  lm.Release(4, "r", 5);
}

// -----------------------------------------------------------------------------
// WAL
// -----------------------------------------------------------------------------

TEST(WAL, AppendAndReplay) {
  const std::string path = UniquePath("wal1");
  Nuke(path);
  std::unique_ptr<WAL> wal;
  ASSERT_TRUE(WAL::Open(path, &wal).ok());

  ASSERT_TRUE(wal->Append(7, WALRecordType::BEGIN, WAL::SerializeBeginPayload(11)).ok());
  ASSERT_TRUE(
      wal->Append(7, WALRecordType::WRITE, WAL::SerializeWritePayload(9, "a", "va", 0)).ok());
  ASSERT_TRUE(
      wal->Append(7, WALRecordType::DELETE, WAL::SerializeDeletePayload(9, "b", 0)).ok());
  ASSERT_TRUE(wal->Append(7, WALRecordType::PREPARE, WAL::SerializePreparePayload(40)).ok());
  ASSERT_TRUE(wal->Append(7, WALRecordType::COMMIT, WAL::SerializeCommitPayload(40)).ok());

  std::vector<WALRecord> seen;
  ASSERT_TRUE(wal->Replay([&](const WALRecord& r) { seen.push_back(r); }).ok());
  ASSERT_EQ(seen.size(), 5u);
  EXPECT_EQ(seen[0].type, WALRecordType::BEGIN);
  EXPECT_EQ(seen[1].type, WALRecordType::WRITE);
  EXPECT_EQ(seen[2].type, WALRecordType::DELETE);
  EXPECT_EQ(seen[3].type, WALRecordType::PREPARE);
  EXPECT_EQ(seen[4].type, WALRecordType::COMMIT);
  for (size_t i = 0; i < seen.size(); ++i) {
    EXPECT_EQ(seen[i].lsn, i + 1u);
    EXPECT_EQ(seen[i].txn_id, 7u);
  }
  Nuke(path);
}

TEST(WAL, SyncDurability) {
  const std::string path = UniquePath("wal2");
  Nuke(path);
  {
    std::unique_ptr<WAL> wal;
    ASSERT_TRUE(WAL::Open(path, &wal).ok());
    ASSERT_TRUE(wal->AppendSync(1, WALRecordType::BEGIN, WAL::SerializeBeginPayload(55)).ok());
  }

  std::unique_ptr<WAL> wal2;
  ASSERT_TRUE(WAL::Open(path, &wal2).ok());

  uint64_t replayed_txn_id = 0;
  WALRecordType replayed{};
  wal2->Replay([&](const WALRecord& r) {
    replayed_txn_id = r.txn_id;
    replayed = r.type;
  });
  EXPECT_EQ(replayed_txn_id, 1u);
  EXPECT_EQ(replayed, WALRecordType::BEGIN);
  Nuke(path);
}

TEST(WAL, TruncatedRecordIgnored) {
  const std::string path = UniquePath("wal3");
  Nuke(path);
  {
    std::unique_ptr<WAL> wal;
    ASSERT_TRUE(WAL::Open(path, &wal).ok());
    ASSERT_TRUE(wal->AppendSync(1, WALRecordType::BEGIN, WAL::SerializeBeginPayload(1)).ok());
    ASSERT_TRUE(wal->AppendSync(2, WALRecordType::BEGIN, WAL::SerializeBeginPayload(2)).ok());
    ASSERT_TRUE(wal->AppendSync(3, WALRecordType::BEGIN, WAL::SerializeBeginPayload(3)).ok());
  }

  auto sz = fs::file_size(path);
  ASSERT_GT(sz, 8u);
  std::error_code ec;
  fs::resize_file(path, sz - 7, ec);
  ASSERT_FALSE(ec);

  std::unique_ptr<WAL> wal2;
  ASSERT_TRUE(WAL::Open(path, &wal2).ok());
  int count = 0;
  wal2->Replay([&](const WALRecord&) { ++count; });
  EXPECT_EQ(count, 2);
  Nuke(path);
}

TEST(WAL, PayloadSerialization) {
  WAL::BeginInfo bi{};
  ASSERT_TRUE(WAL::DeserializeBeginPayload(WAL::SerializeBeginPayload(123456), &bi));
  EXPECT_EQ(bi.snapshot_ts, 123456u);

  WAL::WriteInfo wi{};
  ASSERT_TRUE(
      WAL::DeserializeWritePayload(WAL::SerializeWritePayload(7, "\x01z", "", 888), &wi));
  EXPECT_EQ(wi.table_id, 7u);
  EXPECT_EQ(wi.key, std::string("\x01z", 2));
  EXPECT_TRUE(wi.value.empty());
  EXPECT_EQ(wi.write_ts, 888u);

  WAL::DeleteInfo di{};
  ASSERT_TRUE(WAL::DeserializeDeletePayload(WAL::SerializeDeletePayload(3, "k9", 2), &di));
  EXPECT_EQ(di.table_id, 3u);
  EXPECT_EQ(di.key, "k9");
  EXPECT_EQ(di.write_ts, 2u);

  WAL::PrepareInfo pi{};
  ASSERT_TRUE(WAL::DeserializePreparePayload(WAL::SerializePreparePayload(99), &pi));
  EXPECT_EQ(pi.commit_ts, 99u);

  WAL::CommitInfo ci{};
  ASSERT_TRUE(WAL::DeserializeCommitPayload(WAL::SerializeCommitPayload(101), &ci));
  EXPECT_EQ(ci.commit_ts, 101u);
}

// -----------------------------------------------------------------------------
// Transaction manager helpers
// -----------------------------------------------------------------------------

static std::string StorePath(std::string_view tag) {
  return UniquePath(std::string("store_").append(tag));
}

class TxnEnv : public ::testing::Test {
protected:
  void SetUp() override {
    store_path = StorePath("env");
    wal_path = UniquePath("wal_env");
    Nuke(store_path);
    Nuke(wal_path);
    ASSERT_TRUE(MVCCStore::Open(store_path, &store).ok());
    ASSERT_TRUE(WAL::Open(wal_path, &wal).ok());
    tm = std::make_unique<TxnManager>(store.get(), wal.get(), &locks);
  }

  void TearDown() override {
    tm.reset();
    wal.reset();
    store.reset();
    Nuke(store_path);
    Nuke(wal_path);
  }

  std::string store_path;
  std::string wal_path;
  std::unique_ptr<MVCCStore> store;
  std::unique_ptr<WAL> wal;
  LockManager locks{std::chrono::milliseconds(5000)};
  std::unique_ptr<TxnManager> tm;
};

// -----------------------------------------------------------------------------
// Transaction manager
// -----------------------------------------------------------------------------

TEST_F(TxnEnv, SimpleReadWrite) {
  const uint64_t t1 = tm->Begin(10);
  ASSERT_TRUE(tm->Write(t1, 88, "k", "v").ok());
  std::string v;
  ASSERT_TRUE(tm->Read(t1, 88, "k", &v).ok());
  EXPECT_EQ(v, "v");

  ASSERT_TRUE(tm->CommitSingleShard(t1, 20).ok());

  const uint64_t t2 = tm->Begin(25);
  std::string saw;
  ASSERT_TRUE(tm->Read(t2, 88, "k", &saw).ok());
  EXPECT_EQ(saw, "v");
}

TEST_F(TxnEnv, SnapshotIsolation) {
  ASSERT_TRUE(store->Put(1, "k", 5, "old").ok());

  uint64_t a = tm->Begin(10);
  uint64_t b = tm->Begin(12);
  ASSERT_TRUE(tm->Write(b, 1, "k", "new").ok());
  ASSERT_TRUE(tm->CommitSingleShard(b, 20).ok());

  std::string v;
  ASSERT_TRUE(tm->Read(a, 1, "k", &v).ok());
  EXPECT_EQ(v, "old");
}

TEST_F(TxnEnv, ReadSetValidationDetectsConflict) {
  ASSERT_TRUE(store->Put(5, "k", 5, "v0").ok());

  uint64_t a = tm->Begin(10);
  uint64_t b = tm->Begin(11);
  std::string r;
  ASSERT_TRUE(tm->Read(a, 5, "k", &r).ok());
  EXPECT_EQ(r, "v0");

  ASSERT_TRUE(tm->Write(b, 5, "k", "v1").ok());
  ASSERT_TRUE(tm->CommitSingleShard(b, 15).ok());

  EXPECT_EQ(tm->Prepare(a, 20).code(), StatusCode::Conflict);
}

TEST_F(TxnEnv, CommitSingleShardHappyPath) {
  uint64_t t = tm->Begin(1);
  ASSERT_TRUE(tm->Write(t, 3, "a", "1").ok());
  ASSERT_TRUE(tm->Write(t, 3, "b", "2").ok());
  ASSERT_TRUE(tm->Write(t, 3, "c", "3").ok());
  ASSERT_TRUE(tm->CommitSingleShard(t, 9).ok());

  uint64_t r = tm->Begin(10);
  for (auto key_expected : std::initializer_list<std::pair<const char*, const char*>>(
           {{"a", "1"}, {"b", "2"}, {"c", "3"}})) {
    const char* key = key_expected.first;
    std::string v;
    ASSERT_TRUE(tm->Read(r, 3, key, &v).ok()) << key;
    EXPECT_EQ(v, key_expected.second) << key;
  }
}

TEST_F(TxnEnv, AbortReleasesLocks) {
  uint64_t t1 = tm->Begin(1);
  ASSERT_TRUE(tm->Write(t1, 44, "k", "x").ok());
  ASSERT_TRUE(tm->Abort(t1).ok());

  LockManager standalone;
  ASSERT_TRUE(standalone.Acquire(44, "k", 777).ok());
  standalone.ReleaseAll(777);
}

TEST(TxnManager, WriteConflictAborts) {
  auto store_path = StorePath("conflict");
  auto wal_path = UniquePath("wal_conflict");
  Nuke(store_path);
  Nuke(wal_path);

  LockManager locks(std::chrono::milliseconds(50));
  std::unique_ptr<MVCCStore> store;
  std::unique_ptr<WAL> wal;
  ASSERT_TRUE(MVCCStore::Open(store_path, &store).ok());
  ASSERT_TRUE(WAL::Open(wal_path, &wal).ok());
  TxnManager mgr(store.get(), wal.get(), &locks);

  uint64_t a = mgr.Begin(5);
  uint64_t b = mgr.Begin(6);
  ASSERT_TRUE(mgr.Write(a, 77, "x", "a_val").ok());
  EXPECT_FALSE(mgr.Write(b, 77, "x", "b_val").ok());

  ASSERT_TRUE(mgr.CommitSingleShard(a, 30).ok());

  Nuke(store_path);
  Nuke(wal_path);
}

TEST(TxnManager, RecoverCommittedTxn) {
  auto store_path = StorePath("rc");
  auto wal_path = UniquePath("w_rc");
  Nuke(store_path);
  Nuke(wal_path);

  LockManager lm;
  std::unique_ptr<MVCCStore> store;
  std::unique_ptr<WAL> wal;
  ASSERT_TRUE(MVCCStore::Open(store_path, &store).ok());
  ASSERT_TRUE(WAL::Open(wal_path, &wal).ok());

  {
    TxnManager tm(store.get(), wal.get(), &lm);
    uint64_t tid = tm.Begin(1);
    ASSERT_TRUE(tm.Write(tid, 8, "p", "z").ok());
    ASSERT_TRUE(tm.Prepare(tid, 20).ok());
    ASSERT_TRUE(tm.Commit(tid, 20).ok());
  }

  TxnManager tm2(store.get(), wal.get(), &lm);
  ASSERT_TRUE(tm2.Recover().ok());

  std::string out;
  uint64_t vt = 0;
  ASSERT_TRUE(store->Get(8, "p", 99, &out, &vt).ok());
  EXPECT_EQ(out, "z");

  Nuke(store_path);
  Nuke(wal_path);
}

TEST(TxnManager, RecoverAbortedTxn) {
  auto store_path = StorePath("ra");
  auto wal_path = UniquePath("w_ra");
  Nuke(store_path);
  Nuke(wal_path);

  LockManager lm;
  std::unique_ptr<MVCCStore> store;
  std::unique_ptr<WAL> wal;
  ASSERT_TRUE(MVCCStore::Open(store_path, &store).ok());
  ASSERT_TRUE(WAL::Open(wal_path, &wal).ok());

  {
    TxnManager tm(store.get(), wal.get(), &lm);
    uint64_t tid = tm.Begin(1);
    ASSERT_TRUE(tm.Write(tid, 2, "q", "y").ok());
    ASSERT_TRUE(tm.Abort(tid).ok());
  }

  TxnManager tm2(store.get(), wal.get(), &lm);
  ASSERT_TRUE(tm2.Recover().ok());

  std::string out;
  uint64_t vt = 0;
  EXPECT_FALSE(store->Get(2, "q", 50, &out, &vt).ok());

  Nuke(store_path);
  Nuke(wal_path);
}

TEST(TxnManager, RecoverPreparedTxn) {
  auto store_path = StorePath("rp");
  auto wal_path = UniquePath("w_rp");
  Nuke(store_path);
  Nuke(wal_path);

  LockManager lm;
  std::unique_ptr<MVCCStore> store;
  std::unique_ptr<WAL> wal;
  ASSERT_TRUE(MVCCStore::Open(store_path, &store).ok());
  ASSERT_TRUE(WAL::Open(wal_path, &wal).ok());

  uint64_t committed_tid = 0;
  {
    TxnManager tm(store.get(), wal.get(), &lm);
    uint64_t tid = tm.Begin(3);
    ASSERT_TRUE(tm.Write(tid, 6, "w", "val").ok());
    ASSERT_TRUE(tm.Prepare(tid, 30).ok());
    committed_tid = tid;
  }

  TxnManager tm2(store.get(), wal.get(), &lm);
  ASSERT_TRUE(tm2.Recover().ok());
  Transaction* tr = tm2.GetTxn(committed_tid);
  ASSERT_NE(tr, nullptr);
  EXPECT_EQ(tr->state, TxnState::PREPARED);

  ASSERT_TRUE(tm2.Commit(committed_tid, 30).ok());
  std::string out;
  uint64_t vt = 0;
  ASSERT_TRUE(store->Get(6, "w", 40, &out, &vt).ok());
  EXPECT_EQ(out, "val");

  Nuke(store_path);
  Nuke(wal_path);
}
