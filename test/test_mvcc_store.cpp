#include <gtest/gtest.h>

#include "storage/key_encoding.h"
#include "storage/mvcc_store.h"

#include <cstdlib>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;
using namespace txndb;

static std::string UniqueTempDbPath() {
  auto base = fs::temp_directory_path() / "trans_db_test";
  std::error_code ec;
  fs::create_directories(base, ec);
  return (base / (std::string("db_") + std::to_string(std::rand()))).string();
}

static void DestroyDb(const std::string& path) {
  std::error_code ec;
  fs::remove_all(path, ec);
}

TEST(KeyEncoding, DecodeRoundTrip) {
  std::string enc = EncodeKey(42, "pk\x00val", 1000);
  uint32_t tid = 0;
  std::string pk;
  uint64_t ts = 0;
  ASSERT_TRUE(DecodeKey(enc, &tid, &pk, &ts));
  EXPECT_EQ(tid, 42u);
  EXPECT_EQ(pk, "pk\x00val");
  EXPECT_EQ(ts, 1000u);
}

TEST(KeyEncoding, NewerWriteTimestampSortsBeforeOlder) {
  std::string k_new = EncodeKey(1, "row", 200);
  std::string k_old = EncodeKey(1, "row", 100);
  EXPECT_LT(k_new, k_old);
}

TEST(KeyEncoding, InvertIsSelfInverse) {
  const uint64_t samples[] = {0, 1, UINT64_MAX, 919191919u};
  for (uint64_t ts : samples) {
    uint64_t inv = InvertTimestamp(ts);
    EXPECT_EQ(InvertTimestamp(inv), ts);
  }
}

TEST(MVCCStore, PutGetLatestVisibleVersion) {
  std::string path = UniqueTempDbPath();
  std::unique_ptr<MVCCStore> store;
  ASSERT_TRUE(MVCCStore::Open(path, &store).ok());

  constexpr uint32_t kTable = 7;
  ASSERT_TRUE(store->Put(kTable, "k", 10, "v10").ok());
  ASSERT_TRUE(store->Put(kTable, "k", 20, "v20").ok());
  ASSERT_TRUE(store->Put(kTable, "k", 30, "v30").ok());

  std::string v;
  uint64_t vt = 0;
  ASSERT_TRUE(store->Get(kTable, "k", 25, &v, &vt).ok());
  EXPECT_EQ(v, "v20");
  EXPECT_EQ(vt, 20u);

  DestroyDb(path);
}

TEST(MVCCStore, SnapshotIsolationBetweenVersions) {
  std::string path = UniqueTempDbPath();
  std::unique_ptr<MVCCStore> store;
  ASSERT_TRUE(MVCCStore::Open(path, &store).ok());

  constexpr uint32_t kTable = 1;
  ASSERT_TRUE(store->Put(kTable, "a", 1, "a1").ok());
  ASSERT_TRUE(store->Put(kTable, "b", 2, "b2").ok());
  ASSERT_TRUE(store->Put(kTable, "a", 5, "a5").ok());

  std::string v;
  uint64_t vt = 0;
  ASSERT_TRUE(store->Get(kTable, "a", 4, &v, &vt).ok());
  EXPECT_EQ(v, "a1");

  ASSERT_TRUE(store->Get(kTable, "a", 10, &v, &vt).ok());
  EXPECT_EQ(v, "a5");

  DestroyDb(path);
}

TEST(MVCCStore, DeleteHidesOlderValues) {
  std::string path = UniqueTempDbPath();
  std::unique_ptr<MVCCStore> store;
  ASSERT_TRUE(MVCCStore::Open(path, &store).ok());

  constexpr uint32_t kTable = 3;
  ASSERT_TRUE(store->Put(kTable, "k", 10, "live").ok());
  ASSERT_TRUE(store->Delete(kTable, "k", 40).ok());
  ASSERT_TRUE(store->Put(kTable, "k", 50, "again").ok());

  std::string v;
  uint64_t vt = 0;
  ASSERT_TRUE(store->Get(kTable, "k", 35, &v, &vt).ok());
  EXPECT_EQ(v, "live");

  EXPECT_FALSE(store->Get(kTable, "k", 45, &v, &vt).ok());

  ASSERT_TRUE(store->Get(kTable, "k", 55, &v, &vt).ok());
  EXPECT_EQ(v, "again");

  DestroyDb(path);
}

TEST(MVCCStore, ScanExclusiveEndAndMvccVisibility) {
  std::string path = UniqueTempDbPath();
  std::unique_ptr<MVCCStore> store;
  ASSERT_TRUE(MVCCStore::Open(path, &store).ok());

  constexpr uint32_t kTable = 9;
  ASSERT_TRUE(store->Put(kTable, "b", 1, "b1").ok());
  ASSERT_TRUE(store->Put(kTable, "d", 1, "d1").ok());
  ASSERT_TRUE(store->Put(kTable, "a", 2, "a2").ok());
  ASSERT_TRUE(store->Put(kTable, "c", 3, "c3").ok());

  auto it = store->Scan(kTable, "a", "c", 5);
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->PrimaryKey(), "a");
  EXPECT_EQ(it->Value(), "a2");
  it->Next();
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->PrimaryKey(), "b");
  it->Next();
  EXPECT_FALSE(it->Valid());

  DestroyDb(path);
}

TEST(MVCCStore, ScanOpenEndedUpperBounds) {
  std::string path = UniqueTempDbPath();
  std::unique_ptr<MVCCStore> store;
  ASSERT_TRUE(MVCCStore::Open(path, &store).ok());

  constexpr uint32_t kTable = 11;
  ASSERT_TRUE(store->Put(kTable, "m", 1, "m1").ok());
  ASSERT_TRUE(store->Put(kTable, "z", 2, "z2").ok());

  auto it = store->Scan(kTable, "m", "", 9, /*open_end_exclusive=*/true);
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->PrimaryKey(), "m");
  EXPECT_EQ(it->Value(), "m1");
  it->Next();
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->PrimaryKey(), "z");
  EXPECT_EQ(it->Value(), "z2");
  it->Next();
  EXPECT_FALSE(it->Valid());

  DestroyDb(path);
}