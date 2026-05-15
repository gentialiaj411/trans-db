#include "storage/key_encoding.h"

#include <cstring>

namespace txndb {

static void AppendBigEndianU32(std::string* out, uint32_t v) {
  char buf[4];
  buf[0] = static_cast<char>((v >> 24) & 0xff);
  buf[1] = static_cast<char>((v >> 16) & 0xff);
  buf[2] = static_cast<char>((v >> 8) & 0xff);
  buf[3] = static_cast<char>(v & 0xff);
  out->append(buf, sizeof(buf));
}

static void AppendBigEndianU64(std::string* out, uint64_t v) {
  char buf[8];
  for (int i = 7; i >= 0; --i) {
    buf[i] = static_cast<char>(v & 0xff);
    v >>= 8;
  }
  out->append(buf, sizeof(buf));
}

static uint32_t ReadBigEndianU32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) {
    v = (v << 8) | static_cast<unsigned char>(p[i]);
  }
  return v;
}

static uint64_t ReadBigEndianU64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v = (v << 8) | static_cast<unsigned char>(p[i]);
  }
  return v;
}

uint64_t InvertTimestamp(uint64_t write_ts) { return ~write_ts; }

std::string EncodeKey(uint32_t table_id, std::string_view primary_key, uint64_t write_ts) {
  std::string out;
  out.reserve(4 + primary_key.size() + 8);
  AppendBigEndianU32(&out, table_id);
  out.append(primary_key);
  AppendBigEndianU64(&out, InvertTimestamp(write_ts));
  return out;
}

std::string EncodeKeyPrefixNewest(uint32_t table_id, std::string_view primary_key) {
  return EncodeKey(table_id, primary_key, UINT64_MAX);
}

std::string EncodePkSpanEnd(uint32_t table_id, std::string_view end_pk_exclusive) {
  std::string out;
  out.reserve(4 + end_pk_exclusive.size());
  AppendBigEndianU32(&out, table_id);
  out.append(end_pk_exclusive);
  return out;
}

bool DecodeKey(std::string_view encoded, uint32_t* table_id, std::string* primary_key,
               uint64_t* write_ts) {
  if (encoded.size() < 4 + 8) {
    return false;
  }
  const char* data = encoded.data();
  *table_id = ReadBigEndianU32(data);
  size_t pk_len = encoded.size() - 4 - 8;
  primary_key->assign(encoded.data() + 4, pk_len);
  uint64_t inv = ReadBigEndianU64(data + 4 + pk_len);
  *write_ts = InvertTimestamp(inv);
  return true;
}

}  // namespace txndb
