#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace txndb {

// Key: [table_id big-endian 4B][primary_key bytes][inverted_ts big-endian 8B]
// inverted_ts = UINT64_MAX - write_ts so newer writes sort first in ascending order.

std::string EncodeKey(uint32_t table_id, std::string_view primary_key, uint64_t write_ts);

// Smallest byte key for (table_id, pk): newest chronologically (max write_ts).
std::string EncodeKeyPrefixNewest(uint32_t table_id, std::string_view primary_key);

// Exclusive upper bound prefix for iterating pk < end_pk (table_id must match).
std::string EncodePkSpanEnd(uint32_t table_id, std::string_view end_pk_exclusive);

bool DecodeKey(std::string_view encoded, uint32_t* table_id, std::string* primary_key,
               uint64_t* write_ts);

uint64_t InvertTimestamp(uint64_t write_ts);

}  // namespace txndb
