cmake_minimum_required(VERSION 3.20)
project(trans-db LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(RocksDB CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

find_program(PROTOC_EXECUTABLE
  NAMES protoc protoc.exe
  DOC "Protobuf compiler")

find_program(GRPC_CPP_PLUGIN
  NAMES grpc_cpp_plugin grpc_cpp_plugin.exe
  DOC "gRPC cpp plugin")

if(NOT PROTOC_EXECUTABLE)
  message(FATAL_ERROR "protoc not found (install protobuf via vcpkg)")
endif()
if(NOT GRPC_CPP_PLUGIN)
  message(FATAL_ERROR "grpc_cpp_plugin not found (install grpc via vcpkg)")
endif()

set(PROTO_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GENERATED_DIR})

set(PROTO_FILE ${PROTO_SRC_DIR}/shard.proto)
set(PROTO_GEN_CC ${PROTO_GENERATED_DIR}/shard.pb.cc)
set(PROTO_GEN_H ${PROTO_GENERATED_DIR}/shard.pb.h)
set(GRPC_GEN_CC ${PROTO_GENERATED_DIR}/shard.grpc.pb.cc)
set(GRPC_GEN_H ${PROTO_GENERATED_DIR}/shard.grpc.pb.h)

add_custom_command(
  OUTPUT ${PROTO_GEN_CC} ${PROTO_GEN_H} ${GRPC_GEN_CC} ${GRPC_GEN_H}
  COMMAND ${PROTOC_EXECUTABLE}
    --proto_path=${PROTO_SRC_DIR}
    --cpp_out=${PROTO_GENERATED_DIR}
    --grpc_out=${PROTO_GENERATED_DIR}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
    ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating protobuf/gRPC sources from shard.proto"
)

add_library(trans_db_proto
  ${PROTO_GEN_CC}
  ${GRPC_GEN_CC}
)
target_include_directories(trans_db_proto PUBLIC ${PROTO_GENERATED_DIR})
target_link_libraries(trans_db_proto PUBLIC
  protobuf::libprotobuf
  gRPC::grpc++
  gRPC::grpc
)

add_library(trans_db_storage
  src/storage/key_encoding.cpp
  src/storage/mvcc_iterator.cpp
  src/storage/mvcc_store.cpp
)
target_include_directories(trans_db_storage PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(trans_db_storage PUBLIC RocksDB::rocksdb)

add_library(trans_db_txn
  src/txn/lock_manager.cpp
  src/txn/wal.cpp
  src/txn/txn_manager.cpp
)
target_include_directories(trans_db_txn PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(trans_db_txn PUBLIC trans_db_storage)

enable_testing()
add_executable(test_mvcc_store test/test_mvcc_store.cpp)
target_link_libraries(test_mvcc_store PRIVATE trans_db_storage GTest::gtest_main)
add_test(NAME mvcc_store COMMAND test_mvcc_store)

add_executable(test_txn test/test_txn.cpp)
target_link_libraries(test_txn PRIVATE trans_db_txn GTest::gtest_main)
add_test(NAME txn COMMAND test_txn)

add_library(trans_db_raft
  src/raft/raft_log.cpp
  src/raft/raft_node.cpp
  src/raft/raft_transport.cpp
  src/raft/raft_state_machine.cpp
)
target_include_directories(trans_db_raft PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(trans_db_raft PUBLIC trans_db_txn)

add_executable(test_raft test/test_raft.cpp)
target_link_libraries(test_raft PRIVATE trans_db_raft GTest::gtest_main)
add_test(NAME raft COMMAND test_raft)

add_library(trans_db_shard
  src/shard/shard_server.cpp
)
target_include_directories(trans_db_shard PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(trans_db_shard PUBLIC trans_db_raft trans_db_proto)
target_compile_definitions(trans_db_shard PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)

add_library(trans_db_coordinator
  src/coordinator/coordinator.cpp
)
target_include_directories(trans_db_coordinator PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(trans_db_coordinator PUBLIC trans_db_proto)
target_compile_definitions(trans_db_coordinator PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)

add_executable(test_e2e test/test_e2e.cpp)
target_compile_definitions(test_e2e PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
target_link_libraries(test_e2e PRIVATE trans_db_shard trans_db_coordinator trans_db_storage GTest::gtest_main)
add_test(NAME e2e COMMAND test_e2e)

add_library(trans_db_sql
  src/coordinator/catalog.cpp
  src/coordinator/parser.cpp
  src/coordinator/executor.cpp
)
target_include_directories(trans_db_sql PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(trans_db_sql PUBLIC trans_db_coordinator)

add_library(trans_db_pgwire
  src/pgwire/pgwire_server.cpp
)
target_include_directories(trans_db_pgwire PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(trans_db_pgwire PUBLIC trans_db_sql)
if(WIN32)
  target_compile_definitions(trans_db_pgwire PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
  target_link_libraries(trans_db_pgwire PUBLIC ws2_32)
endif()

add_executable(trans_db_server src/main.cpp)
target_link_libraries(trans_db_server PRIVATE trans_db_pgwire trans_db_shard)
if(WIN32)
  target_compile_definitions(trans_db_server PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
endif()

add_executable(test_parser test/test_parser.cpp)
target_link_libraries(test_parser PRIVATE trans_db_sql GTest::gtest_main)
add_test(NAME parser COMMAND test_parser)

add_executable(test_pgwire test/test_pgwire.cpp)
target_include_directories(test_pgwire PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(test_pgwire PRIVATE trans_db_pgwire trans_db_shard GTest::gtest_main)
if(WIN32)
  target_compile_definitions(test_pgwire PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
  target_link_libraries(test_pgwire PRIVATE ws2_32)
endif()
add_test(NAME pgwire COMMAND test_pgwire)
