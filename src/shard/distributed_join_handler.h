#pragma once

#include "shard.grpc.pb.h"

namespace txndb {

class TxnManager;

bool RunDistributedJoin(TxnManager* txn_mgr, uint64_t local_txn_id,
                        const DistributedJoinRequest& request, DistributedJoinResponse* response);

}  // namespace txndb
