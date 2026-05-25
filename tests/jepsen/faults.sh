#!/usr/bin/env bash
# Apply/clear network faults inside the compose network (Linux + NET_ADMIN).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/deploy/compose/docker-compose.yml" -f "$ROOT/tests/jepsen/compose.jepsen.yml")

action="${1:-}"
scenario="${2:-}"

exec_runner() {
  "${COMPOSE[@]}" exec -T jepsen-runner sh -c "$*"
}

exec_rep() {
  local svc="$1"
  shift
  "${COMPOSE[@]}" exec -T "$svc" sh -c "$*"
}

clear_all() {
  exec_runner "iptables -F || true; iptables -X || true" || true
  for svc in shard0-rep0 shard0-rep1 shard0-rep2 shard1-rep0 shard1-rep1 shard1-rep2 shard2-rep0 shard2-rep1 shard2-rep2; do
    exec_rep "$svc" "iptables -F || true; tc qdisc del dev eth0 root 2>/dev/null || true" || true
  done
}

apply_scenario() {
  case "$scenario" in
    partition_2pc_prepare)
      exec_runner "iptables -A OUTPUT -d $(getent hosts shard1-rep0 | awk '{print $1}') -j DROP"
      ;;
    partition_raft_leader)
      exec_rep shard0-rep0 "iptables -A INPUT -p tcp --dport 58000:58099 -j DROP; iptables -A OUTPUT -p tcp --dport 58000:58099 -j DROP"
      ;;
    clock_skew_replica)
      exec_rep shard1-rep1 "date -s '+2 minutes' || true"
      ;;
    duplicate_appendentries)
      echo "duplicate append enabled via TRANS_DB_RAFT_DUPLICATE_APPEND on leaders"
      ;;
    slow_replica_network)
      exec_rep shard2-rep2 "tc qdisc add dev eth0 root netem delay 250ms 50ms"
      ;;
    *)
      echo "unknown scenario: $scenario" >&2
      exit 2
      ;;
  esac
}

case "$action" in
  clear) clear_all ;;
  apply) apply_scenario ;;
  *) echo "usage: faults.sh {apply|clear} <scenario>" >&2; exit 2 ;;
esac
