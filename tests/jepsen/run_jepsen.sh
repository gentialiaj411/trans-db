#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f deploy/compose/docker-compose.yml -f tests/jepsen/compose.jepsen.yml)
RESULTS="$ROOT/tests/jepsen/results"
mkdir -p "$RESULTS"

echo "==> history checker self-test"
python3 tests/jepsen/history_checker.py --self-test

echo "==> build + start cluster"
"${COMPOSE[@]}" build jepsen-runner shard0-rep0 shard1-rep0 shard2-rep0
"${COMPOSE[@]}" up -d
sleep 12

echo "==> run scenarios"
python3 tests/jepsen/run_scenarios.py

echo "==> done; see $RESULTS/SUMMARY.md"
