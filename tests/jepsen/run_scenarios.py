#!/usr/bin/env python3
"""Run Jepsen-style fault scenarios (Docker compose or local gRPC cluster)."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "tests" / "jepsen" / "results"
COMPOSE_BASE = ROOT / "deploy" / "compose" / "docker-compose.yml"
COMPOSE_JEPSEN = ROOT / "tests" / "jepsen" / "compose.jepsen.yml"
FAULTS_SH = ROOT / "tests" / "jepsen" / "faults.sh"


@dataclass
class Scenario:
    name: str
    description: str


SCENARIOS = [
    Scenario("partition_2pc_prepare", "Drop runner -> shard1-rep0 traffic during workload"),
    Scenario("partition_raft_leader", "Partition shard0-rep0 raft ports from peers"),
    Scenario("clock_skew_replica", "Skew clock on shard1-rep1 (best-effort)"),
    Scenario("duplicate_appendentries", "Leaders send duplicate AppendEntries"),
    Scenario("slow_replica_network", "250ms netem delay on shard2-rep2"),
]


def compose_cmd(*args: str) -> list[str]:
    return [
        "docker",
        "compose",
        "-f",
        str(COMPOSE_BASE),
        "-f",
        str(COMPOSE_JEPSEN),
        *args,
    ]


def run(cmd: list[str], check: bool = True, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, text=True, check=check, cwd=ROOT, env=env)


def apply_fault(name: str) -> None:
    if name == "duplicate_appendentries":
        env = os.environ.copy()
        env["TRANS_DB_RAFT_DUPLICATE_APPEND"] = "1"
        run(
            compose_cmd(
                "up",
                "-d",
                "--force-recreate",
                "shard0-rep0",
                "shard1-rep0",
                "shard2-rep0",
            ),
            env=env,
        )
        time.sleep(8)
        return
    run(["bash", str(FAULTS_SH), "apply", name], check=False)


def clear_faults() -> None:
    run(["bash", str(FAULTS_SH), "clear"], check=False)
    env = os.environ.copy()
    env.pop("TRANS_DB_RAFT_DUPLICATE_APPEND", None)
    run(
        compose_cmd("up", "-d", "shard0-rep0", "shard1-rep0", "shard2-rep0"),
        check=False,
        env=env,
    )


def worker_env() -> dict[str, str]:
    env = os.environ.copy()
    env["JEPSEN_EXTERNAL_CLUSTER"] = "1"
    env["JEPSEN_SHARD0"] = "shard0-rep0:57000"
    env["JEPSEN_SHARD1"] = "shard1-rep0:57010"
    env["JEPSEN_SHARD2"] = "shard2-rep0:57020"
    env["JEPSEN_SHARD0_REPLICAS"] = "shard0-rep0:57000,shard0-rep1:57001,shard0-rep2:57002"
    env["JEPSEN_SHARD1_REPLICAS"] = "shard1-rep0:57010,shard1-rep1:57011,shard1-rep2:57012"
    env["JEPSEN_SHARD2_REPLICAS"] = "shard2-rep0:57020,shard2-rep1:57021,shard2-rep2:57022"
    return env


def find_local_worker() -> Path:
    for candidate in (
        ROOT / "build" / "Release" / "jepsen_worker.exe",
        ROOT / "build" / "jepsen_worker.exe",
        ROOT / "build" / "Release" / "jepsen_worker",
        ROOT / "build" / "jepsen_worker",
    ):
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("jepsen_worker not built; run cmake --build build --target jepsen_worker")


def run_workload_docker(scenario: str) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    hist = RESULTS / f"{scenario}_history.jsonl"
    log = RESULTS / f"{scenario}_run.log"
    rel = hist.relative_to(ROOT).as_posix()
    cmd = compose_cmd(
        "exec",
        "-T",
        "jepsen-runner",
        "/usr/local/bin/jepsen_worker",
        "--duration",
        "12",
        "--threads",
        "4",
        "--history",
        f"/workspace/{rel}",
        "--data-dir",
        "/workspace/tests/jepsen/results/data",
    )
    with log.open("w", encoding="utf-8") as lf:
        proc = subprocess.run(
            cmd, cwd=ROOT, env=worker_env(), stdout=lf, stderr=subprocess.STDOUT, text=True
        )
    if proc.returncode != 0:
        raise RuntimeError(f"workload failed for {scenario} (exit {proc.returncode})")
    return hist


def run_workload_local(scenario: str) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    hist = RESULTS / f"{scenario}_history.jsonl"
    log = RESULTS / f"{scenario}_run.log"
    worker = find_local_worker()
    env = os.environ.copy()
    if scenario == "duplicate_appendentries":
        env["TRANS_DB_RAFT_DUPLICATE_APPEND"] = "1"
    else:
        env.pop("TRANS_DB_RAFT_DUPLICATE_APPEND", None)
    cmd = [
        str(worker),
        "--duration",
        "8",
        "--threads",
        "4",
        "--history",
        str(hist),
        "--data-dir",
        str(RESULTS / "local_data"),
    ]
    with log.open("w", encoding="utf-8") as lf:
        proc = subprocess.run(cmd, cwd=ROOT, env=env, stdout=lf, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"local workload failed for {scenario} (exit {proc.returncode})")
    return hist


def check_history(hist: Path) -> tuple[bool, str]:
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tests" / "jepsen" / "history_checker.py"), str(hist)],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    msg = (proc.stdout or proc.stderr or "").strip()
    return proc.returncode == 0, msg


def write_result(scenario: str, passed: bool, detail: str, hist: Path) -> None:
    out = RESULTS / f"{scenario}.md"
    out.write_text(
        "\n".join(
            [
                f"# {scenario}",
                "",
                f"- **pass:** {passed}",
                f"- **detail:** {detail}",
                f"- **history:** `{hist}`",
                "",
            ]
        ),
        encoding="utf-8",
    )


def run_all(local: bool) -> int:
    failures = 0
    for sc in SCENARIOS:
        print(f"=== scenario: {sc.name} ===", flush=True)
        if not local:
            clear_faults()
            time.sleep(1)
        try:
            if not local:
                apply_fault(sc.name)
            hist = run_workload_local(sc.name) if local else run_workload_docker(sc.name)
            ok, msg = check_history(hist)
            write_result(sc.name, ok, msg, hist)
            print(f"{sc.name}: {'PASS' if ok else 'FAIL'} — {msg}")
            if not ok:
                failures += 1
        except Exception as exc:  # noqa: BLE001
            write_result(sc.name, False, str(exc), RESULTS / f"{sc.name}_history.jsonl")
            print(f"{sc.name}: FAIL — {exc}")
            failures += 1
        finally:
            if not local:
                clear_faults()
                time.sleep(1)

    summary = RESULTS / "SUMMARY.md"
    mode = "local-smoke" if local else "docker"
    summary.write_text(
        "\n".join(
            [
                "# Jepsen scenario summary",
                "",
                f"- mode: {mode}",
                f"- total: {len(SCENARIOS)}",
                f"- failed: {failures}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Run workload on local gRPC cluster (no iptables)")
    parser.add_argument("--self-test-only", action="store_true")
    args = parser.parse_args()

    if args.self_test_only:
        proc = subprocess.run(
            [sys.executable, str(ROOT / "tests" / "jepsen" / "history_checker.py"), "--self-test"],
            cwd=ROOT,
        )
        return proc.returncode

    proc = subprocess.run(
        [sys.executable, str(ROOT / "tests" / "jepsen" / "history_checker.py"), "--self-test"],
        cwd=ROOT,
    )
    if proc.returncode != 0:
        print("history checker self-test failed", file=sys.stderr)
        return 1

    if args.local:
        return run_all(local=True)

    return run_all(local=False)


if __name__ == "__main__":
    raise SystemExit(main())
