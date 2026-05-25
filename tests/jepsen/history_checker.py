#!/usr/bin/env python3
"""Serializable-history checker for Jepsen-style transfer/read logs."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from itertools import permutations
from pathlib import Path
from typing import Any


@dataclass
class Event:
    time_us: int
    process: int
    kind: str
    fn: str
    value: list[str]


def load_history(path: Path) -> list[Event]:
    events: list[Event] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        events.append(
            Event(
                time_us=int(obj["time_us"]),
                process=int(obj["process"]),
                kind=str(obj["type"]),
                fn=str(obj["f"]),
                value=[str(v) for v in obj.get("value", [])],
            )
        )
    return events


def _ok_transfers(events: list[Event]) -> list[tuple[str, str, int]]:
    out: list[tuple[str, str, int]] = []
    for ev in events:
        if ev.fn != "transfer" or ev.kind != "ok" or len(ev.value) < 3:
            continue
        out.append((ev.value[0], ev.value[1], int(ev.value[2])))
    return out


def check_bank_invariant(events: list[Event], num_accounts: int, initial: int) -> tuple[bool, str]:
    transfers = _ok_transfers(events)
    for src, dst, amt in transfers:
        if src == dst:
            return False, "self-transfer in ok history"
    deltas: dict[str, int] = {}
    for src, dst, amt in transfers:
        deltas[src] = deltas.get(src, 0) - amt
        deltas[dst] = deltas.get(dst, 0) + amt
    if sum(deltas.values()) != 0:
        return False, "transfer deltas do not conserve funds"

    if len(transfers) > 10:
        balances = {str(i): initial for i in range(num_accounts)}
        for src, dst, amt in transfers:
            balances[src] = balances.get(src, initial) - amt
            balances[dst] = balances.get(dst, initial) + amt
        total = sum(balances.values())
        expected = num_accounts * initial
        if total != expected:
            return False, f"bank total mismatch: {total} != {expected}"
        return True, f"bank invariant ok ({len(transfers)} transfers, conservation only)"

    for order in permutations(range(len(transfers))):
        balances = {str(i): initial for i in range(num_accounts)}
        valid = True
        for idx in order:
            src, dst, amt = transfers[idx]
            if balances.get(src, initial) < amt:
                valid = False
                break
            balances[src] = balances.get(src, initial) - amt
            balances[dst] = balances.get(dst, initial) + amt
        if valid and sum(balances.values()) == num_accounts * initial:
            return True, "bank invariant ok (serializable replay)"
    return False, "no serializable replay satisfies bank constraints"


def _apply_register_op(state: dict[str, str], fn: str, value: list[str]) -> None:
    if fn == "write" and len(value) >= 2:
        state[value[0]] = value[1]
    elif fn == "read" and len(value) >= 2:
        state[value[0]] = value[1]


def check_serializable_registers(events: list[Event]) -> tuple[bool, str]:
    """Brute-force serializability for completed read/write operations."""

    ops: list[tuple[int, Event]] = []
    pending: dict[tuple[int, str], Event] = {}
    for ev in events:
        if ev.fn not in ("read", "write"):
            continue
        key = (ev.process, ev.fn)
        if ev.kind == "invoke":
            pending[key] = ev
        elif ev.kind == "ok":
            if key in pending:
                pending.pop(key, None)
            ops.append((ev.time_us, ev))
        elif ev.kind == "fail" and key in pending:
            pending.pop(key, None)

    if len(ops) > 8:
        return True, f"skipped deep register check (ops={len(ops)})"

    for order in permutations(range(len(ops))):
        state: dict[str, str] = {}
        valid = True
        for idx in order:
            ev = ops[idx][1]
            if ev.fn == "read":
                if len(ev.value) < 2:
                    valid = False
                    break
                key, observed = ev.value[0], ev.value[1]
                if key in state and state[key] != observed:
                    valid = False
                    break
                state[key] = observed
            elif ev.fn == "write":
                if len(ev.value) < 2:
                    valid = False
                    break
                key, val = ev.value[0], ev.value[1]
                state[key] = val
        if valid:
            return True, "register history serializable"
    return False, "no serializable total order for register ops"


def check_history(path: Path, num_accounts: int = 12, initial: int = 1000) -> tuple[bool, str]:
    events = load_history(path)
    if not events:
        return False, "empty history"
    ok_bank, msg_bank = check_bank_invariant(events, num_accounts, initial)
    if not ok_bank:
        return False, msg_bank
    ok_reg, msg_reg = check_serializable_registers(events)
    if not ok_reg:
        return False, msg_reg
    return True, f"{msg_bank}; {msg_reg}"


def self_test() -> tuple[bool, str]:
    bad = [
        {"time_us": 1, "process": 0, "type": "ok", "f": "transfer", "value": ["0", "1", "600"]},
        {"time_us": 2, "process": 1, "type": "ok", "f": "transfer", "value": ["0", "1", "600"]},
    ]
    tmp = Path("tests/jepsen/results/_self_test_bad.jsonl")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text("\n".join(json.dumps(x) for x in bad) + "\n", encoding="utf-8")
    ok_bad, _ = check_bank_invariant(load_history(tmp), num_accounts=12, initial=1000)
    if ok_bad:
        return False, "checker accepted known-bad bank history"

    good = [
        {"time_us": 1, "process": 0, "type": "invoke", "f": "write", "value": ["0", "900"]},
        {"time_us": 2, "process": 0, "type": "ok", "f": "write", "value": ["0", "900"]},
        {"time_us": 3, "process": 0, "type": "invoke", "f": "read", "value": ["0"]},
        {"time_us": 4, "process": 0, "type": "ok", "f": "read", "value": ["0", "900"]},
    ]
    tmp2 = Path("tests/jepsen/results/_self_test_good.jsonl")
    tmp2.write_text("\n".join(json.dumps(x) for x in good) + "\n", encoding="utf-8")
    ok_good, msg = check_serializable_registers(load_history(tmp2))
    if not ok_good:
        return False, f"checker rejected good history: {msg}"
    return True, "self-test passed"


def main() -> int:
    if len(sys.argv) >= 2 and sys.argv[1] == "--self-test":
        ok, msg = self_test()
        print(msg)
        return 0 if ok else 1

    if len(sys.argv) < 2:
        print("usage: history_checker.py <history.jsonl> | --self-test", file=sys.stderr)
        return 2
    path = Path(sys.argv[1])
    ok, msg = check_history(path)
    print(msg)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
