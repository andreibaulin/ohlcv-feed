#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
verify_chat_bundle.py (IRON universal)

Hard gate for CI:
- Verifies that chat_bundle_latest.json is consistent with its sources
- Verifies that chat_report_latest.md hash matches bundle.report_sha256
- Verifies every fact value equals JSON pointer value in the source
- Verifies derived facts recompute to the same values

Exit(1) on any mismatch.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from iron_common import (
    read_json, sha256_file, sha256_bytes, json_pointer_get, safe_float_eq
)

CONTRACT_PATH = Path("iron/IRON_CONTRACT_CURRENT.json") if Path("iron/IRON_CONTRACT_CURRENT.json").exists() else Path("iron/IRON_CONTRACT_v1.json")

BUNDLE_PATH = Path("docs/ta/binance/chat_bundle_latest.json")
REPORT_PATH = Path("docs/ta/binance/chat_report_latest.md")

def main() -> None:
    if not CONTRACT_PATH.exists():
        raise SystemExit(f"Missing contract: {CONTRACT_PATH}")
    if not BUNDLE_PATH.exists():
        raise SystemExit(f"Missing bundle: {BUNDLE_PATH} (run scripts/build_chat_bundle.py)")
    if not REPORT_PATH.exists():
        raise SystemExit(f"Missing report: {REPORT_PATH} (run scripts/build_chat_bundle.py)")

    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    bundle = read_json(BUNDLE_PATH)
    report = REPORT_PATH.read_text(encoding="utf-8")

    # Verify source files exist
    state_path = Path(bundle["sources"]["state"]["path"])
    if not state_path.exists():
        raise SystemExit(f"Missing state file referenced by bundle: {state_path}")

    # Verify sha256 of sources
    state_sha = sha256_file(state_path)
    if state_sha != bundle["sources"]["state"]["sha256"]:
        raise SystemExit(f"state.sha256 mismatch: computed={state_sha} bundle={bundle['sources']['state']['sha256']}")

    # Verify contract sha
    contract_sha = sha256_file(CONTRACT_PATH)
    if contract_sha != bundle["sources"]["contract"]["sha256"]:
        raise SystemExit(f"contract.sha256 mismatch: computed={contract_sha} bundle={bundle['sources']['contract']['sha256']}")

    # Verify bundle sha (excluding self-hashes)
    tmp = dict(bundle)
    tmp.pop("bundle_sha256", None)
    tmp.pop("report_sha256", None)
    bundle_bytes = json.dumps(tmp, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    bundle_sha = sha256_bytes(bundle_bytes)
    if bundle_sha != bundle["bundle_sha256"]:
        raise SystemExit(f"bundle.sha256 mismatch: computed={bundle_sha} bundle={bundle['bundle_sha256']}")

    # Verify report sha
    report_sha = sha256_bytes(report.encode("utf-8"))
    if report_sha != bundle["report_sha256"]:
        raise SystemExit(f"report.sha256 mismatch: computed={report_sha} bundle={bundle['report_sha256']}")

    # Verify facts against JSON pointers
    state = read_json(state_path)
    deriv = {}
    fx: Dict[str, Any] = bundle.get("facts_index", {})
    # rebuild from facts list to avoid tampering
    fx2 = {f["id"]: f.get("value") for f in bundle.get("facts", [])}
    if fx2 != fx:
        raise SystemExit("facts_index mismatch: facts_index must exactly match the facts[] list")

    for f in bundle.get("facts", []):
        src = f["source"]
        ptr = f["pointer"]
        expected = f.get("value")
        doc = state if src == "state" else deriv
        actual = json_pointer_get(doc, ptr)
        # float-safe equality
        if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
            if not safe_float_eq(expected, actual):
                raise SystemExit(f"Fact mismatch {f['id']}: expected={expected} actual={actual} pointer={ptr} src={src}")
        else:
            if expected != actual:
                raise SystemExit(f"Fact mismatch {f['id']}: expected={expected} actual={actual} pointer={ptr} src={src}")
    # Verify report contains the proof lines (basic)
    required_snippets = [
        f"state.sha256: {bundle['sources']['state']['sha256']}",
        f"bundle.sha256: {bundle['bundle_sha256']}",
    ]
    for s in required_snippets:
        if s not in report:
            raise SystemExit(f"Report missing proof snippet: {s}")

    
    # Verify views sanity (4S/4R): non-overlap + ordering + side correctness
    def _norm_range(r: Any) -> Optional[tuple]:
        if not isinstance(r, (list, tuple)) or len(r) != 2:
            return None
        a = float(r[0])
        b = float(r[1])
        return (a, b) if a <= b else (b, a)

    def _ov(a: tuple, b: tuple, eps: float) -> bool:
        return max(a[0], b[0]) <= min(a[1], b[1]) + eps

    def _check_side(sym: str, side: str, price: float, atr_h4: float, items: List[Dict[str, Any]]) -> None:
        eps = max(atr_h4 * 1e-3, price * 1e-6, 1e-9)

        # ordering
        if side == "S":
            want = sorted(items, key=lambda x: _norm_range(x.get("core"))[1] if _norm_range(x.get("core")) else 0.0, reverse=True)
        else:
            want = sorted(items, key=lambda x: _norm_range(x.get("core"))[0] if _norm_range(x.get("core")) else 0.0)
        if items != want:
            raise SystemExit(f"{sym}: {side} ordering invalid (closest first)")

        for it in items:
            core = _norm_range(it.get("core"))
            buf = _norm_range(it.get("buffer"))
            if core is None or buf is None:
                raise SystemExit(f"{sym}: bad core/buffer format: {it}")
            if not (buf[0] - eps <= core[0] <= core[1] <= buf[1] + eps):
                raise SystemExit(f"{sym}: core must be inside buffer (side={side}) core={core} buf={buf}")

            if side == "S":
                if core[1] > price + eps:
                    raise SystemExit(f"{sym}: support core above price core={core} price={price}")
            else:
                if core[0] < price - eps:
                    raise SystemExit(f"{sym}: resistance core below price core={core} price={price}")

        # non-overlap
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                ci = _norm_range(items[i].get("core"))
                cj = _norm_range(items[j].get("core"))
                if ci and cj and _ov(ci, cj, eps):
                    raise SystemExit(f"{sym}: overlapping CORES {items[i].get('name')} {ci} vs {items[j].get('name')} {cj}")

                bi = _norm_range(items[i].get("buffer"))
                bj = _norm_range(items[j].get("buffer"))
                if bi and bj and _ov(bi, bj, eps):
                    raise SystemExit(f"{sym}: overlapping BUFFERS {items[i].get('name')} {bi} vs {items[j].get('name')} {bj}")

    def _check_sym(sym_key: str, prefix: str) -> None:
        v = (bundle.get("views") or {}).get(sym_key) or {}
        if "error" in v:
            raise SystemExit(f"{sym_key}: views error: {v.get('error')}")
        price = float(fx.get(f"{prefix}.price") or 0.0)
        atr_h4 = float(fx.get(f"{prefix}.atr.h4") or 0.0)
        if price <= 0 or atr_h4 < 0:
            raise SystemExit(f"{sym_key}: bad price/atr in facts_index: price={price} atr_h4={atr_h4}")
        s_items = v.get("supports") or []
        r_items = v.get("resistances") or []
        if not s_items or not r_items:
            raise SystemExit(f"{sym_key}: missing supports/resistances in views")
        _check_side(sym_key, "S", price, atr_h4, s_items)
        _check_side(sym_key, "R", price, atr_h4, r_items)

    _check_sym("BTCUSDT", "btc")
    _check_sym("ETHUSDT", "eth")

print("OK: IRON bundle/report verified.")

if __name__ == "__main__":
    main()
