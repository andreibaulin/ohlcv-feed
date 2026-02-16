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

BUNDLE_PATH = Path("ta/binance/chat_bundle_latest.json")
REPORT_PATH = Path("ta/binance/chat_report_latest.md")

def recompute_derived(fx: Dict[str, Any]) -> Dict[str, Optional[float]]:
    def premium_pct(pidx: Dict[str, Any]) -> Optional[float]:
        try:
            mark = float(pidx.get("markPrice"))
            idx = float(pidx.get("indexPrice"))
            if idx == 0:
                return None
            return (mark - idx) / idx * 100.0
        except Exception:
            return None

    def oi_delta_pct(hist: Any) -> Optional[float]:
        try:
            if not isinstance(hist, list) or len(hist) < 2:
                return None
            first = float(hist[0].get("openInterest"))
            last = float(hist[-1].get("openInterest"))
            if first == 0:
                return None
            return (last - first) / first * 100.0
        except Exception:
            return None

    out: Dict[str, Optional[float]] = {}
    for prefix in ("btc","eth"):
        pidx = fx.get(f"{prefix}.deriv.premiumIndex")
        hist = fx.get(f"{prefix}.deriv.openInterestHist_1h_30")
        out[f"{prefix}.deriv.premium_pct"] = premium_pct(pidx) if isinstance(pidx, dict) else None
        out[f"{prefix}.deriv.oi_delta_pct_30h"] = oi_delta_pct(hist)
    return out

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
    deriv_path = Path(bundle["sources"]["deriv_mini"]["path"])
    if not state_path.exists():
        raise SystemExit(f"Missing state file referenced by bundle: {state_path}")
    if not deriv_path.exists():
        raise SystemExit(f"Missing deriv_mini file referenced by bundle: {deriv_path}")

    # Verify sha256 of sources
    state_sha = sha256_file(state_path)
    deriv_sha = sha256_file(deriv_path)
    if state_sha != bundle["sources"]["state"]["sha256"]:
        raise SystemExit(f"state.sha256 mismatch: computed={state_sha} bundle={bundle['sources']['state']['sha256']}")
    if deriv_sha != bundle["sources"]["deriv_mini"]["sha256"]:
        raise SystemExit(f"deriv_mini.sha256 mismatch: computed={deriv_sha} bundle={bundle['sources']['deriv_mini']['sha256']}")

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
    deriv = read_json(deriv_path)
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

    # Verify derived facts
    derived = {d["id"]: d.get("value") for d in bundle.get("derived_facts", [])}
    recomputed = recompute_derived(fx)
    for k, v in recomputed.items():
        if k not in derived:
            raise SystemExit(f"Missing derived fact: {k}")
        dv = derived[k]
        if dv is None and v is None:
            continue
        if isinstance(dv, (int, float)) and isinstance(v, (int, float)):
            if not safe_float_eq(dv, v, eps=1e-9):
                raise SystemExit(f"Derived mismatch {k}: bundle={dv} recomputed={v}")
        else:
            if dv != v:
                raise SystemExit(f"Derived mismatch {k}: bundle={dv} recomputed={v}")

    # Verify report contains the proof lines (basic)
    required_snippets = [
        f"state.sha256: {bundle['sources']['state']['sha256']}",
        f"deriv_mini.sha256: {bundle['sources']['deriv_mini']['sha256']}",
        f"bundle.sha256: {bundle['bundle_sha256']}",
        f"report.sha256: {bundle['report_sha256']}",
    ]
    for s in required_snippets:
        if s not in report:
            raise SystemExit(f"Report missing proof snippet: {s}")

    print("OK: IRON bundle/report verified.")

if __name__ == "__main__":
    main()
