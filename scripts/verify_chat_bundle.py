#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
verify_chat_bundle.py (IRON universal)

Non-blocking verifier for CI (B+C mode):
- Verifies candidate bundle/report against sources and internal hashes
- Writes results into docs/ta/binance/build_status_latest.json
- NEVER exits non-zero (the workflow "Signal quality" step decides job status)

This is intentionally redundant: build_chat_bundle.py already self-verifies.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from iron_common import (
    read_json, write_json, sha256_file, sha256_bytes, json_pointer_get, safe_float_eq
)

CONTRACT_PATH = Path("iron/IRON_CONTRACT_CURRENT.json") if Path("iron/IRON_CONTRACT_CURRENT.json").exists() else Path("iron/IRON_CONTRACT_v1.json")

DOCS_ROOT = Path("docs")
STATUS_PATH = DOCS_ROOT / "ta/binance/build_status_latest.json"


def _choose_target(status: Dict[str, Any]) -> Optional[Dict[str, str]]:
    cand = status.get("candidate")
    if isinstance(cand, dict) and cand.get("bundle_rel") and cand.get("report_rel"):
        return {"bundle_rel": cand["bundle_rel"], "report_rel": cand["report_rel"]}

    # Fallbacks (if status is missing/corrupt)
    rel_latest_b = "ta/binance/chat_bundle_latest.json"
    rel_latest_r = "ta/binance/chat_report_latest.md"
    if (DOCS_ROOT / rel_latest_b).exists() and (DOCS_ROOT / rel_latest_r).exists():
        return {"bundle_rel": rel_latest_b, "report_rel": rel_latest_r}

    rel_bad_b = "ta/binance/chat_bundle_bad_latest.json"
    rel_bad_r = "ta/binance/chat_report_bad_latest.md"
    if (DOCS_ROOT / rel_bad_b).exists() and (DOCS_ROOT / rel_bad_r).exists():
        return {"bundle_rel": rel_bad_b, "report_rel": rel_bad_r}

    return None


def main() -> None:
    status: Dict[str, Any] = {}
    if STATUS_PATH.exists():
        try:
            status = read_json(STATUS_PATH)
        except Exception:
            status = {}
    if not isinstance(status, dict):
        status = {}

    status.setdefault("schema", "iron.build_status.v1")
    status.setdefault("generated_utc", None)
    status.setdefault("quality", "FAIL")
    status.setdefault("warnings", [])
    status.setdefault("errors", [])
    status.setdefault("verify", {"ran": False, "errors": [], "warnings": []})

    verify_errors: List[str] = []
    verify_warnings: List[str] = []

    target = _choose_target(status)
    if not target:
        verify_errors.append("verify: no target bundle/report found (neither candidate nor latest nor bad_latest).")
        status["verify"] = {"ran": True, "errors": verify_errors, "warnings": verify_warnings}
        status["quality"] = "FAIL"
        status["errors"] = list(dict.fromkeys((status.get("errors") or []) + verify_errors))
        write_json(STATUS_PATH, status)
        return

    bundle_path = DOCS_ROOT / target["bundle_rel"]
    report_path = DOCS_ROOT / target["report_rel"]

    if not CONTRACT_PATH.exists():
        verify_errors.append(f"verify: missing contract: {CONTRACT_PATH}")

    if not bundle_path.exists():
        verify_errors.append(f"verify: missing bundle: {bundle_path}")
    if not report_path.exists():
        verify_errors.append(f"verify: missing report: {report_path}")

    bundle: Dict[str, Any] = {}
    report = ""
    contract: Dict[str, Any] = {}

    if CONTRACT_PATH.exists():
        try:
            contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            verify_errors.append(f"verify: contract read/parse error: {e}")

    if bundle_path.exists():
        try:
            bundle = read_json(bundle_path)
        except Exception as e:
            verify_errors.append(f"verify: bundle read/parse error: {e}")

    if report_path.exists():
        try:
            report = report_path.read_text(encoding="utf-8")
        except Exception as e:
            verify_errors.append(f"verify: report read error: {e}")

    # Run checks only if we have bundle & report
    if bundle and report:
        # Verify source files exist
        try:
            state_path = Path(bundle["sources"]["state"]["path"])
            if not state_path.exists():
                verify_errors.append(f"verify: missing state file referenced by bundle: {state_path}")
        except Exception as e:
            verify_errors.append(f"verify: bad sources.state.path: {e}")
            state_path = None  # type: ignore

        # Verify sha256 of sources
        if state_path and getattr(state_path, "exists", lambda: False)():
            state_sha = sha256_file(state_path)
            if state_sha != bundle["sources"]["state"].get("sha256"):
                verify_errors.append(f"verify: state.sha256 mismatch: computed={state_sha} bundle={bundle['sources']['state'].get('sha256')}")

        # Verify contract sha
        if CONTRACT_PATH.exists():
            contract_sha = sha256_file(CONTRACT_PATH)
            if contract_sha != bundle["sources"]["contract"].get("sha256"):
                verify_errors.append(f"verify: contract.sha256 mismatch: computed={contract_sha} bundle={bundle['sources']['contract'].get('sha256')}")

        # Verify bundle sha (excluding self-hashes)
        tmp = dict(bundle)
        tmp.pop("bundle_sha256", None)
        tmp.pop("report_sha256", None)
        bundle_bytes = json.dumps(tmp, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        bundle_sha = sha256_bytes(bundle_bytes)
        if bundle_sha != bundle.get("bundle_sha256"):
            verify_errors.append(f"verify: bundle.sha256 mismatch: computed={bundle_sha} bundle={bundle.get('bundle_sha256')}")

        # Verify report sha
        report_sha = sha256_bytes(report.encode("utf-8"))
        if report_sha != bundle.get("report_sha256"):
            verify_errors.append(f"verify: report.sha256 mismatch: computed={report_sha} bundle={bundle.get('report_sha256')}")

        # Verify facts against JSON pointers
        try:
            state_doc = read_json(state_path) if state_path else {}
        except Exception as e:
            verify_errors.append(f"verify: state JSON read error: {e}")
            state_doc = {}

        deriv_doc: Dict[str, Any] = {}

        fx: Dict[str, Any] = bundle.get("facts_index", {}) or {}
        fx2 = {f.get("id"): f.get("value") for f in bundle.get("facts", [])}
        if fx2 != fx:
            verify_errors.append("verify: facts_index mismatch: facts_index must exactly match the facts[] list")

        for f in bundle.get("facts", []) or []:
            try:
                fid = f["id"]
                src = f["source"]
                ptr = f["pointer"]
                expected = f.get("value")
                doc = state_doc if src == "state" else deriv_doc
                actual = json_pointer_get(doc, ptr)
                if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
                    if not safe_float_eq(float(expected), float(actual)):
                        verify_errors.append(f"verify: fact mismatch {fid}: expected={expected} actual={actual} pointer={ptr} src={src}")
                else:
                    if expected != actual:
                        verify_errors.append(f"verify: fact mismatch {fid}: expected={expected} actual={actual} pointer={ptr} src={src}")
            except Exception as e:
                verify_errors.append(f"verify: fact check exception: {e}")

        # Verify report contains proof snippets
        try:
            required_snippets = [
                f"state.sha256: {bundle['sources']['state']['sha256']}",
                f"bundle.sha256: {bundle['bundle_sha256']}",
            ]
            for s in required_snippets:
                if s not in report:
                    verify_errors.append(f"verify: report missing proof snippet: {s}")
        except Exception as e:
            verify_errors.append(f"verify: report proof check exception: {e}")

    # Update status
    status["verify"] = {"ran": True, "errors": verify_errors, "warnings": verify_warnings, "target": target}

    if verify_errors:
        status["quality"] = "FAIL"
        status["errors"] = list(dict.fromkeys((status.get("errors") or []) + verify_errors))
    else:
        # keep existing quality (set by builder)
        pass

    write_json(STATUS_PATH, status)
    print("OK: verify finished (non-blocking).")


if __name__ == "__main__":
    main()
