#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""verify_chat_bundle.py (IRON Guard, non-blocking)

B+C mode expectations:
- build_chat_bundle.py writes docs/ta/binance/build_status_latest.json every run.
- This verifier MUST NEVER crash the workflow. It always exits 0.
- It may downgrade/upgrade status.quality:
    - adds verify.errors / verify.warnings
    - if verify.errors present -> forces quality=FAIL
    - otherwise keeps existing quality (OK/WARN)

What it checks (lightweight but reliable):
- build_status_latest.json is readable/writable
- target bundle/report exist (candidate -> latest -> bad_latest)
- bundle JSON parses as dict
- if bundle contains bundle_sha256/report_sha256 -> verify them
- basic schema sanity (has symbols, views or levels, sources block if present)

This file is intentionally standalone (no project imports) to avoid import-related failures.
"""

from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DOCS_ROOT = Path("docs")
STATUS_PATH = DOCS_ROOT / "ta/binance/build_status_latest.json"

# Canonical rel paths (what builder should write)
REL_LATEST_BUNDLE = "ta/binance/chat_bundle_latest.json"
REL_LATEST_REPORT = "ta/binance/chat_report_latest.md"
REL_BAD_BUNDLE = "ta/binance/chat_bundle_bad_latest.json"
REL_BAD_REPORT = "ta/binance/chat_report_bad_latest.md"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def _write_json(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _choose_target(status: Dict[str, Any]) -> Optional[Dict[str, str]]:
    cand = status.get("candidate")
    if isinstance(cand, dict) and isinstance(cand.get("bundle_rel"), str) and isinstance(cand.get("report_rel"), str):
        return {"bundle_rel": cand["bundle_rel"], "report_rel": cand["report_rel"]}

    # latest first
    if (DOCS_ROOT / REL_LATEST_BUNDLE).exists() and (DOCS_ROOT / REL_LATEST_REPORT).exists():
        return {"bundle_rel": REL_LATEST_BUNDLE, "report_rel": REL_LATEST_REPORT}

    # bad_latest as fallback
    if (DOCS_ROOT / REL_BAD_BUNDLE).exists() and (DOCS_ROOT / REL_BAD_REPORT).exists():
        return {"bundle_rel": REL_BAD_BUNDLE, "report_rel": REL_BAD_REPORT}

    return None


def _force_status_shape(status: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(status, dict):
        status = {}
    status.setdefault("schema", "iron.build_status.v1")
    status.setdefault("generated_utc", None)
    status.setdefault("quality", "FAIL")
    status.setdefault("kept_last_good", False)
    status.setdefault("warnings", [])
    status.setdefault("errors", [])
    status.setdefault("verify", {"ran": False, "errors": [], "warnings": []})
    return status


def main() -> None:
    verify_errors: List[str] = []
    verify_warnings: List[str] = []

    # Load or init status
    status: Dict[str, Any] = {}
    if STATUS_PATH.exists():
        try:
            status = _read_json(STATUS_PATH)
        except Exception as e:
            status = {}
            verify_warnings.append(f"verify: cannot read existing build status, will recreate: {e}")
    status = _force_status_shape(status)

    target = _choose_target(status)
    if not target:
        verify_errors.append("verify: no target bundle/report found (candidate/latest/bad_latest missing).")

        status["verify"] = {"ran": True, "errors": verify_errors, "warnings": verify_warnings}
        status["quality"] = "FAIL"
        status["errors"] = list(dict.fromkeys((status.get("errors") or []) + verify_errors))
        status["warnings"] = list(dict.fromkeys((status.get("warnings") or []) + verify_warnings))
        _write_json(STATUS_PATH, status)
        return

    bundle_path = DOCS_ROOT / target["bundle_rel"]
    report_path = DOCS_ROOT / target["report_rel"]

    if not bundle_path.exists():
        verify_errors.append(f"verify: missing bundle: {bundle_path}")
    if not report_path.exists():
        verify_errors.append(f"verify: missing report: {report_path}")

    bundle: Dict[str, Any] = {}
    report_text: str = ""

    if bundle_path.exists():
        try:
            obj = _read_json(bundle_path)
            if not isinstance(obj, dict):
                verify_errors.append("verify: bundle is not a JSON object (dict)")
            else:
                bundle = obj
        except Exception as e:
            verify_errors.append(f"verify: bundle parse error: {e}")

    if report_path.exists():
        try:
            report_text = report_path.read_text(encoding="utf-8")
        except Exception as e:
            verify_errors.append(f"verify: report read error: {e}")

    # If we have the bundle, do integrity checks
    if bundle:
        # Basic schema sanity (non-fatal warnings)
        if not isinstance(bundle.get("symbols"), list):
            verify_warnings.append("verify: bundle.symbols missing or not a list")
        if "views" not in bundle and "levels" not in bundle:
            verify_warnings.append("verify: bundle has no 'views' and no 'levels' (schema?)")

        # Verify bundle hash if provided
        try:
            bundle_sha_expected = bundle.get("bundle_sha256")
            if isinstance(bundle_sha_expected, str) and bundle_sha_expected:
                tmp = dict(bundle)
                tmp.pop("bundle_sha256", None)
                tmp.pop("report_sha256", None)
                tmp_bytes = json.dumps(tmp, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
                bundle_sha = _sha256_bytes(tmp_bytes)
                if bundle_sha != bundle_sha_expected:
                    verify_errors.append(f"verify: bundle_sha256 mismatch: computed={bundle_sha} expected={bundle_sha_expected}")
        except Exception as e:
            verify_warnings.append(f"verify: bundle sha check skipped due to error: {e}")

        # Verify report hash if provided
        try:
            report_sha_expected = bundle.get("report_sha256")
            if isinstance(report_sha_expected, str) and report_sha_expected and report_text:
                report_sha = _sha256_bytes(report_text.encode("utf-8"))
                if report_sha != report_sha_expected:
                    verify_errors.append(f"verify: report_sha256 mismatch: computed={report_sha} expected={report_sha_expected}")
        except Exception as e:
            verify_warnings.append(f"verify: report sha check skipped due to error: {e}")

        # Optional: verify source files referenced by bundle exist (warning -> error only if strict)
        try:
            sources = bundle.get("sources") or {}
            if isinstance(sources, dict):
                st = sources.get("state") or {}
                if isinstance(st, dict) and isinstance(st.get("path"), str):
                    sp = Path(st["path"])
                    if not sp.exists():
                        verify_warnings.append(f"verify: referenced state file missing: {sp}")
        except Exception as e:
            verify_warnings.append(f"verify: sources check error: {e}")

    # Write verify results back to status
    status["verify"] = {"ran": True, "errors": verify_errors, "warnings": verify_warnings, "target": target}

    if verify_errors:
        status["quality"] = "FAIL"
        status["errors"] = list(dict.fromkeys((status.get("errors") or []) + verify_errors))
    status["warnings"] = list(dict.fromkeys((status.get("warnings") or []) + verify_warnings))

    _write_json(STATUS_PATH, status)

    # Print a short summary (useful in Actions log)
    if verify_errors:
        print("VERIFY: FAIL")
        for e in verify_errors[:10]:
            print(" -", e)
    elif verify_warnings:
        print("VERIFY: WARN")
        for w in verify_warnings[:10]:
            print(" -", w)
    else:
        print("VERIFY: OK")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Absolute last-resort: never fail the step due to verifier issues.
        fallback = _force_status_shape({})
        fallback["verify"] = {"ran": True, "errors": [f"verify: crashed: {e}"], "warnings": []}
        fallback["quality"] = "FAIL"
        fallback["errors"] = [f"verify: crashed: {e}"]
        _write_json(STATUS_PATH, fallback)
        print("VERIFY: CRASHED -> wrote FAIL status")
