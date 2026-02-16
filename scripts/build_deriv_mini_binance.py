#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_deriv_mini_binance.py

Fetches a minimal, *raw* derivatives snapshot for BTCUSDT/ETHUSDT from Binance USDT-M Futures API.
Outputs (both in repo root and in docs/):
  deriv/binance/deriv_mini_btc_eth_latest.json

Fail-closed policy:
- If fetch fails and there is a previous snapshot in the repo, we keep it (fallback_used=true) and record errors.
- If fetch fails and there is NO previous snapshot, exit(1).
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

OUT_ROOTS = [Path("."), Path("docs")]
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]

FAPI_BASES = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
]

UA = "ohlcv-feed-iron-deriv-mini/1.0"

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def http_get_json(path: str, params: Dict[str, Any]) -> Any:
    qs = urlencode(params)
    last_err: Optional[Exception] = None
    for base in FAPI_BASES:
        url = f"{base}{path}?{qs}"
        try:
            req = Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
            last_err = e
            continue
    raise RuntimeError(f"All FAPI bases failed for {path} params={params}. Last error: {last_err}")

def fetch_symbol(symbol: str) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    errors: List[Dict[str, str]] = []
    out: Dict[str, Any] = {}
    # 1) premiumIndex (also includes lastFundingRate)
    try:
        out["premiumIndex"] = http_get_json("/fapi/v1/premiumIndex", {"symbol": symbol})
    except Exception as e:
        errors.append({"endpoint": "premiumIndex", "error": str(e)})
    # 2) openInterest snapshot
    try:
        out["openInterest"] = http_get_json("/fapi/v1/openInterest", {"symbol": symbol})
    except Exception as e:
        errors.append({"endpoint": "openInterest", "error": str(e)})
    # 3) openInterestHist 1h 30 bars
    try:
        out["openInterestHist_1h_30"] = http_get_json("/futures/data/openInterestHist", {"symbol": symbol, "period": "1h", "limit": 30})
    except Exception as e:
        errors.append({"endpoint": "openInterestHist", "error": str(e)})
    # 4) fundingRate 30 last (historical)
    try:
        out["fundingRate_30"] = http_get_json("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 30})
    except Exception as e:
        errors.append({"endpoint": "fundingRate", "error": str(e)})
    return out, errors

def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True), encoding="utf-8")

def main() -> None:
    generated_utc = utc_now_iso()
    symbols_out: Dict[str, Any] = {}
    all_errors: List[Dict[str, str]] = []
    for sym in DEFAULT_SYMBOLS:
        data, errs = fetch_symbol(sym)
        symbols_out[sym] = data
        all_errors.extend([{"symbol": sym, **e} for e in errs])

    out = {
        "schema": "iron.deriv_mini.v1",
        "meta": {
            "generated_utc": generated_utc,
            "source": "binance-fapi",
            "symbols": DEFAULT_SYMBOLS,
            "fallback_used": False,
            "errors": all_errors,
        },
        "symbols": symbols_out,
    }

    # If we have any missing critical endpoints, treat as degraded.
    critical_missing = False
    for sym in DEFAULT_SYMBOLS:
        for key in ("premiumIndex", "openInterest", "openInterestHist_1h_30", "fundingRate_30"):
            if key not in symbols_out.get(sym, {}) or symbols_out[sym].get(key) in (None, {}):
                critical_missing = True

    # Output path
    rel = Path("deriv/binance/deriv_mini_btc_eth_latest.json")
    prev_path = Path(".") / rel
    if critical_missing:
        if prev_path.exists():
            # keep previous snapshot, mark fallback, append errors
            prev = json.loads(prev_path.read_text(encoding="utf-8"))
            prev_meta = prev.get("meta", {})
            prev_meta["fallback_used"] = True
            prev_meta["fallback_generated_utc"] = generated_utc
            prev_meta.setdefault("errors", [])
            prev_meta["errors"].extend(all_errors)
            prev["meta"] = prev_meta
            out = prev
        else:
            print("ERROR: Deriv MINI critical endpoints missing AND no previous snapshot to fallback to.")
            print(json.dumps(all_errors, ensure_ascii=False, indent=2))
            raise SystemExit(1)

    for root in OUT_ROOTS:
        write_json(root / rel, out)

if __name__ == "__main__":
    main()
