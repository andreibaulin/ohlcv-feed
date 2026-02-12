#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

OUT_ROOTS = [Path("."), Path("docs")]
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]

FAPI_BASES = [
    "https://fapi.binance.com",
    # иногда в некоторых сетапах data-api проксирует и фьючи; если нет — просто фейлнется, но мы поймаем.
    "https://data-api.binance.vision",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def write_json_compact(path: Path, obj: Any) -> None:
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    atomic_write_bytes(path, s.encode("utf-8"))


def http_get_json(url: str, params: Dict[str, Any], timeout: int = 25) -> Any:
    full = url + ("?" + urlencode(params) if params else "")
    req = Request(full, headers={"User-Agent": "ohlcv-feed (GitHub Actions)", "Accept": "application/json"})
    with urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw.decode("utf-8"))


def try_get(path: str, params: Dict[str, Any], retries: int = 3) -> Any:
    last_err: Exception | None = None
    for base in FAPI_BASES:
        url = base.rstrip("/") + path
        for attempt in range(1, retries + 1):
            try:
                return http_get_json(url, params)
            except (HTTPError, URLError, TimeoutError, ValueError) as e:
                last_err = e
                time.sleep(0.6 * attempt)
    raise RuntimeError(f"fetch failed for {path}: {last_err}")


def parse_symbols_env() -> List[str]:
    raw = os.environ.get("DERIV_SYMBOLS", "").strip()
    if not raw:
        return DEFAULT_SYMBOLS
    out: List[str] = []
    for x in raw.split(","):
        s = x.strip().upper()
        if s:
            out.append(s)
    return out or DEFAULT_SYMBOLS


def main() -> None:
    updated_utc = utc_now_iso()
    symbols = parse_symbols_env()

    out: Dict[str, Any] = {
        "updated_utc": updated_utc,
        "source": "binance_usdtm_futures",
        "symbols": {},
    }

    for sym in symbols:
        entry: Dict[str, Any] = {"errors": []}

        # Funding snapshot (premiumIndex)
        try:
            prem = try_get("/fapi/v1/premiumIndex", {"symbol": sym})
            entry["funding"] = {
                "lastFundingRate": prem.get("lastFundingRate"),
                "nextFundingTime": prem.get("nextFundingTime"),
                "markPrice": prem.get("markPrice"),
                "indexPrice": prem.get("indexPrice"),
                "time": prem.get("time"),
            }
        except Exception as e:
            entry["funding"] = None
            entry["errors"].append(f"funding: {e}")

        # Open interest snapshot
        try:
            oi = try_get("/fapi/v1/openInterest", {"symbol": sym})
            entry["open_interest"] = {
                "openInterest": oi.get("openInterest"),
                "time": oi.get("time"),
            }
        except Exception as e:
            entry["open_interest"] = None
            entry["errors"].append(f"open_interest: {e}")

        # Global long/short account ratio (last point)
        try:
            gls = try_get(
                "/futures/data/globalLongShortAccountRatio",
                {"symbol": sym, "period": "4h", "limit": 30},
            )
            if isinstance(gls, list) and gls:
                entry["global_long_short_account_ratio"] = gls[-1]
            else:
                entry["global_long_short_account_ratio"] = gls
            entry["global_long_short_account_ratio_note"] = "period=4h, last point"
        except Exception as e:
            entry["global_long_short_account_ratio"] = None
            entry["errors"].append(f"long_short: {e}")

        out["symbols"][sym] = entry

    for root in OUT_ROOTS:
        d = root / "deriv" / "binance"
        d.mkdir(parents=True, exist_ok=True)
        write_json_compact(d / "core5_latest.json", out)
        write_json_compact(d / "core10_latest.json", out)


if __name__ == "__main__":
    main()
