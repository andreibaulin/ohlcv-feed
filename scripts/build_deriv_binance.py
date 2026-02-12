#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Binance USDⓈ-M Futures derivatives snapshot for BTC/ETH:
- funding (via premiumIndex)
- open interest
- global long/short account ratio

Output:
- docs/deriv/binance/core5_latest.json
- docs/deriv/binance/core10_latest.json

Без API ключей (публичные эндпоинты).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


SYMBOLS: List[str] = ["BTCUSDT", "ETHUSDT"]

FAPI = "https://fapi.binance.com"

# Long/Short ratio period. 15m — компромисс: меньше шума, но всё ещё “свежее”.
LS_PERIOD = "15m"
LS_LIMIT = 1

OUT_DIR = Path("docs/deriv/binance")
OUT_CORE5 = OUT_DIR / "core5_latest.json"
OUT_CORE10 = OUT_DIR / "core10_latest.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def http_get_json(url: str, params: Dict[str, Any], timeout: int = 20) -> Any:
    full = url + "?" + urlencode(params)
    req = Request(
        full,
        headers={
            "User-Agent": "ohlcv-feed/deriv (GitHub Actions)",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw.decode("utf-8"))


def atomic_write(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    tmp.write_text(s, encoding="utf-8")
    tmp.replace(path)


def safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs), None
    except (HTTPError, URLError, TimeoutError, ValueError) as e:
        return None, str(e)


def main() -> None:
    updated = utc_now_iso()

    out: Dict[str, Any] = {
        "updated_utc": updated,
        "source": "binance_usdm_futures",
        "symbols": {},
        "notes": {
            "liq": "not_collected (websocket forceOrder is event-driven; use external feed if needed)",
        },
    }

    for sym in SYMBOLS:
        sym_obj: Dict[str, Any] = {}

        # funding + mark/index price
        prem, err = safe_call(http_get_json, f"{FAPI}/fapi/v1/premiumIndex", {"symbol": sym})
        if prem is None:
            sym_obj["funding"] = {"error": err}
        else:
            # keep key names from Binance response for максимальной совместимости
            sym_obj["funding"] = {
                "time": prem.get("time"),
                "markPrice": prem.get("markPrice"),
                "indexPrice": prem.get("indexPrice"),
                "lastFundingRate": prem.get("lastFundingRate"),
                "nextFundingTime": prem.get("nextFundingTime"),
            }

        # open interest
        oi, err = safe_call(http_get_json, f"{FAPI}/fapi/v1/openInterest", {"symbol": sym})
        if oi is None:
            sym_obj["open_interest"] = {"error": err}
        else:
            sym_obj["open_interest"] = {
                "openInterest": oi.get("openInterest"),
                "time": oi.get("time"),
            }

        # global long/short account ratio (trading data)
        ls, err = safe_call(
            http_get_json,
            f"{FAPI}/futures/data/globalLongShortAccountRatio",
            {"symbol": sym, "period": LS_PERIOD, "limit": LS_LIMIT},
        )
        if ls is None:
            sym_obj["global_long_short"] = {"error": err, "period": LS_PERIOD}
        else:
            last = ls[-1] if isinstance(ls, list) and ls else {}
            sym_obj["global_long_short"] = {
                "period": LS_PERIOD,
                "longShortRatio": last.get("longShortRatio"),
                "longAccount": last.get("longAccount"),
                "shortAccount": last.get("shortAccount"),
                "timestamp": last.get("timestamp"),
            }

        out["symbols"][sym] = sym_obj

    # core5/core10 — одинаковые (пока ты не хочешь лишних метрик)
    atomic_write(OUT_CORE5, out)
    atomic_write(OUT_CORE10, out)

    print(f"OK: wrote {OUT_CORE5} and {OUT_CORE10} @ {updated}")


if __name__ == "__main__":
    main()
