#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests


BINANCE_SPOT_KLINES = "https://api.binance.com/api/v3/klines"

SYMBOLS = ["BTCUSDT", "ETHUSDT"]

TF_CONFIG = {
    # ключи — как тебе привычно в проекте
    "H1": {"interval": "1h", "limit": 240},
    "H4": {"interval": "4h", "limit": 240},
    "D1": {"interval": "1d", "limit": 400},
    "W1": {"interval": "1w", "limit": 260},
}

OUT_DIR = os.path.join("ohlcv", "binance")
OUT_PACK = os.path.join(OUT_DIR, "pack_btc_eth.txt")
OUT_STATUS = os.path.join(OUT_DIR, "status_btc_eth.json")

HTTP_TIMEOUT_SEC = 20
RETRIES = 5
BACKOFF_BASE_SEC = 1.2


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ms_to_utc_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_klines(symbol: str, interval: str, limit: int) -> List[List[Any]]:
    params = {"symbol": symbol, "interval": interval, "limit": int(limit)}
    last_err = None

    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(BINANCE_SPOT_KLINES, params=params, timeout=HTTP_TIMEOUT_SEC)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            data = r.json()
            if not isinstance(data, list) or not data:
                raise RuntimeError("Empty klines payload")
            return data
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                sleep_s = BACKOFF_BASE_SEC ** attempt
                time.sleep(sleep_s)
            else:
                break

    raise RuntimeError(f"Failed to fetch klines {symbol} {interval} limit={limit}: {last_err}")


def normalize_klines(raw: List[List[Any]]) -> Tuple[List[List[float]], int, int]:
    """
    Binance kline:
    [
      [
        0 openTime(ms),
        1 open(str),
        2 high(str),
        3 low(str),
        4 close(str),
        5 volume(str),
        6 closeTime(ms),
        ...
      ],
      ...
    ]

    Return:
      data6: [ [t_ms, o, h, l, c, v], ... ]  (floats)
      first_open_ms
      last_close_ms
    """
    out: List[List[float]] = []
    first_open = int(raw[0][0])
    last_close = int(raw[-1][6])

    for k in raw:
        t = float(int(k[0]))
        o = float(k[1])
        h = float(k[2])
        l = float(k[3])
        c = float(k[4])
        v = float(k[5])
        out.append([t, o, h, l, c, v])

    return out, first_open, last_close


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    updated_utc = utc_now_iso()
    pack: Dict[str, Any] = {
        "meta": {
            "source": "Binance Spot /api/v3/klines",
            "symbols": SYMBOLS,
            "tfs": list(TF_CONFIG.keys()),
            "updated_utc": updated_utc,
            "notes": "This file is intentionally small and multiline so ChatGPT/web loaders can always read it.",
        }
    }

    status: Dict[str, Any] = {"updated_utc": updated_utc, "symbols": {}}

    for sym in SYMBOLS:
        pack[sym] = {}
        status["symbols"][sym] = {}

        for tf_key, cfg in TF_CONFIG.items():
            interval = cfg["interval"]
            limit = int(cfg["limit"])

            raw = fetch_klines(sym, interval, limit)
            data6, first_open_ms, last_close_ms = normalize_klines(raw)

            pack[sym][tf_key] = {
                "tf": tf_key,
                "interval": interval,
                "bars": len(data6),
                "first_open_utc": ms_to_utc_iso(first_open_ms),
                "last_close_utc": ms_to_utc_iso(last_close_ms),
                "data": data6,
            }

            status["symbols"][sym][tf_key] = {
                "interval": interval,
                "bars": len(data6),
                "first_open_utc": ms_to_utc_iso(first_open_ms),
                "last_close_utc": ms_to_utc_iso(last_close_ms),
            }

    # Пишем МНОГОСТРОЧНО (важно), чтобы не было “одна строка на мегабайты”
    with open(OUT_PACK, "w", encoding="utf-8") as f:
        json.dump(pack, f, ensure_ascii=False, indent=2)

    with open(OUT_STATUS, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

    print(f"OK: wrote {OUT_PACK} and {OUT_STATUS} updated_utc={updated_utc}")


if __name__ == "__main__":
    main()
