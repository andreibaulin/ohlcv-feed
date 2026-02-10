#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# ---- CONFIG (совпадает с тем, что у тебя уже отдаётся в symbols/status)
SYMBOLS_CORE10: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "AVAXUSDT",
    "LINKUSDT", "AAVEUSDT", "UNIUSDT", "ARBUSDT", "ADAUSDT",
]
SYMBOLS_CORE5: List[str] = SYMBOLS_CORE10[:5]
CRITICAL = {"BTCUSDT", "ETHUSDT"}

TFS: List[str] = ["H1", "H4", "D1", "W1"]
TF_TO_INTERVAL: Dict[str, str] = {"H1": "1h", "H4": "4h", "D1": "1d", "W1": "1w"}

# Binance max limit = 1000
FETCH_LIMIT: Dict[str, int] = {"H1": 1000, "H4": 1000, "D1": 1000, "W1": 1000}

# Хвост для “быстрого дерганья”
TAIL_N: Dict[str, int] = {"H1": 240, "H4": 240, "D1": 400, "W1": 260}

# фильтр незакрытого бара
SAFETY_MS = 60_000

# ВАЖНО: первым — data-api (у тебя уже был 451 на api.binance.com из GitHub Actions)
KLINES_ENDPOINTS = [
    "https://data-api.binance.vision/api/v3/klines",
    "https://api.binance.com/api/v3/klines",
]

OUT_DIR = Path("ohlcv/binance")
PAGES_BASE_URL = "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/"


# ---- helpers
def utc_now_iso(microseconds: bool = True) -> str:
    dt = datetime.now(timezone.utc)
    if not microseconds:
        dt = dt.replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def ms_to_utc_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def write_json(path: Path, obj: Any, compact: bool = True) -> None:
    if compact:
        s = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    else:
        s = json.dumps(obj, ensure_ascii=False, indent=2) + "\n"
    atomic_write_text(path, s)


def http_get_json(url: str, params: Dict[str, Any], timeout: int = 25) -> Any:
    full_url = url + "?" + urlencode(params)
    req = Request(
        full_url,
        headers={
            "User-Agent": "ohlcv-feed/1.0 (GitHub Actions)",
            "Accept": "application/json,text/plain,*/*",
        },
        method="GET",
    )
    with urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw.decode("utf-8"))


def fetch_klines(symbol: str, tf: str, retries_per_endpoint: int = 3) -> List[list]:
    interval = TF_TO_INTERVAL[tf]
    limit = FETCH_LIMIT[tf]
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    last_err: Optional[Exception] = None

    for base in KLINES_ENDPOINTS:
        for attempt in range(1, retries_per_endpoint + 1):
            try:
                data = http_get_json(base, params)
                if not isinstance(data, list) or not data:
                    raise RuntimeError("empty payload")
                return data
            except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError) as e:
                last_err = e
                time.sleep(0.8 * attempt)

    raise RuntimeError(f"Failed to fetch klines {symbol} {interval} limit={limit}: {last_err}")


def simplify_klines(raw: List[list]) -> List[list]:
    # Формат как сейчас в *_last.json: [t_ms,"o","h","l","c","v",close_t_ms]
    out: List[list] = []
    for k in raw:
        out.append([int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]), str(k[5]), int(k[6])])
    return out


def only_closed(bars: List[list], now_ms: int) -> List[list]:
    cutoff = now_ms - SAFETY_MS
    return [b for b in bars if int(b[6]) <= cutoff]


def make_url(file_name: str, v: str) -> str:
    return f"{PAGES_BASE_URL}{file_name}?v={v}"


# ---- main
def main() -> None:
    updated_utc = utc_now_iso(microseconds=True)
    now_ms = int(time.time() * 1000)

    by_symbol_tf: Dict[str, Dict[str, Dict[str, Any]]] = {}
    errors: List[str] = []

    # 1) тянем свечи и пишем файлы (как у тебя сейчас в ohlcv/binance)
    for symbol in SYMBOLS_CORE10:
        by_symbol_tf[symbol] = {}
        for tf in TFS:
            try:
                raw = fetch_klines(symbol, tf)
                bars = only_closed(simplify_klines(raw), now_ms)
                if len(bars) < 50:
                    raise RuntimeError(f"too few closed bars: {len(bars)}")

                n = TAIL_N[tf]
                tail_bars = bars[-n:] if len(bars) > n else bars

                txt_name = f"{symbol}_{tf}.txt"
                last_name = f"{symbol}_{tf}_last.json"
                tail_name = f"{symbol}_{tf}_tail{n}.json"

                # Пишем все три (txt оставляем, но В PACK больше не даём ссылку на txt)
                write_json(OUT_DIR / txt_name, bars, compact=True)
                write_json(OUT_DIR / last_name, bars[-1], compact=True)
                write_json(OUT_DIR / tail_name, tail_bars, compact=True)

                by_symbol_tf[symbol][tf] = {
                    "bars": bars,
                    "tail": tail_bars,
                    "txt_name": txt_name,
                    "last_name": last_name,
                    "tail_name": tail_name,
                }

                time.sleep(0.12)

            except Exception as e:
                msg = f"{symbol} {tf}: {e}"
                errors.append(msg)
                if symbol in CRITICAL:
                    raise
                continue

    # 2) symbols.json (как сейчас)
    symbols_json = {
        "tfs": TFS,
        "updated_utc": updated_utc,
        "symbols": SYMBOLS_CORE10,
        "desired_symbols": SYMBOLS_CORE10,
    }
    write_json(OUT_DIR / "symbols.json", symbols_json, compact=True)

    # 3) status_btc_eth.json (как сейчас; txt остаётся внутри статуса — ок)
    status_symbols: Dict[str, Any] = {}
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        status_symbols[symbol] = {}
        for tf in TFS:
            info = by_symbol_tf[symbol][tf]
            last_bar = info["bars"][-1]
            status_symbols[symbol][tf] = {
                "parse_ok": True,
                "last_open_utc": ms_to_utc_iso(int(last_bar[0])),
                "last_close_utc": ms_to_utc_iso(int(last_bar[6])),
                "files": {"txt": info["txt_name"], "last": info["last_name"], "tail": info["tail_name"]},
            }

    status_btc_eth = {"updated_utc": updated_utc, "base_url": PAGES_BASE_URL, "symbols": status_symbols}
    write_json(OUT_DIR / "status_btc_eth.json", status_btc_eth, compact=True)

    # 4) pack_btc_eth.json (данные одним файлом — удобно)
    pack_btc_eth_json: Dict[str, Any] = {
        "meta": {
            "updated_utc": updated_utc,
            "source": "Binance spot /api/v3/klines",
            "timezone": "UTC",
            "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
        },
        "symbols": {},
    }
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        pack_btc_eth_json["symbols"][symbol] = {}
        for tf in TFS:
            info = by_symbol_tf[symbol][tf]
            pack_btc_eth_json["symbols"][symbol][tf] = {
                "last": info["bars"][-1],
                "tail": info["tail"],
                "tail_n": TAIL_N[tf],
            }

    write_json(OUT_DIR / "pack_btc_eth.json", pack_btc_eth_json, compact=True)

    # 5) core5_latest.json (как витрина, H4/D1/W1)
    core5_latest: Dict[str, Any] = {
        "meta": {"source": "Binance spot /api/v3/klines", "timezone": "UTC", "generated_utc": updated_utc},
        "tfs": ["H4", "D1", "W1"],
        "symbols": {},
    }
    for sym in SYMBOLS_CORE5:
        core5_latest["symbols"][sym] = {}
        for tf in ["H4", "D1", "W1"]:
            info = by_symbol_tf.get(sym, {}).get(tf)
            if not info:
                continue
            last_close_utc = ms_to_utc_iso(int(info["bars"][-1][6]))
            core5_latest["symbols"][sym][tf] = {
                "tf": tf,
                "bars": len(info["tail"]),
                "last_close_utc": last_close_utc,
                "data": info["tail"],
            }
    write_json(OUT_DIR / "core5_latest.json", core5_latest, compact=True)

    # 6) pack_btc_eth.txt — ВАЖНЫЙ ФИКС:
    #    - многострочный
    #    - БЕЗ ссылок на тяжёлые *.txt (оставляем только last+tail)
    v = updated_utc
    lines: List[str] = []
    lines.append(f"# updated_utc: {updated_utc}")
    lines.append("# cache-bust: все ссылки ниже содержат ?v=updated_utc")
    lines.append("# MAIN")
    lines.append(make_url("core5_latest.json", v))
    lines.append(make_url("symbols.json", v))
    lines.append(make_url("status_btc_eth.json", v))
    lines.append(make_url("pack_btc_eth.json", v))
    lines.append(make_url("pack_btc_eth.txt", v))

    for symbol in ["BTCUSDT", "ETHUSDT"]:
        lines.append(f"# {symbol}")
        for tf in TFS:
            n = TAIL_N[tf]
            lines.append(make_url(f"{symbol}_{tf}_last.json", v))
            lines.append(make_url(f"{symbol}_{tf}_tail{n}.json", v))

    atomic_write_text(OUT_DIR / "pack_btc_eth.txt", "\n".join(lines) + "\n")

    if errors:
        print("WARN non-critical errors:")
        for e in errors:
            print(" -", e)

    print("OK updated_utc:", updated_utc)


if __name__ == "__main__":
    main()
