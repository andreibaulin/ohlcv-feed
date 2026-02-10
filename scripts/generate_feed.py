#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GitHub Pages OHLCV feed generator (Binance spot klines).

Ключевая цель для стабильного "дёрганья свечей" (в т.ч. из ChatGPT):
- НЕ полагаться на гигантские *_TF.txt (они часто в одну строку и тяжёлые)
- Делать маленькие файлы:
  *_TF_last.json
  *_TF_tail{N}.json   <-- ВАЖНО: JSON (НЕ jsonl), чтобы нормально тянулось
- Делать status_btc_eth.json + pack_btc_eth.txt с cache-bust ?v=updated_utc
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# ---- Config

SYMBOLS_CORE10: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "AVAXUSDT",
    "LINKUSDT", "AAVEUSDT", "UNIUSDT", "ARBUSDT", "ADAUSDT",
]
SYMBOLS_CORE5: List[str] = SYMBOLS_CORE10[:5]
CRITICAL_SYMBOLS = {"BTCUSDT", "ETHUSDT"}

TFS: List[str] = ["H1", "H4", "D1", "W1"]
TF_TO_BINANCE_INTERVAL: Dict[str, str] = {
    "H1": "1h",
    "H4": "4h",
    "D1": "1d",
    "W1": "1w",
}

# Сколько баров просим у Binance (max=1000).
# Можно держать умеренно, чтобы репа не пухла.
FETCH_LIMIT: Dict[str, int] = {
    "H1": 900,
    "H4": 600,
    "D1": 800,
    "W1": 520,
}

# Хвост (маленький JSON), который реально используется для анализа
TAIL_N: Dict[str, int] = {
    "H1": 240,
    "H4": 240,
    "D1": 400,
    "W1": 260,
}

# Фильтр "только закрытые свечи": бар считается закрытым, если close_time <= now - SAFETY_MS
SAFETY_MS = 60_000

BINANCE_KLINES_ENDPOINTS = [
    "https://api.binance.com/api/v3/klines",
    # запасной домен (часто помогает при региональных/сетевых глюках)
    "https://data-api.binance.vision/api/v3/klines",
]

# База GitHub Pages (абсолютная), как сейчас в твоём status/pack
PAGES_BASE_URL = "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/"


# ---- Helpers

def utc_now_iso(microseconds: bool = True) -> str:
    dt = datetime.now(timezone.utc)
    if not microseconds:
        dt = dt.replace(microsecond=0)
    s = dt.isoformat()
    return s.replace("+00:00", "Z")


def ms_to_utc_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def write_json(path: Path, obj: Any, compact: bool = True) -> None:
    if compact:
        payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    else:
        payload = json.dumps(obj, ensure_ascii=False, indent=2) + "\n"
    atomic_write_bytes(path, payload.encode("utf-8"))


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
    interval = TF_TO_BINANCE_INTERVAL[tf]
    limit = FETCH_LIMIT[tf]

    last_err: Optional[Exception] = None
    for base in BINANCE_KLINES_ENDPOINTS:
        for attempt in range(1, retries_per_endpoint + 1):
            try:
                data = http_get_json(base, {"symbol": symbol, "interval": interval, "limit": limit})
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected response type: {type(data)}")
                return data
            except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError) as e:
                last_err = e
                time.sleep(0.8 * attempt)

    raise RuntimeError(f"Failed to fetch klines {symbol} {tf}: {last_err}")


def simplify_klines(raw: List[list]) -> List[list]:
    """
    Выходной формат (как у тебя в *_last.json):
    [open_time_ms,"open","high","low","close","volume","close_time_ms"]
    """
    out: List[list] = []
    for k in raw:
        # Binance kline: [0] open_time, [1] open, [2] high, [3] low, [4] close, [5] volume, [6] close_time, ...
        out.append([int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]), str(k[5]), int(k[6])])
    return out


def only_closed(bars: List[list], now_ms: int) -> List[list]:
    cutoff = now_ms - SAFETY_MS
    return [b for b in bars if int(b[6]) <= cutoff]


def make_url(file_name: str, v: str) -> str:
    return f"{PAGES_BASE_URL}{file_name}?v={v}"


@dataclass
class TFInfo:
    parse_ok: bool
    last_open_utc: str
    last_close_utc: str
    txt: str
    last: str
    tail: str


# ---- Main

def main() -> None:
    out_root = Path(os.getenv("OHLCV_OUT_DIR", "docs")).resolve()
    ohlcv_dir = out_root / "ohlcv" / "binance"
    deriv_dir = out_root / "deriv" / "binance"

    updated_utc = utc_now_iso(microseconds=True)
    now_ms = int(time.time() * 1000)

    # Собираем данные
    by_symbol_tf: Dict[str, Dict[str, Dict[str, Any]]] = {}
    errors: List[str] = []

    for symbol in SYMBOLS_CORE10:
        by_symbol_tf[symbol] = {}
        for tf in TFS:
            try:
                raw = fetch_klines(symbol, tf)
                bars = only_closed(simplify_klines(raw), now_ms)
                if len(bars) < 20:
                    raise RuntimeError(f"too few closed bars: {len(bars)}")

                # файлы
                txt_name = f"{symbol}_{tf}.txt"
                last_name = f"{symbol}_{tf}_last.json"
                tail_n = TAIL_N[tf]
                tail_name = f"{symbol}_{tf}_tail{tail_n}.json"  # <-- ВАЖНО: .json (НЕ .jsonl)

                # пишем
                write_json(ohlcv_dir / txt_name, bars, compact=True)  # большой файл, но пусть будет
                write_json(ohlcv_dir / last_name, bars[-1], compact=True)

                tail_bars = bars[-tail_n:] if len(bars) > tail_n else bars
                write_json(ohlcv_dir / tail_name, tail_bars, compact=True)

                by_symbol_tf[symbol][tf] = {
                    "bars": bars,
                    "txt_name": txt_name,
                    "last_name": last_name,
                    "tail_name": tail_name,
                }

                # чуть бережнее к API
                time.sleep(0.15)

            except Exception as e:
                msg = f"{symbol} {tf}: {e}"
                errors.append(msg)
                # критичные (BTC/ETH) — валим джобу, чтобы не коммитить мусор
                if symbol in CRITICAL_SYMBOLS:
                    raise
                # остальные — пропускаем, но продолжаем
                continue

    # symbols.json
    symbols_json = {
        "tfs": TFS,
        "updated_utc": updated_utc,
        "symbols": [s for s in SYMBOLS_CORE10 if s in by_symbol_tf],
        "desired_symbols": SYMBOLS_CORE10,
    }
    write_json(ohlcv_dir / "symbols.json", symbols_json, compact=True)

    # status_btc_eth.json (только BTC/ETH, как у тебя сейчас)
    status_symbols: Dict[str, Dict[str, Any]] = {}
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        sym_block: Dict[str, Any] = {}
        for tf in TFS:
            info = by_symbol_tf.get(symbol, {}).get(tf)
            if not info:
                sym_block[tf] = {"parse_ok": False}
                continue
            last_bar = info["bars"][-1]
            tf_info = TFInfo(
                parse_ok=True,
                last_open_utc=ms_to_utc_iso(int(last_bar[0])),
                last_close_utc=ms_to_utc_iso(int(last_bar[6])),
                txt=info["txt_name"],
                last=info["last_name"],
                tail=info["tail_name"],
            )
            sym_block[tf] = {
                "parse_ok": tf_info.parse_ok,
                "last_open_utc": tf_info.last_open_utc,
                "last_close_utc": tf_info.last_close_utc,
                "files": {"txt": tf_info.txt, "last": tf_info.last, "tail": tf_info.tail},
            }
        status_symbols[symbol] = sym_block

    status = {
        "updated_utc": updated_utc,
        "base_url": PAGES_BASE_URL,
        "symbols": status_symbols,
    }
    write_json(ohlcv_dir / "status_btc_eth.json", status, compact=True)

    # pack_btc_eth.json — один файл, чтобы можно было забирать всё одним запросом
    pack_json: Dict[str, Any] = {
        "updated_utc": updated_utc,
        "base_url": PAGES_BASE_URL,
        "symbols": {},
    }
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        pack_json["symbols"][symbol] = {}
        for tf in TFS:
            info = by_symbol_tf.get(symbol, {}).get(tf)
            if not info:
                continue
            tail_n = TAIL_N[tf]
            tail_bars = info["bars"][-tail_n:] if len(info["bars"]) > tail_n else info["bars"]
            pack_json["symbols"][symbol][tf] = {
                "last": info["bars"][-1],
                "tail": tail_bars,
            }
    write_json(ohlcv_dir / "pack_btc_eth.json", pack_json, compact=True)

    # pack_btc_eth.txt (многострочный, чтобы не был одной гигантской строкой)
    v = updated_utc
    lines: List[str] = []
    lines.append(f"# updated_utc: {updated_utc}")
    lines.append("# cache-bust: все ссылки ниже содержат ?v=updated_utc")
    lines.append("# MAIN")
    lines.append(make_url("core5_latest.json", v))
    lines.append(make_url("symbols.json", v))
    lines.append(make_url("status_btc_eth.json", v))
    lines.append(make_url("pack_btc_eth.txt", v))
    lines.append(make_url("pack_btc_eth.json", v))

    def add_symbol_block(sym: str) -> None:
        lines.append(f"# {sym}")
        for tf in TFS:
            txt = f"{sym}_{tf}.txt"
            last = f"{sym}_{tf}_last.json"
            tail = f"{sym}_{tf}_tail{TAIL_N[tf]}.json"
            lines.append(make_url(txt, v))
            lines.append(make_url(last, v))
            lines.append(make_url(tail, v))

    add_symbol_block("BTCUSDT")
    add_symbol_block("ETHUSDT")

    atomic_write_bytes(ohlcv_dir / "pack_btc_eth.txt", ("\n".join(lines) + "\n").encode("utf-8"))

    # core5_latest.json (простая компактная витрина последних хвостов для core5, H4/D1/W1)
    core5: Dict[str, Any] = {
        "meta": {
            "source": "Binance spot /api/v3/klines",
            "timezone": "UTC",
            "generated_utc": updated_utc,
            "note": "Use *_tailN.json for fast pulls; this file is a convenience bundle.",
        },
        "tfs": ["H4", "D1", "W1"],
        "symbols": {},
    }
    for sym in SYMBOLS_CORE5:
        core5["symbols"][sym] = {}
        for tf in ["H4", "D1", "W1"]:
            info = by_symbol_tf.get(sym, {}).get(tf)
            if not info:
                continue
            n = TAIL_N[tf]
            tail_bars = info["bars"][-n:] if len(info["bars"]) > n else info["bars"]
            last_close_utc = ms_to_utc_iso(int(info["bars"][-1][6]))
            core5["symbols"][sym][tf] = {
                "tf": tf,
                "bars": len(tail_bars),
                "last_close_utc": last_close_utc,
                "data": tail_bars,
            }
    write_json(ohlcv_dir / "core5_latest.json", core5, compact=True)

    # deriv placeholders (как "заглушка", чтобы не ломать структуру папок)
    deriv_dir.mkdir(parents=True, exist_ok=True)
    for name, syms in [("core5_latest.json", SYMBOLS_CORE5), ("core10_latest.json", SYMBOLS_CORE10)]:
        deriv_stub = {
            "meta": {
                "generated_utc": updated_utc,
                "timezone": "UTC",
                "feed": "deriv/binance",
                "disabled": {"binance_spot": True, "binance_fut": True},
            },
            "data": {s: {"source": "none"} for s in syms},
        }
        write_json(deriv_dir / name, deriv_stub, compact=True)

    # optional: простенький feed.json в корне docs (не критично)
    feed = {
        "updated_utc": updated_utc,
        "ohlcv": {
            "binance": {
                "base_url": PAGES_BASE_URL,
                "symbols": make_url("symbols.json", v),
                "pack_btc_eth_txt": make_url("pack_btc_eth.txt", v),
                "pack_btc_eth_json": make_url("pack_btc_eth.json", v),
            }
        },
    }
    write_json(out_root / "feed.json", feed, compact=True)

    if errors:
        print("WARN: some symbols/tfs failed (non-critical):")
        for e in errors:
            print(" -", e)

    print("OK:", updated_utc)


if __name__ == "__main__":
    main()
