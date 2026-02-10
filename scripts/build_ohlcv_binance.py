#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Цель: максимально надёжные "свечи для прогнозов" на GitHub Pages.
Ключевой фикс: tail-файлы делаем .json (НЕ .jsonl), потому что .jsonl у клиентов часто ломается.

Выход:
- ohlcv/binance/{SYMBOL}_{TF}.txt              (полная выборка, JSON массив)
- ohlcv/binance/{SYMBOL}_{TF}_last.json        (последняя закрытая свеча)
- ohlcv/binance/{SYMBOL}_{TF}_tail{N}.json     (хвост N закрытых свечей)  <-- основной источник для анализа
- ohlcv/binance/status_btc_eth.json
- ohlcv/binance/symbols.json
- ohlcv/binance/core5_latest.json
- ohlcv/binance/pack_btc_eth.txt (многострочный)
- ohlcv/binance/pack_btc_eth.json (BTC/ETH хвосты+last одним файлом)

Пишем ДВА набора: в корень (ohlcv/...) и в docs/ohlcv/... (на случай, если Pages настроен на /docs).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# ---------------- CONFIG ----------------

SYMBOLS_CORE10 = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","AVAXUSDT","LINKUSDT","AAVEUSDT","UNIUSDT","ARBUSDT","ADAUSDT"]
SYMBOLS_CORE5  = SYMBOLS_CORE10[:5]
CRITICAL = {"BTCUSDT", "ETHUSDT"}

TFS = ["H1", "H4", "D1", "W1"]
TF_TO_INTERVAL = {"H1":"1h","H4":"4h","D1":"1d","W1":"1w"}

# Binance max limit = 1000
FETCH_LIMIT = {"H1": 1000, "H4": 1000, "D1": 1000, "W1": 1000}

# Хвост для анализа (быстро/стабильно)
TAIL_N = {"H1": 240, "H4": 240, "D1": 400, "W1": 260}

# фильтруем незакрытый бар (страховка)
SAFETY_MS = 60_000

BINANCE_ENDPOINTS = [
    "https://api.binance.com/api/v3/klines",
    "https://data-api.binance.vision/api/v3/klines",
]

PAGES_BASE_URL = "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/"


# ---------------- HELPERS ----------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def ms_to_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms/1000, tz=timezone.utc).replace(microsecond=0)
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
    full = url + "?" + urlencode(params)
    req = Request(full, headers={"User-Agent": "ohlcv-feed/1.0 (GitHub Actions)", "Accept": "application/json"})
    with urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw.decode("utf-8"))

def fetch_klines(symbol: str, tf: str, retries: int = 3) -> List[list]:
    interval = TF_TO_INTERVAL[tf]
    limit = FETCH_LIMIT[tf]
    last_err: Optional[Exception] = None

    for base in BINANCE_ENDPOINTS:
        for attempt in range(1, retries + 1):
            try:
                data = http_get_json(base, {"symbol": symbol, "interval": interval, "limit": limit})
                if not isinstance(data, list):
                    raise RuntimeError(f"unexpected response type: {type(data)}")
                return data
            except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError) as e:
                last_err = e
                time.sleep(0.7 * attempt)

    raise RuntimeError(f"fetch failed {symbol} {tf}: {last_err}")

def simplify(raw: List[list]) -> List[list]:
    # [open_time, open, high, low, close, volume, close_time]
    out: List[list] = []
    for k in raw:
        out.append([int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]), str(k[5]), int(k[6])])
    return out

def only_closed(bars: List[list], now_ms: int) -> List[list]:
    cutoff = now_ms - SAFETY_MS
    return [b for b in bars if int(b[6]) <= cutoff]

def url(file_name: str, v: str) -> str:
    return f"{PAGES_BASE_URL}{file_name}?v={v}"


@dataclass
class TFFiles:
    txt: str
    last: str
    tail: str


# ---------------- MAIN ----------------

def build_into(root: Path, payload: Dict[str, Any]) -> None:
    """Записать все файлы в root/ohlcv/binance и root/deriv/binance (заглушки)."""
    ohlcv_dir: Path = payload["ohlcv_dir_map"][root]
    deriv_dir: Path = payload["deriv_dir_map"][root]
    ohlcv_dir.mkdir(parents=True, exist_ok=True)
    deriv_dir.mkdir(parents=True, exist_ok=True)

    # Пер-символ/тф файлы
    for sym, tfs in payload["by_symbol_tf"].items():
        for tf, info in tfs.items():
            bars = info["bars"]
            write_json(ohlcv_dir / info["txt_name"], bars, compact=True)
            write_json(ohlcv_dir / info["last_name"], bars[-1], compact=True)
            write_json(ohlcv_dir / info["tail_name"], info["tail_bars"], compact=True)

    # symbols.json
    write_json(ohlcv_dir / "symbols.json", payload["symbols_json"], compact=True)

    # status_btc_eth.json
    write_json(ohlcv_dir / "status_btc_eth.json", payload["status_btc_eth"], compact=True)

    # core5_latest.json
    write_json(ohlcv_dir / "core5_latest.json", payload["core5_latest"], compact=True)

    # pack_btc_eth.json + pack_btc_eth.txt
    write_json(ohlcv_dir / "pack_btc_eth.json", payload["pack_btc_eth_json"], compact=True)
    atomic_write_text(ohlcv_dir / "pack_btc_eth.txt", payload["pack_btc_eth_txt"])

    # feed.json (в корне репо/доков)
    write_json(root / "feed.json", payload["feed_json"], compact=True)

    # deriv stubs (чтобы структура не ломалась)
    write_json(deriv_dir / "core5_latest.json", payload["deriv_stub_core5"], compact=True)
    write_json(deriv_dir / "core10_latest.json", payload["deriv_stub_core10"], compact=True)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    out_roots = [
        repo_root,           # Pages from root
        repo_root / "docs",  # Pages from /docs (если так настроено)
    ]

    updated_utc = utc_now_iso()
    now_ms = int(time.time() * 1000)

    by_symbol_tf: Dict[str, Dict[str, Dict[str, Any]]] = {}
    noncritical_errors: List[str] = []

    for sym in SYMBOLS_CORE10:
        by_symbol_tf[sym] = {}
        for tf in TFS:
            try:
                raw = fetch_klines(sym, tf)
                bars = only_closed(simplify(raw), now_ms)
                if len(bars) < 50:
                    raise RuntimeError(f"too few closed bars: {len(bars)}")

                n = TAIL_N[tf]
                tail_bars = bars[-n:] if len(bars) > n else bars

                txt_name  = f"{sym}_{tf}.txt"
                last_name = f"{sym}_{tf}_last.json"
                tail_name = f"{sym}_{tf}_tail{n}.json"   # <-- КЛЮЧЕВО: .json, не .jsonl

                by_symbol_tf[sym][tf] = {
                    "bars": bars,
                    "tail_bars": tail_bars,
                    "txt_name": txt_name,
                    "last_name": last_name,
                    "tail_name": tail_name,
                }

                time.sleep(0.12)

            except Exception as e:
                msg = f"{sym} {tf}: {e}"
                if sym in CRITICAL:
                    raise
                noncritical_errors.append(msg)

    # symbols.json
    symbols_json = {
        "tfs": TFS,
        "updated_utc": updated_utc,
        "symbols": SYMBOLS_CORE10,
        "desired_symbols": SYMBOLS_CORE10,
    }

    # status_btc_eth.json
    status_symbols: Dict[str, Any] = {}
    for sym in ["BTCUSDT", "ETHUSDT"]:
        status_symbols[sym] = {}
        for tf in TFS:
            info = by_symbol_tf[sym][tf]
            last_bar = info["bars"][-1]
            status_symbols[sym][tf] = {
                "parse_ok": True,
                "last_open_utc": ms_to_iso(int(last_bar[0])),
                "last_close_utc": ms_to_iso(int(last_bar[6])),
                "files": {
                    "txt": info["txt_name"],
                    "last": info["last_name"],
                    "tail": info["tail_name"],
                },
            }

    status_btc_eth = {
        "updated_utc": updated_utc,
        "base_url": PAGES_BASE_URL,
        "symbols": status_symbols,
    }

    # pack_btc_eth.json (всё нужное одним запросом)
    pack_btc_eth_json: Dict[str, Any] = {
        "meta": {
            "updated_utc": updated_utc,
            "source": "Binance spot /api/v3/klines",
            "timezone": "UTC",
            "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
        },
        "symbols": {},
    }
    for sym in ["BTCUSDT", "ETHUSDT"]:
        pack_btc_eth_json["symbols"][sym] = {}
        for tf in TFS:
            info = by_symbol_tf[sym][tf]
            pack_btc_eth_json["symbols"][sym][tf] = {
                "last": info["bars"][-1],
                "tail": info["tail_bars"],
                "tail_n": TAIL_N[tf],
            }

    # pack_btc_eth.txt (многострочный + cache-bust)
    v = updated_utc
    lines: List[str] = []
    lines.append(f"# updated_utc: {updated_utc}")
    lines.append("# cache-bust: все ссылки ниже содержат ?v=updated_utc")
    lines.append("# MAIN")
    lines.append(url("core5_latest.json", v))
    lines.append(url("symbols.json", v))
    lines.append(url("status_btc_eth.json", v))
    lines.append(url("pack_btc_eth.json", v))
    lines.append(url("pack_btc_eth.txt", v))
    for sym in ["BTCUSDT", "ETHUSDT"]:
        lines.append(f"# {sym}")
        for tf in TFS:
            n = TAIL_N[tf]
            lines.append(url(f"{sym}_{tf}.txt", v))
            lines.append(url(f"{sym}_{tf}_last.json", v))
            lines.append(url(f"{sym}_{tf}_tail{n}.json", v))
    pack_btc_eth_txt = "\n".join(lines) + "\n"

    # core5_latest.json (удобный пакет, как у тебя уже есть)
    core5_latest: Dict[str, Any] = {
        "meta": {
            "source": "Binance spot /api/v3/klines",
            "timezone": "UTC",
            "generated_utc": updated_utc,
        },
        "tfs": ["H4", "D1", "W1"],
        "symbols": {},
    }
    for sym in SYMBOLS_CORE5:
        core5_latest["symbols"][sym] = {}
        for tf in ["H4", "D1", "W1"]:
            info = by_symbol_tf[sym][tf]
            last_close_utc = ms_to_iso(int(info["bars"][-1][6]))
            core5_latest["symbols"][sym][tf] = {
                "tf": tf,
                "bars": len(info["tail_bars"]),
                "last_close_utc": last_close_utc,
                "data": info["tail_bars"],
            }

    # deriv stubs
    deriv_stub_core5 = {"meta":{"generated_utc":updated_utc,"timezone":"UTC"},"data":{s:{"source":"none"} for s in SYMBOLS_CORE5}}
    deriv_stub_core10 = {"meta":{"generated_utc":updated_utc,"timezone":"UTC"},"data":{s:{"source":"none"} for s in SYMBOLS_CORE10}}

    # feed.json
    feed_json = {
        "updated_utc": updated_utc,
        "ohlcv": {
            "binance": {
                "base_url": PAGES_BASE_URL,
                "pack_btc_eth_txt": url("pack_btc_eth.txt", v),
                "pack_btc_eth_json": url("pack_btc_eth.json", v),
                "symbols": url("symbols.json", v),
                "status_btc_eth": url("status_btc_eth.json", v),
            }
        },
    }

    payload = {
        "by_symbol_tf": by_symbol_tf,
        "symbols_json": symbols_json,
        "status_btc_eth": status_btc_eth,
        "core5_latest": core5_latest,
        "pack_btc_eth_json": pack_btc_eth_json,
        "pack_btc_eth_txt": pack_btc_eth_txt,
        "feed_json": feed_json,
        "deriv_stub_core5": deriv_stub_core5,
        "deriv_stub_core10": deriv_stub_core10,
        "ohlcv_dir_map": {},
        "deriv_dir_map": {},
    }

    # карты директорий для каждого root
    for root in out_roots:
        payload["ohlcv_dir_map"][root] = root / "ohlcv" / "binance"
        payload["deriv_dir_map"][root] = root / "deriv" / "binance"

    # пишем в оба места
    for root in out_roots:
        build_into(root, payload)

    if noncritical_errors:
        print("WARN non-critical fetch errors:")
        for e in noncritical_errors:
            print(" -", e)

    print("OK updated_utc:", updated_utc)


if __name__ == "__main__":
    main()
