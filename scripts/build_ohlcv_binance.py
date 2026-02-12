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

# Хвост под свинг/структуру
TAIL_N = {"H1": 240, "H4": 1500, "D1": 2000, "W1": 520}

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

def _try_fetch_one(base: str, params: Dict[str, Any], retries: int) -> List[list]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            data = http_get_json(base, params)
            if not isinstance(data, list):
                raise RuntimeError(f"unexpected response type: {type(data)}")
            return data
        except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError) as e:
            last_err = e
            time.sleep(0.7 * attempt)
    raise RuntimeError(f"fetch failed: {last_err}")

def fetch_klines(symbol: str, tf: str, desired: Optional[int] = None, retries: int = 3) -> List[list]:
    """
    Тянем >= desired свечей. Binance лимит 1000/запрос -> пейджинг назад через endTime.
    """
    interval = TF_TO_INTERVAL[tf]
    limit_max = FETCH_LIMIT[tf]
    want = int(desired or max(limit_max, TAIL_N[tf]))
    if want <= 0:
        return []

    seen: Dict[int, list] = {}
    end_time: Optional[int] = None
    loops = 0

    while len(seen) < want and loops < 20:
        remaining = want - len(seen)
        req_limit = min(limit_max, remaining) if end_time is not None else min(limit_max, want)

        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": req_limit}
        if end_time is not None:
            params["endTime"] = end_time

        data: Optional[List[list]] = None
        last_err: Optional[Exception] = None
        for base in BINANCE_ENDPOINTS:
            try:
                data = _try_fetch_one(base, params, retries)
                break
            except Exception as e:
                last_err = e
                data = None

        if data is None:
            raise RuntimeError(f"fetch failed {symbol} {tf}: {last_err}")
        if not data:
            break

        for k in data:
            try:
                seen[int(k[0])] = k
            except Exception:
                pass

        first_open = int(data[0][0])
        new_end = first_open - 1
        if end_time is not None and new_end >= end_time:
            break
        end_time = new_end

        if len(data) < req_limit:
            break

        loops += 1
        time.sleep(0.12)

    out = [seen[k] for k in sorted(seen.keys())]
    if len(out) > want:
        out = out[-want:]
    return out

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

            # чистим старые хвосты, если поменялся tail_n
            for old in ohlcv_dir.glob(f"{sym}_{tf}_tail*.json"):
                try:
                    old.unlink()
                except FileNotFoundError:
                    pass

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
    status_btc_eth = {
        "updated_utc": updated_utc,
        "parse_ok": True,
        "errors": noncritical_errors,
        "symbols": {},
    }
    for sym in ["BTCUSDT", "ETHUSDT"]:
        status_btc_eth["symbols"][sym] = {}
        for tf in TFS:
            n = TAIL_N[tf]
            status_btc_eth["symbols"][sym][tf] = {
                "tf": tf,
                "tail_n": n,
                "last_close_utc": ms_to_iso(int(by_symbol_tf[sym][tf]["tail_bars"][-1][6])),
                "files": {
                    "txt": by_symbol_tf[sym][tf]["txt_name"],
                    "last": by_symbol_tf[sym][tf]["last_name"],
                    "tail": by_symbol_tf[sym][tf]["tail_name"],
                },
                "urls": {
                    "txt": url(by_symbol_tf[sym][tf]["txt_name"], updated_utc),
                    "last": url(by_symbol_tf[sym][tf]["last_name"], updated_utc),
                    "tail": url(by_symbol_tf[sym][tf]["tail_name"], updated_utc),
                },
            }

    # core5_latest.json
    core5_latest = {"updated_utc": updated_utc, "symbols": {}}
    for sym in SYMBOLS_CORE5:
        core5_latest["symbols"][sym] = {}
        for tf in TFS:
            n = TAIL_N[tf]
            core5_latest["symbols"][sym][tf] = {
                "tf": tf,
                "tail_n": n,
                "last_close_utc": ms_to_iso(int(by_symbol_tf[sym][tf]["tail_bars"][-1][6])),
                "last_url": url(by_symbol_tf[sym][tf]["last_name"], updated_utc),
                "tail_url": url(by_symbol_tf[sym][tf]["tail_name"], updated_utc),
            }

    # pack_btc_eth.json
    pack_btc_eth_json = {"updated_utc": updated_utc, "symbols": {}}
    for sym in ["BTCUSDT", "ETHUSDT"]:
        pack_btc_eth_json["symbols"][sym] = {}
        for tf in TFS:
            n = TAIL_N[tf]
            pack_btc_eth_json["symbols"][sym][tf] = {
                "tf": tf,
                "tail_n": n,
                "last_close_utc": ms_to_iso(int(by_symbol_tf[sym][tf]["tail_bars"][-1][6])),
                "last_url": url(by_symbol_tf[sym][tf]["last_name"], updated_utc),
                "tail_url": url(by_symbol_tf[sym][tf]["tail_name"], updated_utc),
            }

    # pack_btc_eth.txt (многострочный)
    pack_lines: List[str] = []
    pack_lines.append(url("status_btc_eth.json", updated_utc))
    for sym in ["BTCUSDT", "ETHUSDT"]:
        for tf in TFS:
            n = TAIL_N[tf]
            pack_lines.append(url(f"{sym}_{tf}_last.json", updated_utc))
            pack_lines.append(url(f"{sym}_{tf}_tail{n}.json", updated_utc))
    pack_lines.append(url("pack_btc_eth.json", updated_utc))
    pack_btc_eth_txt = "\n".join(pack_lines) + "\n"

    # feed.json (минимальный)
    feed_json = {
        "updated_utc": updated_utc,
        "source": "binance",
        "symbols": SYMBOLS_CORE10,
        "tfs": TFS,
    }

    payload = {
        "by_symbol_tf": by_symbol_tf,
        "symbols_json": symbols_json,
        "status_btc_eth": status_btc_eth,
        "core5_latest": core5_latest,
        "pack_btc_eth_json": pack_btc_eth_json,
        "pack_btc_eth_txt": pack_btc_eth_txt,
        "feed_json": feed_json,
        "ohlcv_dir_map": {
            out_roots[0]: out_roots[0] / "ohlcv" / "binance",
            out_roots[1]: out_roots[1] / "ohlcv" / "binance",
        },
        "deriv_dir_map": {
            out_roots[0]: out_roots[0] / "deriv" / "binance",
            out_roots[1]: out_roots[1] / "deriv" / "binance",
        },
        "deriv_stub_core5": {"updated_utc": updated_utc, "note": "stub"},
        "deriv_stub_core10": {"updated_utc": updated_utc, "note": "stub"},
    }

    for root in out_roots:
        build_into(root, payload)


if __name__ == "__main__":
    main()
