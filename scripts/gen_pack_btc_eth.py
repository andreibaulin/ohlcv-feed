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

# ---------------- CONFIG ----------------

SYMBOLS_CORE10: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "AVAXUSDT",
    "LINKUSDT", "AAVEUSDT", "UNIUSDT", "ARBUSDT", "ADAUSDT",
]
SYMBOLS_CORE5: List[str] = SYMBOLS_CORE10[:5]
CRITICAL = {"BTCUSDT", "ETHUSDT"}

TFS: List[str] = ["H1", "H4", "D1", "W1"]
TF_TO_INTERVAL: Dict[str, str] = {"H1": "1h", "H4": "4h", "D1": "1d", "W1": "1w"}

FETCH_LIMIT: Dict[str, int] = {"H1": 1000, "H4": 1000, "D1": 1000, "W1": 1000}
TAIL_N: Dict[str, int] = {"H1": 240, "H4": 240, "D1": 400, "W1": 260}

# если интерфейс/где-то ещё “схлопывает переносы” — всё равно будут читабельны маленькие чанки
CHUNK_SIZE: Dict[str, int] = {"H1": 12, "H4": 10, "D1": 10, "W1": 10}

SAFETY_MS = 60_000  # отсекаем незакрытый бар

# data-api первым (меньше 451)
KLINES_ENDPOINTS = [
    "https://data-api.binance.vision/api/v3/klines",
    "https://api.binance.com/api/v3/klines",
]

# пишем в ./ohlcv и ./docs/ohlcv
OUT_ROOTS = [Path("."), Path("docs")]

PAGES_BASE_URL = "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/"


# ---------------- HELPERS ----------------

def utc_now_iso(microseconds: bool = True) -> str:
    dt = datetime.now(timezone.utc)
    if not microseconds:
        dt = dt.replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def ms_to_utc_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def atomic_write_text(path: Path, text: str) -> None:
    # Явно пишем LF
    atomic_write_bytes(path, text.encode("utf-8"))


def json_compact(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def write_json(path: Path, obj: Any) -> None:
    atomic_write_text(path, json_compact(obj) + "\n")


def write_pack_txt(path: Path, lines: List[str]) -> None:
    # Даже если где-то переносы ломаются — это не критично для работы (ссылки всё равно парсятся),
    # но в норме будет многострочно.
    text = "\n".join(lines).rstrip("\n") + "\n"
    atomic_write_text(path, text)


def write_jsonl(path: Path, rows: List[Any]) -> None:
    # 1 объект = 1 строка (лучший формат для “хвостов”)
    # Если где-то переносы реально уничтожаются — тогда используем chunks (ниже).
    out = []
    for r in rows:
        out.append(json_compact(r))
    atomic_write_text(path, "\n".join(out) + "\n")


def write_tail_chunks(base_dir: Path, symbol: str, tf: str, n: int, rows: List[Any], updated_utc: str) -> Dict[str, Any]:
    """
    Пишем:
      - {symbol}_{tf}_tail{n}_chunks.json  (манифест)
      - {symbol}_{tf}_tail{n}_p000.json ... (маленькие массивы, обычно одна строка и короткая)
    Это работает даже если переносы не сохраняются.
    """
    chunk_size = CHUNK_SIZE[tf]
    parts: List[Dict[str, Any]] = []

    total = len(rows)
    idx = 0
    part_i = 0
    while idx < total:
        part = rows[idx: idx + chunk_size]
        part_name = f"{symbol}_{tf}_tail{n}_p{part_i:03d}.json"
        write_json(base_dir / part_name, part)

        parts.append({
            "file": part_name,
            "count": len(part),
            "from_open_time_ms": int(part[0][0]) if part else None,
            "to_close_time_ms": int(part[-1][6]) if part else None,
            "url": f"{PAGES_BASE_URL}{part_name}?v={updated_utc}",
        })

        idx += chunk_size
        part_i += 1

    manifest_name = f"{symbol}_{tf}_tail{n}_chunks.json"
    manifest = {
        "updated_utc": updated_utc,
        "symbol": symbol,
        "tf": tf,
        "tail_n": n,
        "chunk_size": chunk_size,
        "parts": parts,
    }
    write_json(base_dir / manifest_name, manifest)
    return {
        "chunks_manifest": manifest_name,
        "chunks_manifest_url": f"{PAGES_BASE_URL}{manifest_name}?v={updated_utc}",
        "parts": parts,
    }


def http_get_json(url: str, params: Dict[str, Any], timeout: int = 25) -> Any:
    full_url = url + "?" + urlencode(params)
    req = Request(
        full_url,
        headers={
            "User-Agent": "ohlcv-feed/1.2 (GitHub Actions)",
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
    # [open_time_ms, "o","h","l","c","v", close_time_ms]
    out: List[list] = []
    for k in raw:
        out.append([int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]), str(k[5]), int(k[6])])
    return out


def only_closed(bars: List[list], now_ms: int) -> List[list]:
    cutoff = now_ms - SAFETY_MS
    return [b for b in bars if int(b[6]) <= cutoff]


def make_url(file_name: str, v: str) -> str:
    return f"{PAGES_BASE_URL}{file_name}?v={v}"


# ---------------- MAIN ----------------

def main() -> None:
    updated_utc = utc_now_iso(microseconds=True)
    now_ms = int(time.time() * 1000)

    # данные в памяти
    by_symbol_tf: Dict[str, Dict[str, Dict[str, Any]]] = {}
    errors: List[str] = []

    # 1) тянем свечи
    for symbol in SYMBOLS_CORE10:
        by_symbol_tf[symbol] = {}
        for tf in TFS:
            try:
                raw = fetch_klines(symbol, tf)
                bars = only_closed(simplify_klines(raw), now_ms)
                if len(bars) < 50:
                    raise RuntimeError(f"too few closed bars: {len(bars)}")

                n = TAIL_N[tf]
                tail = bars[-n:] if len(bars) > n else bars

                by_symbol_tf[symbol][tf] = {
                    "bars": bars,
                    "tail": tail,
                    "tail_n": n,
                    "last": bars[-1],
                }

                time.sleep(0.12)

            except Exception as e:
                msg = f"{symbol} {tf}: {e}"
                errors.append(msg)
                if symbol in CRITICAL:
                    raise
                continue

    v = updated_utc

    # 2) symbols.json
    symbols_json = {
        "tfs": TFS,
        "updated_utc": updated_utc,
        "symbols": SYMBOLS_CORE10,
        "desired_symbols": SYMBOLS_CORE10,
    }

    # 3) status_btc_eth.json (ссылки на last + jsonl + chunks)
    status_symbols: Dict[str, Any] = {}
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        status_symbols[symbol] = {}
        for tf in TFS:
            info = by_symbol_tf[symbol][tf]
            last = info["last"]
            n = info["tail_n"]
            txt_ptr = f"{symbol}_{tf}.txt"
            last_name = f"{symbol}_{tf}_last.json"
            tail_jsonl = f"{symbol}_{tf}_tail{n}.jsonl"
            tail_chunks = f"{symbol}_{tf}_tail{n}_chunks.json"

            status_symbols[symbol][tf] = {
                "parse_ok": True,
                "last_open_utc": ms_to_utc_iso(int(last[0])),
                "last_close_utc": ms_to_utc_iso(int(last[6])),
                "files": {
                    "txt": txt_ptr,                 # маленький pointer-файл (не массив)
                    "last": last_name,              # 1 свеча
                    "tail_jsonl": tail_jsonl,       # NDJSON
                    "tail_chunks": tail_chunks,     # манифест маленьких JSON-чанков
                },
            }

    status_btc_eth = {"updated_utc": updated_utc, "base_url": PAGES_BASE_URL, "symbols": status_symbols}

    # 4) pack_btc_eth.json (лёгкий манифест)
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
            n = info["tail_n"]
            last = info["last"]
            pack_btc_eth_json["symbols"][symbol][tf] = {
                "last": last,
                "last_close_utc": ms_to_utc_iso(int(last[6])),
                "last_url": make_url(f"{symbol}_{tf}_last.json", v),
                "tail_jsonl_url": make_url(f"{symbol}_{tf}_tail{n}.jsonl", v),
                "tail_chunks_url": make_url(f"{symbol}_{tf}_tail{n}_chunks.json", v),
                "tail_n": n,
            }

    # 5) core5_latest.json (лёгкий манифест для H4/D1/W1)
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
            n = info["tail_n"]
            last = info["last"]
            core5_latest["symbols"][sym][tf] = {
                "tf": tf,
                "tail_n": n,
                "last_close_utc": ms_to_utc_iso(int(last[6])),
                "last_url": make_url(f"{sym}_{tf}_last.json", v),
                "tail_jsonl_url": make_url(f"{sym}_{tf}_tail{n}.jsonl", v),
                "tail_chunks_url": make_url(f"{sym}_{tf}_tail{n}_chunks.json", v),
            }

    # 6) pack_btc_eth.txt (ссылки)
    lines: List[str] = []
    lines.append(f"# updated_utc: {updated_utc}")
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
            lines.append(make_url(f"{symbol}_{tf}_tail{n}.jsonl", v))
            lines.append(make_url(f"{symbol}_{tf}_tail{n}_chunks.json", v))

    # 7) пишем файлы в оба каталога
    for root in OUT_ROOTS:
        out_dir = root / "ohlcv" / "binance"
        out_dir.mkdir(parents=True, exist_ok=True)

        # per symbol/tf
        for symbol in SYMBOLS_CORE10:
            for tf in TFS:
                info = by_symbol_tf.get(symbol, {}).get(tf)
                if not info:
                    continue

                n = info["tail_n"]
                tail = info["tail"]
                last = info["last"]

                # 7.1 last.json (маленький)
                write_json(out_dir / f"{symbol}_{tf}_last.json", last)

                # 7.2 tail.jsonl (NDJSON)
                write_jsonl(out_dir / f"{symbol}_{tf}_tail{n}.jsonl", tail)

                # 7.3 tail chunks (железный фолбэк)
                chunks_meta = write_tail_chunks(out_dir, symbol, tf, n, tail, updated_utc)

                # 7.4 pointer-файл под старое имя *.txt (маленький, всегда открывается)
                # Можно парсить как JSON: указывает, где хвост и чанки.
                pointer = {
                    "updated_utc": updated_utc,
                    "symbol": symbol,
                    "tf": tf,
                    "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
                    "last_url": make_url(f"{symbol}_{tf}_last.json", v),
                    "tail_jsonl_url": make_url(f"{symbol}_{tf}_tail{n}.jsonl", v),
                    "tail_chunks_url": chunks_meta["chunks_manifest_url"],
                    "tail_n": n,
                }
                write_json(out_dir / f"{symbol}_{tf}.txt", pointer)

        # manifests
        write_json(out_dir / "symbols.json", symbols_json)
        write_json(out_dir / "status_btc_eth.json", status_btc_eth)
        write_json(out_dir / "pack_btc_eth.json", pack_btc_eth_json)
        write_json(out_dir / "core5_latest.json", core5_latest)
        write_pack_txt(out_dir / "pack_btc_eth.txt", lines)

    if errors:
        print("WARN non-critical errors:")
        for e in errors:
            print(" -", e)

    print("OK updated_utc:", updated_utc)


if __name__ == "__main__":
    main()
