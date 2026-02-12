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
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "AAVEUSDT",
    "UNIUSDT",
    "ARBUSDT",
    "ADAUSDT",
]
SYMBOLS_CORE5: List[str] = SYMBOLS_CORE10[:5]
CRITICAL = {"BTCUSDT", "ETHUSDT"}

TFS: List[str] = ["H1", "H4", "D1", "W1"]
TF_TO_INTERVAL: Dict[str, str] = {"H1": "1h", "H4": "4h", "D1": "1d", "W1": "1w"}

# Binance max limit = 1000
FETCH_LIMIT: Dict[str, int] = {"H1": 1000, "H4": 1000, "D1": 1000, "W1": 1000}

# хвост (закрытые бары) — под свинг/структуру
TAIL_N: Dict[str, int] = {"H1": 240, "H4": 1500, "D1": 2000, "W1": 520}

# Чанки крупнее -> меньше файлов p###.
CHUNK_SIZE: Dict[str, int] = {"H1": 60, "H4": 250, "D1": 250, "W1": 260}

# отсекаем незакрытый бар
SAFETY_MS = 60_000

# data-api первым (часто стабильнее по 451)
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


def write_json(path: Path, obj: Any) -> None:
    # компактно (в одну строку) чтобы не раздувать репо
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    atomic_write_bytes(path, s.encode("utf-8"))


def http_get_json(base_url: str, params: Dict[str, Any], timeout: int = 25) -> Any:
    full = base_url + "?" + urlencode(params)
    req = Request(
        full,
        headers={
            "User-Agent": "ohlcv-feed/1.0 (GitHub Actions)",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw.decode("utf-8"))


def cleanup_old_tail_artifacts(base_dir: Path, symbol: str, tf: str) -> None:
    """
    При смене tail_n или chunk_size старые файлы нужно удалять, иначе они остаются в репо.
    Чистим всё по шаблонам для конкретного symbol+tf:
    - {symbol}_{tf}_tail*_p###.json
    - {symbol}_{tf}_tail*_chunks.json
    - {symbol}_{tf}_tail*.jsonl
    """
    patterns = [
        f"{symbol}_{tf}_tail*_p*.json",
        f"{symbol}_{tf}_tail*_chunks.json",
        f"{symbol}_{tf}_tail*.jsonl",
    ]
    for pat in patterns:
        for p in base_dir.glob(pat):
            try:
                p.unlink()
            except FileNotFoundError:
                pass


def write_tail_chunks(

    base_dir: Path,
    symbol: str,
    tf: str,
    n: int,
    rows: List[Any],
    updated_utc: str,
) -> Dict[str, Any]:
    """
    Пишем:
    - {symbol}_{tf}_tail{n}_chunks.json (манифест)
    - {symbol}_{tf}_tail{n}_p000.json ... (чанки)
    """
    chunk_size = CHUNK_SIZE[tf]

    # ЧИСТИМ старые p-файлы (важно при смене chunk_size)
    cleanup_old_tail_artifacts(base_dir, symbol, tf)

    parts: List[Dict[str, Any]] = []
    total = len(rows)
    idx = 0
    part_i = 0

    while idx < total:
        part = rows[idx : idx + chunk_size]
        part_name = f"{symbol}_{tf}_tail{n}_p{part_i:03d}.json"
        write_json(base_dir / part_name, part)

        parts.append(
            {
                "file": part_name,
                "count": len(part),
                "from_open_time_ms": int(part[0][0]) if part else None,
                "to_close_time_ms": int(part[-1][6]) if part else None,
                "url": f"{PAGES_BASE_URL}{part_name}?v={updated_utc}",
            }
        )

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
        "total": total,
        "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
    }
    write_json(base_dir / manifest_name, manifest)

    return {
        "chunks_manifest": manifest_name,
        "chunks_manifest_url": f"{PAGES_BASE_URL}{manifest_name}?v={updated_utc}",
        "parts_count": len(parts),
        "total": total,
    }


def _try_fetch(base: str, params: Dict[str, Any], retries_per_endpoint: int) -> List[list]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries_per_endpoint + 1):
        try:
            data = http_get_json(base, params)
            if not isinstance(data, list):
                raise RuntimeError(f"unexpected response type: {type(data)}")
            if not data:
                return []
            return data
        except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError) as e:
            last_err = e
            time.sleep(0.8 * attempt)
    raise RuntimeError(f"fetch failed: {last_err}")


def fetch_klines(symbol: str, tf: str, desired: Optional[int] = None, retries_per_endpoint: int = 3) -> List[list]:
    """
    Тянем >= desired свечей (закрытые фильтруются позже).
    Binance отдаёт максимум 1000 за запрос, поэтому при desired>1000 пейджим назад через endTime.
    """
    interval = TF_TO_INTERVAL[tf]
    limit_max = FETCH_LIMIT[tf]
    want = int(desired or max(limit_max, TAIL_N[tf]))
    if want <= 0:
        return []

    seen: Dict[int, list] = {}
    end_time: Optional[int] = None
    safety_loops = 0

    while len(seen) < want and safety_loops < 20:
        remaining = want - len(seen)
        req_limit = min(limit_max, remaining) if end_time is not None else min(limit_max, want)

        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": req_limit}
        if end_time is not None:
            params["endTime"] = end_time

        data: Optional[List[list]] = None
        last_err: Optional[Exception] = None

        for base in KLINES_ENDPOINTS:
            try:
                data = _try_fetch(base, params, retries_per_endpoint)
                break
            except Exception as e:
                last_err = e
                data = None

        if data is None:
            raise RuntimeError(f"Failed to fetch klines {symbol} {interval}: {last_err}")
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

        safety_loops += 1
        time.sleep(0.15)

    out = [seen[k] for k in sorted(seen.keys())]
    if len(out) > want:
        out = out[-want:]
    return out


def simplify_klines(raw: List[list]) -> List[list]:
    # [open_time_ms, "o","h","l","c","v", close_time_ms]
    out: List[list] = []
    for k in raw:
        out.append(
            [int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]), str(k[5]), int(k[6])]
        )
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

                last_name = f"{symbol}_{tf}_last.json"
                chunks_meta = {}
                for root in OUT_ROOTS:
                    out_dir = (root / "ohlcv" / "binance")
                    out_dir.mkdir(parents=True, exist_ok=True)
                    # last
                    write_json(out_dir / last_name, tail[-1] if tail else bars[-1])
                    # tail chunks
                    chunks_meta = write_tail_chunks(out_dir, symbol, tf, n, tail, updated_utc)

                by_symbol_tf[symbol][tf] = {
                    "symbol": symbol,
                    "tf": tf,
                    "updated_utc": updated_utc,
                    "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
                    "last": last_name,
                    "last_url": make_url(last_name, updated_utc),
                    "tail_n": n,
                    "tail_chunks": chunks_meta.get("chunks_manifest"),
                    "tail_chunks_url": chunks_meta.get("chunks_manifest_url"),
                    "parts_count": chunks_meta.get("parts_count"),
                    "total": chunks_meta.get("total"),
                    "last_close_utc": ms_to_utc_iso(int((tail[-1] if tail else bars[-1])[6])),
                }

                time.sleep(0.12)

            except Exception as e:
                msg = f"{symbol} {tf}: {e}"
                if symbol in CRITICAL:
                    raise
                errors.append(msg)

    # 2) symbols.json
    symbols_json = {
        "updated_utc": updated_utc,
        "tfs": TFS,
        "symbols": SYMBOLS_CORE10,
        "desired_symbols": SYMBOLS_CORE10,
    }

    # 3) status_btc_eth.json
    status_btc_eth = {
        "updated_utc": updated_utc,
        "parse_ok": True,
        "errors": errors,
        "symbols": {},
    }
    for sym in ["BTCUSDT", "ETHUSDT"]:
        status_btc_eth["symbols"][sym] = {}
        for tf in TFS:
            meta = by_symbol_tf[sym][tf]
            n = meta["tail_n"]
            tail_chunks = f"{sym}_{tf}_tail{n}_chunks.json"
            status_btc_eth["symbols"][sym][tf] = {
                "tf": tf,
                "tail_n": n,
                "last": f"{sym}_{tf}_last.json",
                "tail_chunks": tail_chunks,
                "urls": {
                    "last": meta["last_url"],
                    "tail_chunks": meta["tail_chunks_url"],
                },
            }

    # 4) core5_latest.json (пointers для core5)
    core5_latest = {
        "updated_utc": updated_utc,
        "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
        "symbols": {},
    }
    for sym in SYMBOLS_CORE5:
        core5_latest["symbols"][sym] = {}
        for tf in TFS:
            meta = by_symbol_tf[sym][tf]
            core5_latest["symbols"][sym][tf] = {
                "tf": tf,
                "tail_n": meta["tail_n"],
                "last_close_utc": meta["last_close_utc"],
                "last_url": meta["last_url"],
                "tail_chunks_url": meta["tail_chunks_url"],
            }

    # 5) pack_btc_eth.json (BTC/ETH одним файлом: только мета+ссылки)
    pack_btc_eth_json = {
        "updated_utc": updated_utc,
        "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
        "symbols": {},
    }
    for sym in ["BTCUSDT", "ETHUSDT"]:
        pack_btc_eth_json["symbols"][sym] = {}
        for tf in TFS:
            meta = by_symbol_tf[sym][tf]
            pack_btc_eth_json["symbols"][sym][tf] = {
                "tf": tf,
                "tail_n": meta["tail_n"],
                "last_close_utc": meta["last_close_utc"],
                "last_url": meta["last_url"],
                "tail_chunks_url": meta["tail_chunks_url"],
            }

    # 6) pack_btc_eth.txt (ссылки, одна строка = один URL)
    lines: List[str] = []
    v = updated_utc
    # status сначала
    lines.append(make_url("status_btc_eth.json", v))
    # BTC/ETH: H4,H1,M15 хвосты (под твой протокол можно адаптировать, но тут оставляем H1/H4/D1/W1)
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        for tf in TFS:
            n = TAIL_N[tf]
            # last
            lines.append(make_url(f"{symbol}_{tf}_last.json", v))
            # tail chunks
            lines.append(make_url(f"{symbol}_{tf}_tail{n}_chunks.json", v))
    # отдельный json-pack
    lines.append(make_url("pack_btc_eth.json", v))
    pack_btc_eth_txt = "\n".join(lines) + "\n"

    # 7) пишем файлы в ./ohlcv/binance и ./docs/ohlcv/binance
    for root in OUT_ROOTS:
        out_dir = root / "ohlcv" / "binance"
        out_dir.mkdir(parents=True, exist_ok=True)

        # symbols.json
        write_json(out_dir / "symbols.json", symbols_json)

        # status
        write_json(out_dir / "status_btc_eth.json", status_btc_eth)

        # core5_latest
        write_json(out_dir / "core5_latest.json", core5_latest)

        # pack json + txt
        write_json(out_dir / "pack_btc_eth.json", pack_btc_eth_json)
        atomic_write_bytes(out_dir / "pack_btc_eth.txt", pack_btc_eth_txt.encode("utf-8"))

        # pointer .txt per symbol/tf (JSON с урлами)
        for sym in SYMBOLS_CORE10:
            for tf in TFS:
                meta = by_symbol_tf[sym][tf]
                n = meta["tail_n"]
                pointer = {
                    "updated_utc": updated_utc,
                    "symbol": sym,
                    "tf": tf,
                    "schema": meta["schema"],
                    "last_url": meta["last_url"],
                    "tail_n": n,
                    "tail_chunks_url": meta["tail_chunks_url"],
                }
                write_json(out_dir / f"{sym}_{tf}.txt", pointer)


if __name__ == "__main__":
    main()
