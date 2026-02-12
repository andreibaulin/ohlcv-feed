#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# ---------------- CONFIG ----------------

DEFAULT_SYMBOLS: List[str] = ["BTCUSDT", "ETHUSDT"]
TFS: List[str] = ["H1", "H4", "D1", "W1"]
TF_TO_INTERVAL: Dict[str, str] = {"H1": "1h", "H4": "4h", "D1": "1d", "W1": "1w"}

TAIL_N: Dict[str, int] = {"H1": 240, "H4": 1500, "D1": 2000, "W1": 520}
CHUNK_SIZE: Dict[str, int] = {"H1": 80, "H4": 150, "D1": 200, "W1": 130}

# Binance max limit per request
FETCH_LIMIT = 1000

# Remove potentially unclosed last bar
SAFETY_MS = 60_000

KLINES_ENDPOINTS: List[str] = [
    "https://data-api.binance.vision/api/v3/klines",
    "https://api.binance.com/api/v3/klines",
]

OUT_ROOTS = [Path("."), Path("docs")]


# ---------------- HELPERS ----------------

def utc_now_iso() -> str:
    dt = datetime.now(timezone.utc)
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def ms_to_utc_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def write_json_compact(path: Path, obj: Any) -> None:
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    atomic_write_bytes(path, s.encode("utf-8"))


def write_json_array_multiline(path: Path, rows: List[Any]) -> None:
    """Write a JSON array across multiple lines (one item per line)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write("[\n")
        for i, row in enumerate(rows):
            if i:
                f.write(",\n")
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
        f.write("\n]\n")
    tmp.replace(path)


def http_get_json(base_url: str, params: Dict[str, Any], timeout: int = 25) -> Any:
    full = base_url + "?" + urlencode(params)
    req = Request(
        full,
        headers={
            "User-Agent": "ohlcv-feed (GitHub Actions)",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw.decode("utf-8"))


def _try_fetch(base: str, params: Dict[str, Any], retries_per_endpoint: int) -> List[list]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries_per_endpoint + 1):
        try:
            data = http_get_json(base, params)
            if not isinstance(data, list):
                raise RuntimeError(f"unexpected response type: {type(data)}")
            return data
        except (HTTPError, URLError, TimeoutError, ValueError, RuntimeError) as e:
            last_err = e
            time.sleep(0.8 * attempt)
    raise RuntimeError(f"fetch failed: {last_err}")


def fetch_klines(symbol: str, tf: str, desired: int, retries_per_endpoint: int = 3) -> List[list]:
    """Fetch >= desired klines. Paginates backward via endTime for desired>1000."""
    interval = TF_TO_INTERVAL[tf]
    want = int(desired)
    if want <= 0:
        return []

    seen: Dict[int, list] = {}
    end_time: Optional[int] = None
    safety_loops = 0

    while len(seen) < want and safety_loops < 25:
        remaining = want - len(seen)
        req_limit = min(FETCH_LIMIT, remaining) if end_time is not None else min(FETCH_LIMIT, want)

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
        time.sleep(0.12)

    out = [seen[k] for k in sorted(seen.keys())]
    if len(out) > want:
        out = out[-want:]
    return out


def simplify_klines(raw: List[list]) -> List[list]:
    # [open_time_ms, open, high, low, close, volume, close_time_ms]
    out: List[list] = []
    for k in raw:
        out.append([int(k[0]), str(k[1]), str(k[2]), str(k[3]), str(k[4]), str(k[5]), int(k[6])])
    return out


def only_closed(bars: List[list], now_ms: int) -> List[list]:
    cutoff = now_ms - SAFETY_MS
    return [b for b in bars if int(b[6]) <= cutoff]


def compute_pages_base_url() -> str:
    explicit = os.environ.get("PAGES_BASE_URL")
    if explicit:
        return explicit.rstrip("/") + "/"

    repo = os.environ.get("GITHUB_REPOSITORY", "andreibaulin/ohlcv-feed")
    if "/" in repo:
        owner, name = repo.split("/", 1)
    else:
        owner, name = "andreibaulin", repo
    return f"https://{owner}.github.io/{name}/ohlcv/binance/"


def make_url(base: str, file_name: str, v: str) -> str:
    return f"{base}{file_name}?v={v}"


def cleanup_old_tail_artifacts(base_dir: Path, symbol: str, tf: str) -> None:
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
    pages_base: str,
    symbol: str,
    tf: str,
    n: int,
    rows: List[Any],
    updated_utc: str,
) -> Dict[str, Any]:
    chunk_size = CHUNK_SIZE[tf]

    cleanup_old_tail_artifacts(base_dir, symbol, tf)

    parts: List[Dict[str, Any]] = []
    total = len(rows)
    idx = 0
    part_i = 0

    while idx < total:
        part = rows[idx : idx + chunk_size]
        part_name = f"{symbol}_{tf}_tail{n}_p{part_i:03d}.json"
        write_json_array_multiline(base_dir / part_name, part)

        parts.append(
            {
                "file": part_name,
                "count": len(part),
                "from_open_time_ms": int(part[0][0]) if part else None,
                "to_close_time_ms": int(part[-1][6]) if part else None,
                "url": make_url(pages_base, part_name, updated_utc),
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
    write_json_compact(base_dir / manifest_name, manifest)

    return {
        "chunks_manifest": manifest_name,
        "chunks_manifest_url": make_url(pages_base, manifest_name, updated_utc),
        "parts_count": len(parts),
        "total": total,
    }


def parse_symbols_env() -> List[str]:
    raw = os.environ.get("OHLCV_SYMBOLS", "").strip()
    if not raw:
        return DEFAULT_SYMBOLS
    out: List[str] = []
    for x in raw.split(","):
        s = x.strip().upper()
        if s:
            out.append(s)
    return out or DEFAULT_SYMBOLS


# ---------------- MAIN ----------------

def main() -> None:
    updated_utc = utc_now_iso()
    now_ms = int(time.time() * 1000)

    pages_base = compute_pages_base_url()
    symbols = parse_symbols_env()

    by_symbol_tf: Dict[str, Dict[str, Dict[str, Any]]] = {}
    errors: List[str] = []

    for symbol in symbols:
        by_symbol_tf[symbol] = {}
        for tf in TFS:
            try:
                desired = max(FETCH_LIMIT, TAIL_N[tf])
                raw = fetch_klines(symbol, tf, desired=desired)
                bars = only_closed(simplify_klines(raw), now_ms)
                if len(bars) < 50:
                    raise RuntimeError(f"too few closed bars: {len(bars)}")

                n = TAIL_N[tf]
                tail = bars[-n:] if len(bars) > n else bars

                last_name = f"{symbol}_{tf}_last.json"

                chunks_meta: Dict[str, Any] = {}
                for root in OUT_ROOTS:
                    out_dir = root / "ohlcv" / "binance"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    write_json_compact(out_dir / last_name, tail[-1])
                    chunks_meta = write_tail_chunks(out_dir, pages_base, symbol, tf, n, tail, updated_utc)

                by_symbol_tf[symbol][tf] = {
                    "tf": tf,
                    "tail_n": n,
                    "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
                    "last": last_name,
                    "last_url": make_url(pages_base, last_name, updated_utc),
                    "tail_chunks": chunks_meta.get("chunks_manifest"),
                    "tail_chunks_url": chunks_meta.get("chunks_manifest_url"),
                    "parts_count": chunks_meta.get("parts_count"),
                    "total": chunks_meta.get("total"),
                    "last_close_utc": ms_to_utc_iso(int(tail[-1][6])),
                }

                time.sleep(0.08)

            except Exception as e:
                errors.append(f"{symbol} {tf}: {e}")

    # symbols.json
    symbols_json = {
        "updated_utc": updated_utc,
        "tfs": TFS,
        "symbols": symbols,
        "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
    }

    # status_btc_eth.json (kept for compatibility with your forecast protocol)
    status = {
        "updated_utc": updated_utc,
        "parse_ok": True,
        "errors": errors,
        "symbols": {},
    }
    for sym in ["BTCUSDT", "ETHUSDT"]:
        if sym not in by_symbol_tf:
            continue
        status["symbols"][sym] = {}
        for tf in TFS:
            meta = by_symbol_tf[sym][tf]
            n = meta["tail_n"]
            status["symbols"][sym][tf] = {
                "tf": tf,
                "tail_n": n,
                "last": meta["last"],
                "tail_chunks": f"{sym}_{tf}_tail{n}_chunks.json",
                "urls": {"last": meta["last_url"], "tail_chunks": meta["tail_chunks_url"]},
            }

    # core5_latest.json (in this repo it's core2 by default: BTC+ETH)
    core = {
        "updated_utc": updated_utc,
        "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
        "symbols": {},
    }
    for sym in [s for s in ["BTCUSDT", "ETHUSDT"] if s in by_symbol_tf]:
        core["symbols"][sym] = {}
        for tf in TFS:
            meta = by_symbol_tf[sym][tf]
            core["symbols"][sym][tf] = {
                "tf": tf,
                "tail_n": meta["tail_n"],
                "last_close_utc": meta["last_close_utc"],
                "last_url": meta["last_url"],
                "tail_chunks_url": meta["tail_chunks_url"],
            }

    # pack_btc_eth.json
    pack_json = {
        "updated_utc": updated_utc,
        "schema": "[open_time_ms, open, high, low, close, volume, close_time_ms]",
        "symbols": {},
    }
    for sym in ["BTCUSDT", "ETHUSDT"]:
        if sym not in by_symbol_tf:
            continue
        pack_json["symbols"][sym] = {}
        for tf in TFS:
            meta = by_symbol_tf[sym][tf]
            pack_json["symbols"][sym][tf] = {
                "tf": tf,
                "tail_n": meta["tail_n"],
                "last_close_utc": meta["last_close_utc"],
                "last_url": meta["last_url"],
                "tail_chunks_url": meta["tail_chunks_url"],
            }

    # pack_btc_eth.txt
    v = updated_utc
    lines: List[str] = []
    lines.append(make_url(pages_base, "status_btc_eth.json", v))
    for sym in ["BTCUSDT", "ETHUSDT"]:
        if sym not in by_symbol_tf:
            continue
        for tf in TFS:
            n = TAIL_N[tf]
            lines.append(make_url(pages_base, f"{sym}_{tf}_last.json", v))
            lines.append(make_url(pages_base, f"{sym}_{tf}_tail{n}_chunks.json", v))

    # Extra: pointers to deriv + ta state (written by separate scripts)
    site_root = pages_base[:-len('ohlcv/binance/')] if pages_base.endswith('ohlcv/binance/') else pages_base.rstrip('/')
    deriv_url = f"{site_root}/deriv/binance/core5_latest.json?v={v}"
    ta_url = f"{site_root}/ta/binance/state_btc_eth_latest.json?v={v}"
    lines.append(deriv_url)
    lines.append(ta_url)

    lines.append(make_url(pages_base, "pack_btc_eth.json", v))

    pack_txt = "\n".join(lines) + "\n"

    # feed.json (minimal)
    feed_json = {"updated_utc": updated_utc, "source": "binance_spot", "symbols": symbols, "tfs": TFS}

    # Write summary files + pointer txts
    for root in OUT_ROOTS:
        out_dir = root / "ohlcv" / "binance"
        out_dir.mkdir(parents=True, exist_ok=True)

        write_json_compact(out_dir / "symbols.json", symbols_json)
        write_json_compact(out_dir / "status_btc_eth.json", status)
        write_json_compact(out_dir / "core5_latest.json", core)
        write_json_compact(out_dir / "pack_btc_eth.json", pack_json)
        atomic_write_bytes(out_dir / "pack_btc_eth.txt", pack_txt.encode("utf-8"))

        # per-symbol pointer files: {SYMBOL}_{TF}.txt
        for sym in symbols:
            for tf in TFS:
                meta = by_symbol_tf[sym][tf]
                pointer = {
                    "updated_utc": updated_utc,
                    "symbol": sym,
                    "tf": tf,
                    "schema": meta["schema"],
                    "last_url": meta["last_url"],
                    "tail_n": meta["tail_n"],
                    "tail_chunks_url": meta["tail_chunks_url"],
                }
                write_json_compact(out_dir / f"{sym}_{tf}.txt", pointer)

        write_json_compact(root / "feed.json", feed_json)


if __name__ == "__main__":
    main()
