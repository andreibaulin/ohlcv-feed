#!/usr/bin/env python3
from __future__ import annotations

import json
import datetime as dt
from pathlib import Path
from typing import Any, Optional, Tuple

BASE_URL = "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/"

TARGET_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
PREFERRED_TFS = ["H1", "H4", "D1", "W1"]

# tail sizes: small enough for fast fetch + easy parsing
TAIL_N = {
    "H1": 240,   # ~10 days
    "H4": 240,   # ~40 days
    "D1": 400,   # ~1.1 years
    "W1": 260,   # ~5 years
}


def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def pick_base_dir() -> Path:
    """
    Prefer docs/ohlcv/binance (common for GitHub Pages source),
    fallback to ohlcv/binance.
    """
    candidates = [Path("docs/ohlcv/binance"), Path("ohlcv/binance")]
    for p in candidates:
        if (p / "symbols.json").exists():
            return p
    for p in candidates:
        if p.exists():
            return p
    raise SystemExit("No ohlcv/binance directory found (expected docs/ohlcv/binance or ohlcv/binance).")


def safe_read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="replace")


def extract_candles(obj: Any) -> Optional[list]:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in ("data", "candles", "ohlcv"):
            v = obj.get(k)
            if isinstance(v, list):
                return v
    return None


def read_candles(path: Path) -> Optional[list]:
    if not path.exists():
        return None
    txt = safe_read_text(path).strip()
    if not txt:
        return []
    try:
        obj = json.loads(txt)
        candles = extract_candles(obj)
        return candles if candles is not None else None
    except Exception:
        # fallback: JSONL
        out = []
        for ln in txt.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            try:
                out.append(json.loads(ln))
            except Exception:
                continue
        return out if out else None


def last_times_ms(candle: Any) -> Tuple[Optional[int], Optional[int]]:
    """
    Binance klines often: [open_ms, open, high, low, close, vol, close_ms, ...]
    but we don't hard-require the shape.
    """
    if not isinstance(candle, list) or not candle:
        return None, None

    open_ms = candle[0] if isinstance(candle[0], (int, float)) else None
    close_ms = candle[6] if len(candle) > 6 and isinstance(candle[6], (int, float)) else None

    if open_ms is not None:
        open_ms = int(open_ms)
    if close_ms is not None:
        close_ms = int(close_ms)
    elif open_ms is not None:
        close_ms = open_ms

    return open_ms, close_ms


def ms_to_utc_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    try:
        return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def write_json(path: Path, obj: Any, pretty: bool) -> None:
    if pretty:
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    else:
        path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")


def main() -> None:
    base = pick_base_dir()
    base.mkdir(parents=True, exist_ok=True)

    updated_utc = utc_now_iso()

    # discover available TFs (H1 already есть у тебя) 2
    tfs = PREFERRED_TFS[:]
    symbols_meta_path = base / "symbols.json"
    if symbols_meta_path.exists():
        try:
            meta = json.loads(safe_read_text(symbols_meta_path))
            if isinstance(meta.get("tfs"), list):
                tfs = [tf for tf in tfs if tf in meta["tfs"]]
        except Exception:
            pass

    status: dict[str, Any] = {
        "updated_utc": updated_utc,
        "base_url": BASE_URL,
        "symbols": {},
    }

    for sym in TARGET_SYMBOLS:
        status["symbols"][sym] = {}
        for tf in tfs:
            src = base / f"{sym}_{tf}.txt"
            candles = read_candles(src)
            if candles is None:
                continue
            if not candles:
                continue

            n = int(TAIL_N.get(tf, 240))
            tail = candles[-n:] if len(candles) > n else candles

            tail_json_name = f"{sym}_{tf}_tail{n}.json"
            tail_jsonl_name = f"{sym}_{tf}_tail{n}.jsonl"
            last_json_name = f"{sym}_{tf}_last.json"

            tail_json = base / tail_json_name
            tail_jsonl = base / tail_jsonl_name
            last_json = base / last_json_name

            write_json(tail_json, tail, pretty=False)

            with tail_jsonl.open("w", encoding="utf-8") as w:
                for c in tail:
                    w.write(json.dumps(c, ensure_ascii=False, separators=(",", ":")))
                    w.write("\n")

            last_bar = tail[-1]
            open_ms, close_ms = last_times_ms(last_bar)

            last_obj = {
                "symbol": sym,
                "tf": tf,
                "updated_utc": updated_utc,
                "last_open_ms": open_ms,
                "last_close_ms": close_ms,
                "last_open_utc": ms_to_utc_iso(open_ms),
                "last_close_utc": ms_to_utc_iso(close_ms),
                "bar": last_bar,
            }
            write_json(last_json, last_obj, pretty=True)

            status["symbols"][sym][tf] = {
                "full": f"{BASE_URL}{sym}_{tf}.txt",
                "last": f"{BASE_URL}{last_json_name}",
                "tail_json": f"{BASE_URL}{tail_json_name}",
                "tail_jsonl": f"{BASE_URL}{tail_jsonl_name}",
                "last_close_utc": last_obj["last_close_utc"],
            }

    # status (quick check)
    write_json(base / "status_btc_eth.json", status, pretty=True)

    # pack txt (easy copy/paste)
    pack_lines = [
        f"# updated_utc: {updated_utc}",
        f"# cache-bust tip: add ?v={updated_utc}",
        "",
        "# MAIN",
        f"{BASE_URL}core5_latest.json",
        f"{BASE_URL}symbols.json",
        f"{BASE_URL}status_btc_eth.json",
        f"{BASE_URL}pack_btc_eth.txt",
        "",
        "# BTCUSDT",
    ]
    for tf in tfs:
        pack_lines += [
            f"{BASE_URL}BTCUSDT_{tf}.txt",
            f"{BASE_URL}BTCUSDT_{tf}_last.json",
            f"{BASE_URL}BTCUSDT_{tf}_tail{int(TAIL_N.get(tf,240))}.jsonl",
        ]
    pack_lines += ["", "# ETHUSDT"]
    for tf in tfs:
        pack_lines += [
            f"{BASE_URL}ETHUSDT_{tf}.txt",
            f"{BASE_URL}ETHUSDT_{tf}_last.json",
            f"{BASE_URL}ETHUSDT_{tf}_tail{int(TAIL_N.get(tf,240))}.jsonl",
        ]
    (base / "pack_btc_eth.txt").write_text("\n".join(pack_lines) + "\n", encoding="utf-8")

    # pack json (machine-friendly)
    pack_json = {
        "updated_utc": updated_utc,
        "base_url": BASE_URL,
        "symbols": status["symbols"],
    }
    write_json(base / "pack_btc_eth.json", pack_json, pretty=True)

    # index page for /ohlcv/binance/ (so directory URL works)
    index_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>ohlcv-feed / binance</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }}
    code {{ background: #f4f4f4; padding: 2px 6px; border-radius: 6px; }}
    a {{ text-decoration: none; }}
    ul {{ line-height: 1.8; }}
  </style>
</head>
<body>
  <h2>ohlcv-feed / binance</h2>
  <p>updated_utc: <code>{updated_utc}</code></p>
  <ul>
    <li><a href="pack_btc_eth.txt">pack_btc_eth.txt</a></li>
    <li><a href="pack_btc_eth.json">pack_btc_eth.json</a></li>
    <li><a href="status_btc_eth.json">status_btc_eth.json</a></li>
    <li><a href="symbols.json">symbols.json</a></li>
    <li><a href="core5_latest.json">core5_latest.json</a></li>
  </ul>
  <p>Tip: if browser caches, append <code>?v={updated_utc}</code>.</p>
</body>
</html>
"""
    (base / "index.html").write_text(index_html, encoding="utf-8")

    print(f"postprocess OK: base={base} updated_utc={updated_utc}")


if __name__ == "__main__":
    main()
