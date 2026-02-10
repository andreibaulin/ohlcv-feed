#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path
from datetime import datetime, timezone

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
TFS = ["H1", "H4", "D1", "W1"]

TAIL_N = {
    "H1": 240,
    "H4": 240,
    "D1": 400,
    "W1": 260,
}

BASE_URL = "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/"


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def safe_int(x):
    try:
        return int(x)
    except Exception:
        return None


def read_json_array(path: Path):
    # ожидаем JSON массив (как Binance klines: [ [...], [...], ... ])
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")


def build_versioned(url: str, v: str) -> str:
    # кеш-бастинг через query (работает и в браузере, и в любых клиентах)
    return f"{url}?v={v}"


def main():
    repo_root = Path(__file__).resolve().parents[1]
    out_dir = repo_root / "docs" / "ohlcv" / "binance"
    out_dir.mkdir(parents=True, exist_ok=True)

    updated_utc = iso_utc_now()

    status = {
        "updated_utc": updated_utc,
        "base_url": BASE_URL,
        "symbols": {},
    }

    # Генерим *_last.json и *_tail*.jsonl из существующих *.txt
    for sym in SYMBOLS:
        status["symbols"][sym] = {}
        for tf in TFS:
            src_txt = out_dir / f"{sym}_{tf}.txt"
            if not src_txt.exists():
                # если файла нет — просто пропускаем
                continue

            data = read_json_array(src_txt)
            bars = len(data) if isinstance(data, list) else 0

            last = data[-1] if bars > 0 else None
            last_open_ms = None
            last_close_ms = None

            # Binance kline обычно: [open_time, ..., close_time, ...]
            if isinstance(last, list) and len(last) >= 1:
                last_open_ms = safe_int(last[0])
            if isinstance(last, list) and len(last) >= 7:
                last_close_ms = safe_int(last[6])

            # last.json
            last_path = out_dir / f"{sym}_{tf}_last.json"
            write_json(last_path, last if last is not None else [])

            # tail.jsonl
            n = TAIL_N.get(tf, 240)
            tail_rows = data[-n:] if (isinstance(data, list) and bars > 0) else []
            tail_path = out_dir / f"{sym}_{tf}_tail{n}.jsonl"
            write_jsonl(tail_path, tail_rows)

            status["symbols"][sym][tf] = {
                "bars": bars,
                "last_open_time_ms": last_open_ms,
                "last_open_utc": ms_to_iso(last_open_ms) if last_open_ms is not None else None,
                "last_close_time_ms": last_close_ms,
                "last_close_utc": ms_to_iso(last_close_ms) if last_close_ms is not None else None,
                "files": {
                    "txt": f"{sym}_{tf}.txt",
                    "last": f"{sym}_{tf}_last.json",
                    "tail": f"{sym}_{tf}_tail{n}.jsonl",
                },
            }

    # status_btc_eth.json
    write_json(out_dir / "status_btc_eth.json", status)

    # pack_btc_eth.txt — пишем только реальные файлы + добавляем ?v=updated_utc
    lines = []
    lines.append(f"# updated_utc: {updated_utc}")
    lines.append("# cache-bust: все ссылки ниже уже содержат ?v=updated_utc")

    # MAIN
    main_files = ["core5_latest.json", "symbols.json", "status_btc_eth.json", "pack_btc_eth.txt"]
    lines.append("")
    lines.append("# MAIN")
    for f in main_files:
        lines.append(build_versioned(BASE_URL + f, updated_utc))

    for sym in SYMBOLS:
        lines.append("")
        lines.append(f"# {sym}")
        for tf in TFS:
            # добавляем только если исходный txt существует
            if not (out_dir / f"{sym}_{tf}.txt").exists():
                continue

            n = TAIL_N.get(tf, 240)
            candidates = [
                f"{sym}_{tf}.txt",
                f"{sym}_{tf}_last.json",
                f"{sym}_{tf}_tail{n}.jsonl",
            ]
            for f in candidates:
                p = out_dir / f
                if p.exists():
                    lines.append(build_versioned(BASE_URL + f, updated_utc))

    pack_path = out_dir / "pack_btc_eth.txt"
    pack_path.parent.mkdir(parents=True, exist_ok=True)
    pack_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
