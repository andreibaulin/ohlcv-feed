#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

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


def safe_int(x):
    try:
        return int(x)
    except Exception:
        try:
            return int(str(x).strip().strip('"'))
        except Exception:
            return None


def ms_to_iso(ms: int) -> str | None:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    except Exception:
        return None


def build_versioned(url: str, v: str) -> str:
    return f"{url}?v={v}"


def _try_parse_multiple_json_values(text: str):
    """
    Поддержка случаев:
    - один JSON-массив: [[...],[...]]
    - несколько JSON-значений подряд: [[...]]\n[[...]] ...
    """
    dec = json.JSONDecoder()
    i = 0
    n = len(text)
    values = []

    while i < n:
        # skip whitespace
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break

        try:
            v, j = dec.raw_decode(text, i)
        except Exception:
            break

        values.append(v)
        i = j

    if not values:
        return None

    # Если это один список — возвращаем его
    if len(values) == 1 and isinstance(values[0], list):
        return values[0]

    # Если это несколько списков — склеиваем
    if all(isinstance(v, list) for v in values):
        out = []
        for v in values:
            out.extend(v)
        return out

    # Иначе непонятный формат
    return None


def _try_parse_csv_rows(text: str):
    """
    CSV/текстовый формат: каждая свеча — строка, поля через запятую.
    Сохраняем числа как строки (кроме timestamp), чтобы не ловить float-округления.
    """
    # Иногда встречается \r без \n
    if "\n" not in text and "\r" in text:
        text = text.replace("\r", "\n")

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None

    rows = []
    for ln in lines:
        # если вдруг есть заголовок
        low = ln.lower()
        if "open_time" in low or low.startswith("time,") or low.startswith("open,"):
            continue

        parts = [p.strip() for p in ln.split(",")]
        if len(parts) < 5:
            continue

        row = []
        for idx, p in enumerate(parts):
            p2 = p.strip().strip('"')
            if idx == 0:
                row.append(safe_int(p2))  # open_time_ms
            else:
                # цены/объёмы оставляем строкой
                row.append(p2)
        rows.append(row)

    return rows if rows else None


def read_rows_any(path: Path):
    """
    Возвращает list[list] или None.
    Не кидает исключения наружу.
    """
    try:
        text = path.read_text(encoding="utf-8", errors="replace").strip()
        if not text:
            return None

        # Быстрый роутинг
        first = text[0]
        if first in "[{":
            rows = _try_parse_multiple_json_values(text)
            if isinstance(rows, list):
                return rows

        # Фолбэк: CSV
        if first.isdigit() or first in "-+":
            rows = _try_parse_csv_rows(text)
            if isinstance(rows, list):
                return rows

        # Последняя попытка: вдруг JSON всё же, но не с первой буквы
        rows = _try_parse_multiple_json_values(text)
        if isinstance(rows, list):
            return rows

        return None

    except Exception:
        return None


def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")


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

    # Генерация *_last.json и *_tail*.jsonl (если смогли распарсить)
    for sym in SYMBOLS:
        status["symbols"][sym] = {}
        for tf in TFS:
            src_txt = out_dir / f"{sym}_{tf}.txt"
            if not src_txt.exists():
                continue

            rows = read_rows_any(src_txt)
            parse_ok = isinstance(rows, list) and len(rows) > 0

            last_open_ms = None
            last_close_ms = None

            files = {"txt": f"{sym}_{tf}.txt"}

            if parse_ok:
                last = rows[-1]

                # попытка вытащить времена как у Binance kline:
                # [openTime, open, high, low, close, volume, closeTime, ...]
                if isinstance(last, list) and len(last) >= 1:
                    last_open_ms = safe_int(last[0])
                if isinstance(last, list) and len(last) >= 7:
                    last_close_ms = safe_int(last[6])

                # last.json
                last_path = out_dir / f"{sym}_{tf}_last.json"
                write_json(last_path, last)
                files["last"] = last_path.name

                # tail.jsonl
                n = TAIL_N.get(tf, 240)
                tail_rows = rows[-n:]
                tail_path = out_dir / f"{sym}_{tf}_tail{n}.jsonl"
                write_jsonl(tail_path, tail_rows)
                files["tail"] = tail_path.name

            status["symbols"][sym][tf] = {
                "parse_ok": parse_ok,
                "last_open_utc": ms_to_iso(last_open_ms),
                "last_close_utc": ms_to_iso(last_close_ms),
                "files": files,
            }

    # status_btc_eth.json
    write_json(out_dir / "status_btc_eth.json", status)

    # pack_btc_eth.txt — добавляем только реально существующие файлы
    lines = []
    lines.append(f"# updated_utc: {updated_utc}")
    lines.append("# cache-bust: все ссылки ниже уже содержат ?v=updated_utc")
    lines.append("")
    lines.append("# MAIN")
    for f in ["core5_latest.json", "symbols.json", "status_btc_eth.json", "pack_btc_eth.txt"]:
        lines.append(build_versioned(BASE_URL + f, updated_utc))

    for sym in SYMBOLS:
        lines.append("")
        lines.append(f"# {sym}")
        for tf in TFS:
            txt = out_dir / f"{sym}_{tf}.txt"
            if not txt.exists():
                continue

            # всегда txt
            lines.append(build_versioned(BASE_URL + txt.name, updated_utc))

            # last/tail — только если реально создали
            last = out_dir / f"{sym}_{tf}_last.json"
            if last.exists():
                lines.append(build_versioned(BASE_URL + last.name, updated_utc))

            n = TAIL_N.get(tf, 240)
            tail = out_dir / f"{sym}_{tf}_tail{n}.jsonl"
            if tail.exists():
                lines.append(build_versioned(BASE_URL + tail.name, updated_utc))

    (out_dir / "pack_btc_eth.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
