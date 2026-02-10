from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

BASE_URL = "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/"

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = REPO_ROOT / "docs" / "ohlcv" / "binance"

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
TFS = ["H1", "H4", "D1", "W1"]  # если H1 не нужен — убери из списка


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_json_load(path: Path):
    try:
        txt = path.read_text(encoding="utf-8").strip()
        if not txt:
            return None
        return json.loads(txt)
    except Exception:
        return None


def extract_last_close_utc(obj) -> str | None:
    """
    Поддержка форматов:
    - Binance klines: list[list], где closeTime на индексе 6
    - Обёртки вида {"data":[...]}
    """
    if obj is None:
        return None

    if isinstance(obj, dict) and "data" in obj:
        obj = obj.get("data")

    if not isinstance(obj, list) or not obj:
        return None

    last = obj[-1]

    # dict-формат (на всякий)
    if isinstance(last, dict):
        close_ms = (
            last.get("closeTime")
            or last.get("close_time")
            or last.get("close_time_ms")
            or last.get("t_close")
        )
        try:
            close_ms = int(close_ms)
        except Exception:
            return None
        return datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")

    # list-формат Binance
    if isinstance(last, list):
        close_ms = None
        if len(last) >= 7:
            close_ms = last[6]
        elif len(last) >= 1:
            close_ms = last[0]

        try:
            close_ms = int(close_ms)
        except Exception:
            return None

        return datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")

    return None


def build_status(updated_utc: str) -> dict:
    status = {
        "updated_utc": updated_utc,
        "base_url": BASE_URL,
        "symbols": {},
    }

    for sym in SYMBOLS:
        status["symbols"][sym] = {}
        for tf in TFS:
            fname = f"{sym}_{tf}.txt"
            fpath = DOCS_DIR / fname
            if not fpath.exists():
                continue

            obj = safe_json_load(fpath)
            last_close_utc = extract_last_close_utc(obj)

            status["symbols"][sym][tf] = {
                "url": f"{BASE_URL}{fname}?v={updated_utc}",
                "file": fname,
                "last_close_utc": last_close_utc,
            }

    return status


def build_pack_lines(updated_utc: str) -> list[str]:
    lines: list[str] = []
    lines.append(f"# updated_utc: {updated_utc}")
    lines.append(f"# cache-bust tip: add ?v={updated_utc} to any URL (already applied below)")
    lines.append("")
    lines.append("# MAIN")

    main_files = [
        "core5_latest.json",
        "symbols.json",
        "status_btc_eth.json",
        "pack_btc_eth.txt",
    ]

    for f in main_files:
        # pack и status мы создаём сами; остальные могут быть или нет — но обычно есть
        lines.append(f"{BASE_URL}{f}?v={updated_utc}")

    for sym in SYMBOLS:
        lines.append("")
        lines.append(f"# {sym}")
        for tf in TFS:
            fname = f"{sym}_{tf}.txt"
            if (DOCS_DIR / fname).exists():
                lines.append(f"{BASE_URL}{fname}?v={updated_utc}")

    lines.append("")
    return lines


def main():
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    updated_utc = utc_now_iso()

    # 1) status_btc_eth.json
    status = build_status(updated_utc)
    (DOCS_DIR / "status_btc_eth.json").write_text(
        json.dumps(status, ensure_ascii=False, separators=(", ", ": ")),
        encoding="utf-8",
    )

    # 2) pack_btc_eth.txt (многострочный, без 404-ссылок на *_last/*_tail)
    pack_lines = build_pack_lines(updated_utc)
    (DOCS_DIR / "pack_btc_eth.txt").write_text("\n".join(pack_lines), encoding="utf-8")


if __name__ == "__main__":
    main()
