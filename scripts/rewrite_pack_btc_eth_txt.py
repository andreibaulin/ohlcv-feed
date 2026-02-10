#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path


BASE_FALLBACK = "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/"
SYMS = ["BTCUSDT", "ETHUSDT"]
TFS = ["H1", "H4", "D1", "W1"]


def build_pack_text(status: dict) -> str:
    v = status.get("updated_utc")
    if not v:
        raise RuntimeError("status_btc_eth.json missing updated_utc")

    base = status.get("base_url") or BASE_FALLBACK
    if not base.endswith("/"):
        base += "/"

    lines = [
        f"# updated_utc: {v}",
        "# cache-bust: все ссылки ниже содержат ?v=updated_utc",
        "# MAIN",
        f"{base}core5_latest.json?v={v}",
        f"{base}symbols.json?v={v}",
        f"{base}status_btc_eth.json?v={v}",
        f"{base}pack_btc_eth.json?v={v}",
        f"{base}pack_btc_eth.txt?v={v}",
    ]

    symbols_block = status.get("symbols", {})

    for sym in SYMS:
        lines.append(f"# {sym}")
        sym_block = symbols_block.get(sym, {})
        for tf in TFS:
            tf_block = sym_block.get(tf, {})
            files = tf_block.get("files", {})
            last_fn = files.get("last")
            tail_fn = files.get("tail")
            if last_fn:
                lines.append(f"{base}{last_fn}?v={v}")
            if tail_fn:
                lines.append(f"{base}{tail_fn}?v={v}")

    return "\n".join(lines) + "\n"


def write_if_dir_exists(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> None:
    status_path = Path("ohlcv/binance/status_btc_eth.json")
    if not status_path.exists():
        raise RuntimeError("Missing ohlcv/binance/status_btc_eth.json (generate step failed?)")

    status = json.loads(status_path.read_text(encoding="utf-8"))
    txt = build_pack_text(status)

    # root output
    write_if_dir_exists(Path("ohlcv/binance/pack_btc_eth.txt"), txt)

    # docs output (если Pages настроен на /docs)
    docs_dir = Path("docs/ohlcv/binance")
    if docs_dir.exists():
        write_if_dir_exists(docs_dir / "pack_btc_eth.txt", txt)

    print("OK: rewrote pack_btc_eth.txt")


if __name__ == "__main__":
    main()
