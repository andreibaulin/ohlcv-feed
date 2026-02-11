#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> None:
    # Источник правды: status_btc_eth.json (его уже генерит gen_pack_btc_eth.py)
    status_path = Path("ohlcv/binance/status_btc_eth.json")
    if not status_path.exists():
        raise SystemExit(f"Missing {status_path}. Did gen_pack_btc_eth.py run?")

    status = json.loads(status_path.read_text(encoding="utf-8"))

    updated_utc = status.get("updated_utc", "unknown")
    base_url = status.get("base_url", "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/")

    # Строим аккуратный многострочный pack
    lines: list[str] = []
    lines.append(f"# updated_utc: {updated_utc}")
    lines.append("# cache-bust: все ссылки ниже содержат ?v=updated_utc")
    lines.append("")

    # MAIN (держим совместимость с тем, что уже лежит в pack сейчас)
    lines.append("# MAIN")
    lines.append(f"{base_url}core5_latest.json?v={updated_utc}")
    lines.append(f"{base_url}symbols.json?v={updated_utc}")
    lines.append(f"{base_url}status_btc_eth.json?v={updated_utc}")
    lines.append(f"{base_url}pack_btc_eth.json?v={updated_utc}")
    lines.append(f"{base_url}pack_btc_eth.txt?v={updated_utc}")
    lines.append("")

    # Symbols/TF как в status: files.txt / files.last / files.tail
    symbols = status.get("symbols", {})
    order_syms = ["BTCUSDT", "ETHUSDT"]
    order_tfs = ["H1", "H4", "D1", "W1"]

    for sym in order_syms:
        if sym not in symbols:
            continue
        lines.append(f"# {sym}")
        for tf in order_tfs:
            tf_obj = symbols[sym].get(tf)
            if not tf_obj:
                continue
            files = tf_obj.get("files", {})
            # last/tail нужны всегда; txt — оставим тоже (на будущее)
            last_fn = files.get("last")
            tail_fn = files.get("tail")
            txt_fn = files.get("txt")

            if txt_fn:
                lines.append(f"{base_url}{txt_fn}?v={updated_utc}")
            if last_fn:
                lines.append(f"{base_url}{last_fn}?v={updated_utc}")
            if tail_fn:
                lines.append(f"{base_url}{tail_fn}?v={updated_utc}")
        lines.append("")

    text = "\n".join(lines).rstrip() + "\n"

    # Пишем в ohlcv/ (Pages у тебя реально отдаёт это из /ohlcv/...) и дублируем в docs/ на всякий
    write_text(Path("ohlcv/binance/pack_btc_eth.txt"), text)

    docs_dir = Path("docs/ohlcv/binance")
    if docs_dir.parent.parent.exists():
        write_text(docs_dir / "pack_btc_eth.txt", text)


if __name__ == "__main__":
    main()
