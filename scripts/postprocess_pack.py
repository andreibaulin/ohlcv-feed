from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone


def iso_utc_now_micro() -> str:
    # 2026-02-10T12:06:49.251998Z
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def pages_base_url() -> str:
    # Derive from GITHUB_REPOSITORY if possible, fallback to your repo
    repo = os.environ.get("GITHUB_REPOSITORY", "andreibaulin/ohlcv-feed")
    owner, name = repo.split("/", 1)
    return f"https://{owner}.github.io/{name}/ohlcv/binance/"


def file_exists(p: Path) -> bool:
    try:
        return p.is_file() and p.stat().st_size > 0
    except Exception:
        return False


def build_pack_lines(base_url: str, v: str, out_dir: Path) -> list[str]:
    def u(filename: str) -> str:
        # Cache-bust every underlying file read by clients
        return f"{base_url}{filename}?v={v}"

    lines: list[str] = []
    lines.append(f"# updated_utc: {v}")
    lines.append(f"# cache-bust tip: add ?v={v}")
    lines.append("")
    lines.append("# MAIN")

    meta_files = [
        "core5_latest.json",
        "symbols.json",
        "symbols_last.json",
        "symbols_btc_eth.json",
        "pack_btc_eth.txt",
    ]
    for f in meta_files:
        if file_exists(out_dir / f):
            lines.append(u(f))

    symbols = ["BTCUSDT", "ETHUSDT"]

    # Order and filenames: keep compatible with what you already publish
    tf_order = [
        ("H1", ["{sym}_H1.txt", "{sym}_H1_last.json", "{sym}_H1_tail240.jsonl"]),
        ("H4", ["{sym}_H4.txt", "{sym}_H4_last.json", "{sym}_H4_tail120.jsonl", "{sym}_H4_tail360.jsonl"]),
        ("D1", ["{sym}_D1.txt", "{sym}_D1_last.json", "{sym}_D1_tail300.jsonl"]),
        ("W1", ["{sym}_W1.txt", "{sym}_W1_last.json", "{sym}_W1_tail2160.jsonl"]),
    ]

    for sym in symbols:
        lines.append("")
        lines.append(f"# {sym}")
        for _tf, patterns in tf_order:
            for pat in patterns:
                fn = pat.format(sym=sym)
                if file_exists(out_dir / fn):
                    lines.append(u(fn))

    lines.append("")  # trailing newline at EOF
    return lines


def main() -> None:
    # GitHub Pages is configured to publish from /docs in this repo (your files live there).
    out_dir = Path("docs/ohlcv/binance")
    if not out_dir.exists():
        raise SystemExit("Expected output directory not found: docs/ohlcv/binance")

    v = iso_utc_now_micro()
    base_url = pages_base_url()

    pack_path = out_dir / "pack_btc_eth.txt"
    pack_lines = build_pack_lines(base_url=base_url, v=v, out_dir=out_dir)
    pack_path.write_text("\n".join(pack_lines), encoding="utf-8")


if __name__ == "__main__":
    main()
