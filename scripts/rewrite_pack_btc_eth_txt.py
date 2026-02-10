import json
import pathlib

BASE_URL = "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/"
OUT_REL = pathlib.Path("ohlcv/binance/pack_btc_eth.txt")
STATUS_REL = pathlib.Path("ohlcv/binance/status_btc_eth.json")


def build_lines(status: dict) -> str:
    updated = status.get("updated_utc", "")
    v = updated

    lines = []
    lines.append(f"# updated_utc: {updated}")
    lines.append("# cache-bust: все ссылки ниже содержат ?v=updated_utc")
    lines.append("")
    lines.append("# MAIN")
    lines.append(f"{BASE_URL}core5_latest.json?v={v}")
    lines.append(f"{BASE_URL}symbols.json?v={v}")
    lines.append(f"{BASE_URL}status_btc_eth.json?v={v}")
    lines.append(f"{BASE_URL}pack_btc_eth.json?v={v}")
    lines.append(f"{BASE_URL}pack_btc_eth.txt?v={v}")
    lines.append("")

    symbols = status.get("symbols", {})
    for sym in ["BTCUSDT", "ETHUSDT"]:
        lines.append(f"# {sym}")
        tfs = symbols.get(sym, {})
        for tf in ["H1", "H4", "D1", "W1"]:
            info = tfs.get(tf, {})
            files = info.get("files", {}) if isinstance(info, dict) else {}
            # КЛЮЧЕВОЕ: НИКАКИХ *.txt В PACK — только last + tail json
            last = files.get("last")
            tail = files.get("tail")
            if last:
                lines.append(f"{BASE_URL}{last}?v={v}")
            if tail:
                lines.append(f"{BASE_URL}{tail}?v={v}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def main():
    if not STATUS_REL.exists():
        raise RuntimeError(f"status file not found: {STATUS_REL}")

    status = json.loads(STATUS_REL.read_text(encoding="utf-8"))
    txt = build_lines(status)

    # Пишем в root
    OUT_REL.parent.mkdir(parents=True, exist_ok=True)
    OUT_REL.write_text(txt, encoding="utf-8")

    # Если Pages у тебя из /docs — продублируем
    out2 = pathlib.Path("docs") / OUT_REL
    out2.parent.mkdir(parents=True, exist_ok=True)
    out2.write_text(txt, encoding="utf-8")


if __name__ == "__main__":
    main()
