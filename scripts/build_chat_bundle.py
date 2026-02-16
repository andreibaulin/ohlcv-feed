#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_chat_bundle.py (IRON universal)

Builds:
- ta/binance/chat_bundle_latest.json  (facts + sources hashes + views)
- ta/binance/chat_report_latest.md    (ready-to-paste report for ChatGPT)
- *.sha256 sidecar files

Design principles:
- stdlib only
- contract-driven extraction (IRON_CONTRACT_v1.json)
- fail-closed: if inputs missing -> exit(1)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from zoneinfo import ZoneInfo

from iron_common import (
    read_json, write_json, write_text, sha256_file, sha256_bytes,
    json_pointer_get, coerce_number
)

OUT_ROOTS = [Path("."), Path("docs")]

CONTRACT_PATH = Path("iron/IRON_CONTRACT_CURRENT.json") if Path("iron/IRON_CONTRACT_CURRENT.json").exists() else Path("iron/IRON_CONTRACT_v1.json")

# Handshake links (stable). Chat will use these.
HANDSHAKE_LINKS = [
    # TA + pack
    "https://andreibaulin.github.io/ohlcv-feed/ta/binance/state_btc_eth_latest.json",
    "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/pack_btc_eth.txt",
    # timing candles (binance.vision)
    "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=4h&limit=2",
    "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=2",
    "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=20",
    "https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=4h&limit=2",
    "https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=1h&limit=2",
    "https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=15m&limit=20",
    # core5 (if user wants)
    "https://andreibaulin.github.io/ohlcv-feed/deriv/binance/core5_latest.json",
    # deriv-mini direct
    "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT",
    "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT",
    "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30",
    "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30",
    "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT",
    "https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT",
    "https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30",
    "https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30",
    # IRON files on pages
    "https://andreibaulin.github.io/ohlcv-feed/ta/binance/chat_bundle_latest.json",
    "https://andreibaulin.github.io/ohlcv-feed/ta/binance/chat_report_latest.md",
]

def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def fmt_num(x: Any, digits: int = 2) -> str:
    try:
        v = float(x)
    except Exception:
        return str(x)
    s = f"{v:,.{digits}f}"
    return s.replace(",", " ")  # space thousands

def pick_range_low_high(rng: Any, take: str) -> Any:
    if not isinstance(rng, list) or len(rng) != 2:
        return rng
    return rng[0] if take == "low" else rng[1]

def strength_emoji(q: Optional[Dict[str, Any]]) -> str:
    if not isinstance(q, dict):
        return "⚪"
    tests = q.get("tests", 0) or 0
    rr = q.get("reaction_rate", None)
    fr = q.get("failure_rate", None)
    if tests < 3 or rr is None or fr is None:
        return "⚪"
    # conservative buckets
    if rr >= 0.85 and fr <= 0.30:
        return "🟢"
    if rr >= 0.65 and fr <= 0.55:
        return "🟡"
    return "🔴"

def extract_facts(contract: Dict[str, Any], state: Any, deriv: Any) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []
    for f in contract.get("facts", []):
        src = f["source"]
        ptr = f["pointer"]
        doc = state if src == "state" else deriv
        val = json_pointer_get(doc, ptr)
        facts.append({
            "id": f["id"],
            "source": src,
            "pointer": ptr,
            "type": f.get("type", "any"),
            "value": val,
        })
    return facts

def facts_index(facts: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {f["id"]: f["value"] for f in facts}

def derive_deriv_metrics(bundle: Dict[str, Any], fx: Dict[str, Any]) -> None:
    """
    Add derived metrics (verifier recomputes same).
    - premium_pct = (mark - index) / index * 100
    - oi_delta_pct = (last - first) / first * 100 using openInterestHist_1h_30
    """
    def premium_pct(pidx: Dict[str, Any]) -> Optional[float]:
        try:
            mark = float(pidx.get("markPrice"))
            idx = float(pidx.get("indexPrice"))
            if idx == 0:
                return None
            return (mark - idx) / idx * 100.0
        except Exception:
            return None

    def oi_delta_pct(hist: Any) -> Optional[float]:
        try:
            if not isinstance(hist, list) or len(hist) < 2:
                return None
            first = float(hist[0].get("openInterest"))
            last = float(hist[-1].get("openInterest"))
            if first == 0:
                return None
            return (last - first) / first * 100.0
        except Exception:
            return None

    derived: List[Dict[str, Any]] = []
    for prefix in ("btc", "eth"):
        pidx = fx.get(f"{prefix}.deriv.premiumIndex")
        hist = fx.get(f"{prefix}.deriv.openInterestHist_1h_30")
        derived.append({
            "id": f"{prefix}.deriv.premium_pct",
            "kind": "derived",
            "formula": "((markPrice-indexPrice)/indexPrice)*100",
            "depends_on": [f"{prefix}.deriv.premiumIndex"],
            "value": premium_pct(pidx) if isinstance(pidx, dict) else None,
            "unit": "percent",
        })
        derived.append({
            "id": f"{prefix}.deriv.oi_delta_pct_30h",
            "kind": "derived",
            "formula": "((OI_last - OI_first)/OI_first)*100 using openInterestHist_1h_30",
            "depends_on": [f"{prefix}.deriv.openInterestHist_1h_30"],
            "value": oi_delta_pct(hist),
            "unit": "percent",
        })
    bundle["derived_facts"] = derived

def build_views(contract: Dict[str, Any], fx: Dict[str, Any]) -> Dict[str, Any]:
    views: Dict[str, Any] = {}
    for sym, v in contract.get("views", {}).items():
        out = {"supports": [], "resistances": []}
        for side in ("supports", "resistances"):
            for it in v.get(side, []):
                rng = fx.get(it["fact"])
                if "take" in it:
                    val = pick_range_low_high(rng, it["take"])
                    out[side].append({
                        "name": it["name"],
                        "role": it["role"],
                        "value": val,
                        "from_fact": it["fact"],
                        "take": it["take"],
                    })
                else:
                    q = fx.get(it.get("quality_fact",""))
                    out[side].append({
                        "name": it["name"],
                        "role": it["role"],
                        "range": rng,
                        "from_fact": it["fact"],
                        "quality": {
                            "emoji": strength_emoji(q),
                            "tests": q.get("tests") if isinstance(q, dict) else None,
                            "reaction_rate": q.get("reaction_rate") if isinstance(q, dict) else None,
                            "failure_rate": q.get("failure_rate") if isinstance(q, dict) else None,
                            "days_since_last_test": q.get("days_since_last_test") if isinstance(q, dict) else None,
                        }
                    })
        views[sym] = out
    return views

def render_report(bundle: Dict[str, Any]) -> str:
    # proof header
    gen_utc = bundle["generated_utc"]
    # render local time for Europe/Tallinn
    tallinn = ZoneInfo("Europe/Tallinn")
    gen_local = datetime.fromisoformat(gen_utc.replace("Z","+00:00")).astimezone(tallinn)
    gen_local_str = gen_local.strftime("%Y-%m-%d %H:%M:%S %Z")
    state_upd = bundle["sources"]["state"].get("updated_utc")
    state_sha = bundle["sources"]["state"]["sha256"]
    deriv_sha = bundle["sources"]["deriv_mini"]["sha256"]
    bundle_sha = bundle["bundle_sha256"]
    report_sha = bundle["report_sha256"]

    lines: List[str] = []
    lines.append("IRON-PROOF (НЕ РЕДАКТИРОВАТЬ)")
    lines.append(f"- generated_utc: {gen_utc}")
    lines.append(f"- generated_local: {gen_local_str}")
    lines.append(f"- state.updated_utc: {state_upd}")
    lines.append(f"- state.sha256: {state_sha}")
    lines.append(f"- deriv_mini.sha256: {deriv_sha}")
    lines.append(f"- deriv_mini.fallback_used: {bundle['sources']['deriv_mini'].get('fallback_used', False)}")
    lines.append(f"- bundle.sha256: {bundle_sha}")
    lines.append(f"- report.sha256: {report_sha}")
    lines.append("")
    lines.append("Ссылки (рукопожатие):")
    for u in bundle.get("handshake_links", []):
        lines.append(u)
    lines.append("")
    # Per symbol summary
    for sym in ("BTCUSDT","ETHUSDT"):
        v = bundle["views"].get(sym, {})
        lines.append(f"## {sym}")
        # quick passport
        if sym=="BTCUSDT":
            prefix="btc"
        else:
            prefix="eth"
        fx = bundle["facts_index"]
        lines.append(f"- price(state): {fmt_num(fx[f'{prefix}.price'], 2)}")
        lines.append(f"- regime: {fx[f'{prefix}.regime']} | W1: {fx[f'{prefix}.trend.w1']} | D1: {fx[f'{prefix}.trend.d1']}")
        lines.append(f"- ATR(D1): {fmt_num(fx[f'{prefix}.atr.d1'], 2)} | ATR(H4): {fmt_num(fx[f'{prefix}.atr.h4'], 2)}")
        lines.append(f"- EMA200(D1): {fmt_num(fx[f'{prefix}.ema200.d1'], 2)} | EMA200(W1): {fmt_num(fx[f'{prefix}.ema200.w1'], 2)}")
        lines.append("")
        # levels
        lines.append("### 3 поддержки / 3 сопротивления")
        lines.append("Поддержки:")
        for it in v.get("supports", []):
            if "range" in it:
                r=it["range"]
                q=it.get("quality",{})
                lines.append(f"- {it['name']} ({it['role']}): [{fmt_num(r[0])} – {fmt_num(r[1])}] {q.get('emoji','⚪')} (tests={q.get('tests')}, rr={q.get('reaction_rate')}, fr={q.get('failure_rate')})")
            else:
                lines.append(f"- {it['name']} ({it['role']}): {fmt_num(it['value'])} ⚪")
        lines.append("Сопротивления:")
        for it in v.get("resistances", []):
            if "range" in it:
                r=it["range"]
                q=it.get("quality",{})
                lines.append(f"- {it['name']} ({it['role']}): [{fmt_num(r[0])} – {fmt_num(r[1])}] {q.get('emoji','⚪')} (tests={q.get('tests')}, rr={q.get('reaction_rate')}, fr={q.get('failure_rate')})")
            else:
                lines.append(f"- {it['name']} ({it['role']}): {fmt_num(it['value'])} ⚪")
        lines.append("")
        # deriv mini
        derived = {d["id"]: d for d in bundle.get("derived_facts", [])}
        prem = derived.get(f"{prefix}.deriv.premium_pct", {}).get("value")
        oi_dp = derived.get(f"{prefix}.deriv.oi_delta_pct_30h", {}).get("value")
        # also show mark/index, lastFundingRate, OI snapshot
        pidx = fx.get(f"{prefix}.deriv.premiumIndex", {})
        oi = fx.get(f"{prefix}.deriv.openInterest", {})
        try:
            mark = float(pidx.get("markPrice"))
            idx = float(pidx.get("indexPrice"))
            lfr = float(pidx.get("lastFundingRate"))
        except Exception:
            mark=idx=lfr=None
        try:
            oi_snap = float(oi.get("openInterest"))
        except Exception:
            oi_snap=None
        lines.append("### Deriv MINI (raw + derived)")
        lines.append(f"- premiumIndex: mark={fmt_num(mark, 2)} index={fmt_num(idx, 2)} premium%={fmt_num(prem, 4)} lastFundingRate={lfr}")
        lines.append(f"- openInterest: {oi_snap}")
        lines.append(f"- OI delta 30h: {fmt_num(oi_dp, 2)}%")
        lines.append("")
    return "\n".join(lines).strip() + "\n"

def main() -> None:
    if not CONTRACT_PATH.exists():
        raise SystemExit(f"Missing contract: {CONTRACT_PATH}")

    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    state_path = Path(contract["inputs"]["state_path"])
    deriv_path = Path(contract["inputs"]["deriv_mini_path"])

    if not state_path.exists():
        raise SystemExit(f"Missing state input: {state_path} (run scripts/build_ta_state.py first)")
    if not deriv_path.exists():
        raise SystemExit(f"Missing deriv_mini input: {deriv_path} (run scripts/build_deriv_mini_binance.py first)")

    state = read_json(state_path)
    deriv = read_json(deriv_path)

    facts = extract_facts(contract, state, deriv)
    fx = facts_index(facts)

    state_sha = sha256_file(state_path)
    deriv_sha = sha256_file(deriv_path)

    generated_utc = iso_z(utc_now())

    bundle: Dict[str, Any] = {
        "schema": "iron.chat_bundle.v1",
        "generated_utc": generated_utc,
        "handshake_links": HANDSHAKE_LINKS,
        "sources": {
            "state": {
                "path": str(state_path),
                "sha256": state_sha,
                "updated_utc": state.get("updated_utc"),
                "source": state.get("source"),
            },
            "deriv_mini": {
                "path": str(deriv_path),
                "sha256": deriv_sha,
                "generated_utc": deriv.get("meta", {}).get("generated_utc"),
                "fallback_used": deriv.get("meta", {}).get("fallback_used", False),
            },
            "contract": {
                "path": str(CONTRACT_PATH),
                "sha256": sha256_file(CONTRACT_PATH),
            },
        },
        "facts": facts,
        # convenience index (for report generation)
        "facts_index": fx,
    }

    # Views from contract mapping
    bundle["views"] = build_views(contract, fx)

    # Derived metrics from deriv-mini
    derive_deriv_metrics(bundle, fx)

    # compute sha for bundle (without self hashes first)
    tmp = dict(bundle)
    tmp.pop("bundle_sha256", None)
    tmp.pop("report_sha256", None)
    bundle_bytes = json.dumps(tmp, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    bundle_sha = sha256_bytes(bundle_bytes)
    bundle["bundle_sha256"] = bundle_sha

    # Render report and compute sha
    # report uses facts_index; we keep it in bundle (verified separately)
    report = render_report(bundle)
    report_sha = sha256_bytes(report.encode("utf-8"))
    bundle["report_sha256"] = report_sha

    # Write outputs to both roots
    rel_bundle = Path("ta/binance/chat_bundle_latest.json")
    rel_report = Path("ta/binance/chat_report_latest.md")
    rel_bundle_sha = Path("ta/binance/chat_bundle_latest.sha256")
    rel_report_sha = Path("ta/binance/chat_report_latest.sha256")

    for root in OUT_ROOTS:
        write_json(root / rel_bundle, bundle)
        write_text(root / rel_report, report)
        write_text(root / rel_bundle_sha, f"{bundle_sha}  {rel_bundle.name}\n")
        write_text(root / rel_report_sha, f"{report_sha}  {rel_report.name}\n")

if __name__ == "__main__":
    main()
