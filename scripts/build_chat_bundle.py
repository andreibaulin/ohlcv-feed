#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_chat_bundle.py (IRON universal)

Builds (for GitHub Pages /docs):
- docs/ta/binance/chat_bundle_latest.json  (facts + sources hashes + views)
- docs/ta/binance/chat_report_latest.md    (ready-to-paste report for ChatGPT)
- docs/ta/binance/*.sha256 sidecar files

Design principles:
- stdlib only
- contract-driven extraction for FACTS (IRON_CONTRACT_CURRENT.json / v1)
- views (levels) are computed from FULL state to avoid "one huge overlapping zone"
- fail-closed: if inputs missing -> exit(1)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

from iron_common import (
    read_json, write_json, write_text, sha256_file, sha256_bytes,
    json_pointer_get, coerce_number, safe_float_eq
)

# We publish only to /docs (GitHub Pages). Root duplicates were removed on purpose.
OUT_ROOTS = [Path("docs")]

CONTRACT_PATH = (
    Path("iron/IRON_CONTRACT_CURRENT.json")
    if Path("iron/IRON_CONTRACT_CURRENT.json").exists()
    else Path("iron/IRON_CONTRACT_v1.json")
)

# Handshake links (stable). Chat will use these.
HANDSHAKE_LINKS = [
    # IRON
    "https://andreibaulin.github.io/ohlcv-feed/ta/binance/chat_bundle_latest.json",
    "https://andreibaulin.github.io/ohlcv-feed/ta/binance/chat_report_latest.md",
    # TA (swing + full)
    "https://andreibaulin.github.io/ohlcv-feed/ta/binance/state_btc_eth_latest.json",
    "https://andreibaulin.github.io/ohlcv-feed/ta/binance/state_btc_eth_full_latest.json",
    # pack (OHLCV)
    "https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/pack_btc_eth.txt",
    # deriv snapshot (GitHub Pages)
    "https://andreibaulin.github.io/ohlcv-feed/deriv/binance/core5_latest.json",
    # timing candles (binance.vision)
    "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=4h&limit=2",
    "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=2",
    "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=20",
    "https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=4h&limit=2",
    "https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=1h&limit=2",
    "https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=15m&limit=20",
    # deriv (live, pull directly from Binance)
    "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT",
    "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT",
    "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30",
    "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30",
    "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT",
    "https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT",
    "https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30",
    "https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso_z(s: str) -> Optional[datetime]:
    if not isinstance(s, str) or not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def days_since(ts_utc: Optional[str], now_utc: datetime) -> Optional[float]:
    dt = parse_iso_z(ts_utc) if ts_utc else None
    if dt is None:
        return None
    return round((now_utc - dt).total_seconds() / 86400.0, 3)


def fmt_num(x: Any, digits: int = 2) -> str:
    try:
        v = float(x)
    except Exception:
        return str(x)
    s = f"{v:,.{digits}f}"
    return s.replace(",", " ")  # space thousands


def fmt_range(rng: Any, digits: int = 2) -> str:
    if not isinstance(rng, list) or len(rng) != 2:
        return str(rng)
    return f"[{fmt_num(rng[0], digits)} – {fmt_num(rng[1], digits)}]"


def overlap(a: Tuple[float, float], b: Tuple[float, float]) -> bool:
    return max(a[0], b[0]) <= min(a[1], b[1])


def union(a: Tuple[float, float], b: Tuple[float, float]) -> Tuple[float, float]:
    return (min(a[0], b[0]), max(a[1], b[1]))


def clamp_range(r: Tuple[float, float], lo: Optional[float], hi: Optional[float]) -> Tuple[float, float]:
    a, b = r
    if lo is not None:
        a = max(a, lo)
        b = max(b, lo)
    if hi is not None:
        a = min(a, hi)
        b = min(b, hi)
    if a > b:
        m = (a + b) / 2.0
        return (m, m)
    return (a, b)

def assert_levels_ok(
    sym: str,
    price: float,
    atr_h4: float,
    supports: List[Dict[str, Any]],
    resistances: List[Dict[str, Any]],
) -> None:
    """Hard sanity check for 4S/4R views.

    If this fails, we should NOT publish — it means levels became ambiguous/overlapping.
    """
    eps = max(atr_h4 * 1e-3, price * 1e-6, 1e-9)

    def _norm(r) -> Tuple[float, float]:
        a, b = float(r[0]), float(r[1])
        return (a, b) if a <= b else (b, a)

    def _ov(a: Tuple[float, float], b: Tuple[float, float]) -> bool:
        """True only for a *real* overlap.

        Touching at a single boundary point is allowed (adjacent zones are OK).
        eps protects from float jitter / rounding.
        """
        return max(a[0], b[0]) < (min(a[1], b[1]) - eps)

    def _check(items: List[Dict[str, Any]], side: str) -> None:
        # range integrity + side correctness
        for it in items:
            if "core" not in it or "buffer" not in it:
                raise SystemExit(f"{sym}: missing core/buffer in level item: {it}")
            core = _norm(it["core"])
            buf = _norm(it["buffer"])

            if not (buf[0] - eps <= core[0] <= core[1] <= buf[1] + eps):
                raise SystemExit(f"{sym}: core must be inside buffer (side={side}) core={core} buf={buf}")

            if side == "S":
                # allow touch at price, but not clearly above
                if core[1] > price + eps:
                    raise SystemExit(f"{sym}: support core above price: core={core} price={price}")
            else:
                if core[0] < price - eps:
                    raise SystemExit(f"{sym}: resistance core below price: core={core} price={price}")

        # ordering
        if side == "S":
            if items != sorted(items, key=lambda x: float(_norm(x["core"])[1]), reverse=True):
                raise SystemExit(f"{sym}: supports not sorted (closest first)")
        else:
            if items != sorted(items, key=lambda x: float(_norm(x["core"])[0])):
                raise SystemExit(f"{sym}: resistances not sorted (closest first)")

        # non-overlap (core + buffer)
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                ci = _norm(items[i]["core"])
                cj = _norm(items[j]["core"])
                if _ov(ci, cj):
                    raise SystemExit(f"{sym}: overlapping CORES {items[i].get('name')} {ci} vs {items[j].get('name')} {cj}")
                bi = _norm(items[i]["buffer"])
                bj = _norm(items[j]["buffer"])
                if _ov(bi, bj):
                    raise SystemExit(f"{sym}: overlapping BUFFERS {items[i].get('name')} {bi} vs {items[j].get('name')} {bj}")

    _check(supports, "S")
    _check(resistances, "R")


def strength_emoji_from_rates(tests: int, rr: Optional[float], fr: Optional[float]) -> str:
    if tests < 3 or rr is None or fr is None:
        return "⚪"
    if rr >= 0.85 and fr <= 0.30:
        return "🟢"
    if rr >= 0.65 and fr <= 0.55:
        return "🟡"
    return "🔴"


def behavior_tag(tests: int, rr: Optional[float], fr: Optional[float]) -> str:
    if tests < 3 or rr is None or fr is None:
        return "unknown"
    if rr >= 0.65 and fr <= 0.35:
        return "bounce"
    if fr >= 0.65:
        return "magnet"
    return "mixed"


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



def _local_quality(it: Dict[str, Any], now_utc: datetime) -> Dict[str, Any]:
    touches = int(it.get("touches") or 0)
    rejs = int(it.get("rejections") or 0)
    rr = (rejs / touches) if touches > 0 else None
    fr = (1.0 - rr) if rr is not None else None
    return {
        "tests": touches,
        "reaction_rate": None if rr is None else round(rr, 4),
        "failure_rate": None if fr is None else round(fr, 4),
        "last_touch_utc": it.get("last_touch_utc"),
        "days_since_last_test": days_since(it.get("last_touch_utc"), now_utc),
    }


def _macro_quality_from_reaction(q: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(q, dict):
        return None
    tests = int(q.get("tests") or 0)
    rr = q.get("reaction_rate")
    fr = q.get("failure_rate")
    return {
        "tests": tests,
        "reaction_rate": None if rr is None else round(float(rr), 4),
        "failure_rate": None if fr is None else round(float(fr), 4),
        "last_test_utc": q.get("last_test_utc"),
        "days_since_last_test": q.get("days_since_last_test"),
    }


def _pick_local_levels(
    local_list: List[Dict[str, Any]],
    price: float,
    side: str,
    atr_h4: float,
    n: int = 4,
) -> List[Dict[str, Any]]:
    """Pick up to n disjoint local H4 zones closest to price on the correct side."""
    out: List[Dict[str, Any]] = []

    # Gap to avoid overlaps between chosen levels
    gap = max(price * 0.00035, atr_h4 * 0.03)

    # Distance to price from a zone for the given side
    cands: List[Tuple[float, float, Dict[str, Any]]] = []
    for it in local_list or []:
        z = it.get("zone")
        if not isinstance(z, list) or len(z) != 2:
            continue
        lo, hi = float(z[0]), float(z[1])
        if lo > hi:
            lo, hi = hi, lo

        # keep only relevant side (support below / resistance above),
        # but allow "price inside" (distance=0) as S1/R1 in chop.
        if side == "S":
            if lo <= price <= hi:
                dist = 0.0
            elif hi < price:
                dist = price - hi
            else:
                continue
        else:
            if lo <= price <= hi:
                dist = 0.0
            elif lo > price:
                dist = lo - price
            else:
                continue

        score = float(it.get("score") or 0.0)
        strength = float(it.get("strength") or 0.0)
        # prefer closer; then better score/strength
        cands.append((dist, -(score + 10.0 * strength), it))

    cands.sort(key=lambda x: (x[0], x[1]))

    def _overlaps_any(z: Tuple[float, float]) -> bool:
        for sel in out:
            zl = sel["_core"]
            if overlap(z, zl):
                return True
            # also avoid too tight adjacency
            if z[0] <= zl[1] + gap and z[1] >= zl[0] - gap:
                return True
        return False

    for dist, _, it in cands:
        z = it.get("zone")
        lo, hi = float(z[0]), float(z[1])
        if lo > hi:
            lo, hi = hi, lo
        zr = (lo, hi)
        if _overlaps_any(zr):
            continue
        it2 = dict(it)
        it2["_dist"] = float(dist)
        it2["_core"] = zr
        out.append(it2)
        if len(out) >= n:
            break

    # sort for stable naming: supports top-down, resistances bottom-up
    if side == "S":
        out.sort(key=lambda it: it["_core"][1], reverse=True)
    else:
        out.sort(key=lambda it: it["_core"][0])
    return out


def _macro_context(sym_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Collect macro bands/zones as context for buffer/behavior."""
    out: List[Dict[str, Any]] = []
    zones = (sym_state or {}).get("zones", {}) or {}

    # Range W1 discount/premium bands with reaction stats
    rw1 = zones.get("range_w1") or {}
    for key, side in (("discount", "S"), ("premium", "R")):
        band = (rw1.get("bands") or {}).get(key)
        if isinstance(band, list) and len(band) == 2:
            q = _macro_quality_from_reaction((rw1.get("reaction") or {}).get(key))
            out.append({
                "id": f"range_w1.{key}",
                "tf": "W1",
                "side": side,
                "range": (float(band[0]), float(band[1])),
                "quality": q,
            })

    # Swing D1 entry bands with reaction stats
    sd1 = zones.get("swing_d1") or {}
    for key, side in (("support_entry", "S"), ("resistance_entry", "R")):
        band = (sd1.get("bands") or {}).get(key)
        if isinstance(band, list) and len(band) == 2:
            q = _macro_quality_from_reaction((sd1.get("reaction") or {}).get(key))
            out.append({
                "id": f"swing_d1.{key}",
                "tf": "D1",
                "side": side,
                "range": (float(band[0]), float(band[1])),
                "quality": q,
            })

    # Structural zones list (wide) — buffer only
    structural = (zones.get("structural") or {})
    for side_key, side in (("supports", "S"), ("resistances", "R")):
        for i, it in enumerate(structural.get(side_key) or []):
            z = it.get("zone")
            if isinstance(z, list) and len(z) == 2:
                touches = int(it.get("touches") or 0)
                rejs = int(it.get("rejections") or 0)
                rr = (rejs / touches) if touches > 0 else None
                fr = (1.0 - rr) if rr is not None else None
                q = {
                    "tests": touches,
                    "reaction_rate": None if rr is None else round(rr, 4),
                    "failure_rate": None if fr is None else round(fr, 4),
                    "last_touch_utc": it.get("last_touch_utc"),
                }
                out.append({
                    "id": f"struct.{side_key}.{i}",
                    "tf": it.get("tf"),
                    "side": side,
                    "range": (float(z[0]), float(z[1])),
                    "quality": q,
                })
    return out


def _expand_buffer(
    core: Tuple[float, float],
    macro: List[Dict[str, Any]],
    side: str,
    atr_h4: float,
) -> Tuple[Tuple[float, float], List[str], Optional[Dict[str, Any]]]:
    """
    Expand buffer around a local core by merging overlapping/near macro bands,
    but do NOT let macro swallow other neighbour levels (cap later).
    Returns: (buffer, sources_used, best_macro_quality)
    """
    buf = core
    used: List[str] = []
    best_q: Optional[Dict[str, Any]] = None
    best_q_score = -1.0

    near_tol = max(atr_h4 * 0.10, (core[1] - core[0]) * 0.50)

    for m in macro:
        if m.get("side") != side:
            continue
        mr = m.get("range")
        if not isinstance(mr, tuple) or len(mr) != 2:
            continue
        r = (float(mr[0]), float(mr[1]))
        # overlap OR close enough
        if overlap(buf, r) or abs(r[0] - buf[1]) <= near_tol or abs(buf[0] - r[1]) <= near_tol:
            buf = union(buf, r)
            used.append(m.get("id", "macro"))

        q = m.get("quality")
        if isinstance(q, dict):
            tests = float(q.get("tests") or 0.0)
            rr = q.get("reaction_rate")
            fr = q.get("failure_rate")
            rr_f = float(rr) if rr is not None else 0.0
            fr_f = float(fr) if fr is not None else 1.0
            score = tests * (rr_f + (1.0 - fr_f))
            if score > best_q_score:
                best_q_score = score
                best_q = q

    return buf, used, best_q


def build_levels_v2(sym: str, sym_state: Dict[str, Any], now_utc: datetime) -> Dict[str, Any]:
    """
    Deterministic 4 supports + 4 resistances:
    - Core = local H4 pivot zone
    - Buffer = core expanded by macro bands (W1/D1) that overlap/near, capped to avoid overlap with neighbour levels
    - Strength/behavior derived from local touches/rejections and macro reaction stats (if available)
    """
    data = (sym_state or {}).get("data", {}) or {}
    vol = (sym_state or {}).get("vol", {}) or {}
    zones = (sym_state or {}).get("zones", {}) or {}

    price = float(data.get("price") or 0.0)
    atr_h4 = float(((vol.get("atr14") or {}).get("H4")) or 0.0)

    local = (zones.get("local_h4") or {})
    local_raw_s = list(local.get("supports") or [])
    local_raw_r = list(local.get("resistances") or [])

    structural = (zones.get("structural") or {})

    def _struct_to_local(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for it in items or []:
            z = it.get("zone")
            if isinstance(z, list) and len(z) == 2:
                lo = float(min(z[0], z[1]))
                hi = float(max(z[0], z[1]))
                it2 = dict(it)
                it2["zone"] = [lo, hi]
                # make it comparable with local candidates
                it2["score"] = float(it.get("touches") or 0) * 10.0
                it2["strength"] = float(it.get("strength") or 0)
                out.append(it2)
        return out

    # Combine local + structural, then pick 4 disjoint levels (core ranges) deterministically.
    local_s = _pick_local_levels(local_raw_s + _struct_to_local(structural.get("supports") or []), price, "S", atr_h4, 4)
    local_r = _pick_local_levels(local_raw_r + _struct_to_local(structural.get("resistances") or []), price, "R", atr_h4, 4)
    macro = _macro_context(sym_state)

    def _mk_items(items: List[Dict[str, Any]], side: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for i, it in enumerate(items[:4], start=1):
            core = it.get("_core")
            if not isinstance(core, tuple) or len(core) != 2:
                z = it.get("zone")
                core = (float(z[0]), float(z[1])) if isinstance(z, list) and len(z) == 2 else (0.0, 0.0)
            core = (float(core[0]), float(core[1]))
            if core[0] > core[1]:
                core = (core[1], core[0])

            q_local = _local_quality(it, now_utc)
            buf, used, q_macro_best = _expand_buffer(core, macro, side, atr_h4)

            # choose quality for emoji/behavior: prefer macro reaction stats if present, else local
            q_for_rate = q_macro_best if isinstance(q_macro_best, dict) else q_local
            tests = int(q_for_rate.get("tests") or 0)
            rr = q_for_rate.get("reaction_rate")
            fr = q_for_rate.get("failure_rate")

            beh = behavior_tag(tests, rr, fr)
            emoji = strength_emoji_from_rates(tests, rr, fr)

            # base strength: local 1..5 if present; boost if macro says 🟢
            base_strength = int(it.get("strength") or 0)
            if emoji == "🟢":
                strength_level = min(5, max(base_strength, 4))
            elif emoji == "🟡":
                strength_level = min(5, max(base_strength, 3))
            elif emoji == "🔴":
                strength_level = min(5, max(base_strength, 2))
            else:
                strength_level = max(1, base_strength) if base_strength else 1

            dist = float(it.get("_dist") or 0.0)
            # role label by index (practical): closer = oper, mid = struct, far = macro
            if i <= 2:
                role = "oper"
            elif i == 3:
                role = "struct"
            else:
                role = "macro"

            out.append({
                "name": f"{'S' if side=='S' else 'R'}{i}",
                "role": role,
                "behavior": beh,         # bounce / magnet / mixed / unknown
                "tf_core": it.get("tf", "H4"),
                "core": [round(core[0], 2), round(core[1], 2)],
                "buffer": [round(buf[0], 2), round(buf[1], 2)],
                "strength": {
                    "level": int(strength_level),
                    "emoji": emoji,
                },
                "quality_local": q_local,
                "quality_macro_best": q_macro_best,
                "distance_to_price": round(dist, 2),
                "sources": ["local_h4"] + used,
            })
        return out

    supports = _mk_items(local_s, "S")
    resistances = _mk_items(local_r, "R")

    # Cap buffers to avoid overlaps between adjacent levels
    def _cap(side_items: List[Dict[str, Any]], side: str) -> None:
        # sort by core position
        if side == "S":
            side_items.sort(key=lambda x: x["core"][1], reverse=True)
        else:
            side_items.sort(key=lambda x: x["core"][0])

        # separators between neighbours using cores (not buffers)
        for i in range(len(side_items) - 1):
            a = side_items[i]
            b = side_items[i + 1]
            if side == "S":
                sep = (a["core"][0] + b["core"][1]) / 2.0
                a_buf = (a["buffer"][0], a["buffer"][1])
                b_buf = (b["buffer"][0], b["buffer"][1])
                a_buf = clamp_range(a_buf, sep, None)  # a lower bound >= sep
                b_buf = clamp_range(b_buf, None, sep)  # b upper bound <= sep
                a["buffer"] = [round(a_buf[0], 2), round(a_buf[1], 2)]
                b["buffer"] = [round(b_buf[0], 2), round(b_buf[1], 2)]
            else:
                sep = (a["core"][1] + b["core"][0]) / 2.0
                a_buf = (a["buffer"][0], a["buffer"][1])
                b_buf = (b["buffer"][0], b["buffer"][1])
                a_buf = clamp_range(a_buf, None, sep)  # a upper <= sep
                b_buf = clamp_range(b_buf, sep, None)  # b lower >= sep
                a["buffer"] = [round(a_buf[0], 2), round(a_buf[1], 2)]
                b["buffer"] = [round(b_buf[0], 2), round(b_buf[1], 2)]

        # re-name after sort (stable)
        for idx, it in enumerate(side_items, start=1):
            it["name"] = f"{'S' if side=='S' else 'R'}{idx}"

    _cap(supports, "S")
    _cap(resistances, "R")

    issues: Dict[str, List[str]] = {"warnings": [], "errors": []}
    try:
        assert_levels_ok(sym, price, atr_h4, supports, resistances)
    except SystemExit as e:
        issues["errors"].append(str(e))
    except Exception as e:
        issues["errors"].append(f"{sym}: levels_check_exception: {e}")

    out = {
        "price": price,
        "atr_h4": atr_h4,
        "supports": supports,
        "resistances": resistances,
    }


def build_views_v2(state_full: Dict[str, Any]) -> Dict[str, Any]:
    now_utc = utc_now()
    out: Dict[str, Any] = {}
    symbols = (state_full or {}).get("symbols", {}) or {}
    for sym in ("BTCUSDT", "ETHUSDT"):
        st = symbols.get(sym) or {}
        if isinstance(st, dict) and "error" not in st:
            out[sym] = build_levels_v2(sym, st, now_utc)
        else:
            out[sym] = {"error": st.get("error") if isinstance(st, dict) else "missing"}
    return out


def render_report(bundle: Dict[str, Any]) -> str:
    # proof header
    gen_utc = bundle["generated_utc"]
    tallinn = ZoneInfo("Europe/Tallinn")
    gen_local = datetime.fromisoformat(gen_utc.replace("Z", "+00:00")).astimezone(tallinn)
    gen_local_str = gen_local.strftime("%Y-%m-%d %H:%M:%S %Z")

    state_upd = bundle["sources"]["state"].get("updated_utc")
    state_sha = bundle["sources"]["state"]["sha256"]
    bundle_sha = bundle["bundle_sha256"]

    lines: List[str] = []
    lines.append("IRON-PROOF (НЕ РЕДАКТИРОВАТЬ)")
    lines.append(f"- generated_utc: {gen_utc}")
    lines.append(f"- generated_local: {gen_local_str}")
    lines.append(f"- state.updated_utc: {state_upd}")
    lines.append(f"- state.sha256: {state_sha}")
    lines.append(f"- bundle.sha256: {bundle_sha}")
    lines.append("")
    lines.append("Ссылки (рукопожатие):")
    for u in bundle.get("handshake_links", []):
        lines.append(u)
    lines.append("")

    fx = bundle["facts_index"]

    # Per symbol summary
    for sym in ("BTCUSDT", "ETHUSDT"):
        lines.append(f"## {sym}")
        prefix = "btc" if sym == "BTCUSDT" else "eth"

        lines.append(f"- price(state): {fmt_num(fx.get(f'{prefix}.price'), 2)}")
        lines.append(f"- regime: {fx.get(f'{prefix}.regime')} | W1: {fx.get(f'{prefix}.trend.w1')} | D1: {fx.get(f'{prefix}.trend.d1')}")
        lines.append(f"- ATR(D1): {fmt_num(fx.get(f'{prefix}.atr.d1'), 2)} | ATR(H4): {fmt_num(fx.get(f'{prefix}.atr.h4'), 2)}")
        lines.append(f"- EMA200(D1): {fmt_num(fx.get(f'{prefix}.ema200.d1'), 2)} | EMA200(W1): {fmt_num(fx.get(f'{prefix}.ema200.w1'), 2)}")
        lines.append("")

        v = bundle.get("views", {}).get(sym, {}) or {}
        if "error" in v:
            lines.append(f"### Уровни: ERROR ({v.get('error')})")
        else:
            lines.append("### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)")
            lines.append("Поддержки:")
            for it in v.get("supports", []):
                core = it.get("core")
                buf = it.get("buffer")
                st = it.get("strength", {})
                beh = it.get("behavior")
                role = it.get("role")

                q_used = it.get("quality_macro_best") or it.get("quality_local") or {}
                q_src = "macro" if it.get("quality_macro_best") else "local"

                lines.append(
                    f"- {it['name']} ({role}, {beh}): {fmt_range(core)} "
                    f"{st.get('emoji','⚪')} (силa={st.get('level')}/5, {q_src}: tests={q_used.get('tests')}, rr={q_used.get('reaction_rate')}, fr={q_used.get('failure_rate')})"
                )
                if fmt_range(buf) != fmt_range(core):
                    lines.append(f"  · buf: {fmt_range(buf)}")
            lines.append("Сопротивления:")
            for it in v.get("resistances", []):
                core = it.get("core")
                buf = it.get("buffer")
                st = it.get("strength", {})
                beh = it.get("behavior")
                role = it.get("role")

                q_used = it.get("quality_macro_best") or it.get("quality_local") or {}
                q_src = "macro" if it.get("quality_macro_best") else "local"

                lines.append(
                    f"- {it['name']} ({role}, {beh}): {fmt_range(core)} "
                    f"{st.get('emoji','⚪')} (силa={st.get('level')}/5, {q_src}: tests={q_used.get('tests')}, rr={q_used.get('reaction_rate')}, fr={q_used.get('failure_rate')})"
                )
                if fmt_range(buf) != fmt_range(core):
                    lines.append(f"  · buf: {fmt_range(buf)}")
        lines.append("")
        # derivatives (live links; no GitHub dependency)
        lines.append("### Деривативы (live ссылки Binance FAPI)")
        if sym == "BTCUSDT":
            lines.append("- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT")
            lines.append("- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT")
            lines.append("- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30")
            lines.append("- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30")
        else:
            lines.append("- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT")
            lines.append("- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT")
            lines.append("- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30")
            lines.append("- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def main() -> None:
    generated_utc = iso_z(utc_now())

    # Always produce a status file. Never block publishing/commits.
    status: Dict[str, Any] = {
        "schema": "iron.build_status.v1",
        "generated_utc": generated_utc,
        "quality": "FAIL",
        "warnings": [],
        "errors": [],
        "sources": {},
        "candidate": None,
        "published": None,
        "kept_last_good": False,
        "verify": {"ran": False, "errors": [], "warnings": []},
    }

    rel_status = Path("ta/binance/build_status_latest.json")

    # Standard artifact paths (within docs/)
    rel_bundle_latest = Path("ta/binance/chat_bundle_latest.json")
    rel_report_latest = Path("ta/binance/chat_report_latest.md")
    rel_bundle_latest_sha = Path("ta/binance/chat_bundle_latest.sha256")
    rel_report_latest_sha = Path("ta/binance/chat_report_latest.sha256")

    rel_bundle_bad = Path("ta/binance/chat_bundle_bad_latest.json")
    rel_report_bad = Path("ta/binance/chat_report_bad_latest.md")
    rel_bundle_bad_sha = Path("ta/binance/chat_bundle_bad_latest.sha256")
    rel_report_bad_sha = Path("ta/binance/chat_report_bad_latest.sha256")

    def _set_quality() -> None:
        if status["errors"]:
            status["quality"] = "FAIL"
        elif status["warnings"]:
            status["quality"] = "WARN"
        else:
            status["quality"] = "OK"

    def _verify_candidate(contract_obj: Dict[str, Any], bundle: Dict[str, Any], report: str) -> None:
        # Best-effort self-verify (same spirit as verify_chat_bundle.py), but without aborting the run.
        errs: List[str] = []
        warns: List[str] = []

        # Source files exist
        try:
            state_path = Path(bundle["sources"]["state"]["path"])
        except Exception as e:
            errs.append(f"verify: missing sources.state.path: {e}")
            state_path = None  # type: ignore

        if state_path and not state_path.exists():
            errs.append(f"verify: missing state file referenced by bundle: {state_path}")

        # Sha256 of sources
        if state_path and state_path.exists():
            state_sha = sha256_file(state_path)
            if state_sha != bundle["sources"]["state"].get("sha256"):
                errs.append(f"verify: state.sha256 mismatch: computed={state_sha} bundle={bundle['sources']['state'].get('sha256')}")

        contract_sha = sha256_file(CONTRACT_PATH) if CONTRACT_PATH.exists() else None
        if contract_sha and contract_sha != bundle["sources"]["contract"].get("sha256"):
            errs.append(f"verify: contract.sha256 mismatch: computed={contract_sha} bundle={bundle['sources']['contract'].get('sha256')}")

        # Bundle sha (excluding self-hashes)
        tmp = dict(bundle)
        tmp.pop("bundle_sha256", None)
        tmp.pop("report_sha256", None)
        bundle_bytes = json.dumps(tmp, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        bundle_sha = sha256_bytes(bundle_bytes)
        if bundle_sha != bundle.get("bundle_sha256"):
            errs.append(f"verify: bundle.sha256 mismatch: computed={bundle_sha} bundle={bundle.get('bundle_sha256')}")

        # Report sha
        report_sha = sha256_bytes(report.encode("utf-8"))
        if report_sha != bundle.get("report_sha256"):
            errs.append(f"verify: report.sha256 mismatch: computed={report_sha} bundle={bundle.get('report_sha256')}")

        # Facts pointers
        try:
            state_doc = read_json(state_path) if state_path else {}
        except Exception as e:
            errs.append(f"verify: state JSON read error: {e}")
            state_doc = {}

        deriv_doc: Dict[str, Any] = {}

        fx: Dict[str, Any] = bundle.get("facts_index", {}) or {}
        fx2 = {f.get("id"): f.get("value") for f in bundle.get("facts", [])}
        if fx2 != fx:
            errs.append("verify: facts_index mismatch (facts_index must exactly match facts[] list)")

        for f in bundle.get("facts", []) or []:
            try:
                fid = f["id"]
                src = f["source"]
                ptr = f["pointer"]
                expected = f.get("value")
                doc = state_doc if src == "state" else deriv_doc
                actual = json_pointer_get(doc, ptr)
                if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
                    if not safe_float_eq(float(expected), float(actual)):
                        errs.append(f"verify: fact mismatch {fid}: expected={expected} actual={actual} ptr={ptr} src={src}")
                else:
                    if expected != actual:
                        errs.append(f"verify: fact mismatch {fid}: expected={expected} actual={actual} ptr={ptr} src={src}")
            except Exception as e:
                errs.append(f"verify: fact check exception: {e}")

        # Report proof snippets
        try:
            required_snippets = [
                f"state.sha256: {bundle['sources']['state']['sha256']}",
                f"bundle.sha256: {bundle['bundle_sha256']}",
            ]
            for s in required_snippets:
                if s not in report:
                    errs.append(f"verify: report missing proof snippet: {s}")
        except Exception as e:
            errs.append(f"verify: report proof check exception: {e}")

        # Views sanity (4S/4R): basic non-overlap + ordering + side correctness
        def _norm_range(r: Any) -> Optional[tuple]:
            if not isinstance(r, (list, tuple)) or len(r) != 2:
                return None
            a = float(r[0])
            b = float(r[1])
            return (a, b) if a <= b else (b, a)

        def _ov(a: tuple, b: tuple, eps: float) -> bool:
            # True only for a real overlap; touching is allowed.
            return max(a[0], b[0]) < (min(a[1], b[1]) - eps)

        def _check_side(sym_key: str, side: str, price: float, atr_h4: float, items: List[Dict[str, Any]]) -> None:
            eps = max(atr_h4 * 1e-3, price * 1e-6, 1e-9)

            if side == "S":
                want = sorted(items, key=lambda x: _norm_range(x.get("core"))[1] if _norm_range(x.get("core")) else 0.0, reverse=True)
            else:
                want = sorted(items, key=lambda x: _norm_range(x.get("core"))[0] if _norm_range(x.get("core")) else 0.0)
            if items != want:
                errs.append(f"verify: {sym_key}: {side} ordering invalid (closest first)")

            for it in items:
                core = _norm_range(it.get("core"))
                buf = _norm_range(it.get("buffer"))
                if core is None or buf is None:
                    errs.append(f"verify: {sym_key}: bad core/buffer format: {it}")
                    continue
                if not (buf[0] - eps <= core[0] <= core[1] <= buf[1] + eps):
                    errs.append(f"verify: {sym_key}: core must be inside buffer side={side} core={core} buf={buf}")

                if side == "S":
                    if core[1] > price + eps:
                        errs.append(f"verify: {sym_key}: support core above price core={core} price={price}")
                else:
                    if core[0] < price - eps:
                        errs.append(f"verify: {sym_key}: resistance core below price core={core} price={price}")

            for i in range(len(items)):
                for j in range(i + 1, len(items)):
                    ci = _norm_range(items[i].get("core"))
                    cj = _norm_range(items[j].get("core"))
                    if ci and cj and _ov(ci, cj, eps):
                        errs.append(f"verify: {sym_key}: overlapping CORES {items[i].get('name')} {ci} vs {items[j].get('name')} {cj}")
                    bi = _norm_range(items[i].get("buffer"))
                    bj = _norm_range(items[j].get("buffer"))
                    if bi and bj and _ov(bi, bj, eps):
                        # BUFFER overlap is informational (buffers are padding). CORE overlap is still an error.
                        warns.append(f"verify: {sym_key}: overlapping BUFFERS {items[i].get('name')} {bi} vs {items[j].get('name')} {bj}")

        def _check_sym(sym_key: str, prefix: str) -> None:
            v = (bundle.get("views") or {}).get(sym_key) or {}
            if "error" in v:
                errs.append(f"verify: {sym_key}: views error: {v.get('error')}")
                return
            price = float((bundle.get("facts_index") or {}).get(f"{prefix}.price") or 0.0)
            atr_h4 = float((bundle.get("facts_index") or {}).get(f"{prefix}.atr.h4") or 0.0)
            if price <= 0 or atr_h4 < 0:
                errs.append(f"verify: {sym_key}: bad price/atr in facts_index: price={price} atr_h4={atr_h4}")
                return
            s_items = v.get("supports") or []
            r_items = v.get("resistances") or []
            if not s_items or not r_items:
                errs.append(f"verify: {sym_key}: missing supports/resistances in views")
                return
            _check_side(sym_key, "S", price, atr_h4, s_items)
            _check_side(sym_key, "R", price, atr_h4, r_items)

        _check_sym("BTCUSDT", "btc")
        _check_sym("ETHUSDT", "eth")

        status["verify"]["ran"] = True
        status["verify"]["errors"] = errs
        status["verify"]["warnings"] = warns

        if errs:
            status["errors"].extend(errs)

    # --- build candidate (best-effort) ---
    contract = None
    state = None
    state_path: Optional[Path] = None

    if not CONTRACT_PATH.exists():
        status["errors"].append(f"Missing contract: {CONTRACT_PATH}")
    else:
        try:
            contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            status["errors"].append(f"Contract read/parse error: {e}")

    if contract:
        try:
            state_path = Path(contract["inputs"]["state_path"])
        except Exception as e:
            status["errors"].append(f"Contract missing inputs.state_path: {e}")

    if state_path:
        if not state_path.exists():
            status["errors"].append(f"Missing state input: {state_path} (run scripts/build_ta_state.py first)")
        else:
            try:
                state = read_json(state_path)
            except Exception as e:
                status["errors"].append(f"State read/parse error: {e}")

    candidate_bundle: Optional[Dict[str, Any]] = None
    candidate_report: Optional[str] = None
    candidate_bundle_sha: Optional[str] = None
    candidate_report_sha: Optional[str] = None

    if contract and state and state_path:
        try:
            deriv: Dict[str, Any] = {}

            facts = extract_facts(contract, state, deriv)
            fx = facts_index(facts)

            state_sha = sha256_file(state_path)

            candidate_bundle = {
                "schema": "iron.chat_bundle.v3",
                "generated_utc": generated_utc,
                "handshake_links": HANDSHAKE_LINKS,
                "sources": {
                    "state": {
                        "path": str(state_path),
                        "sha256": state_sha,
                        "updated_utc": state.get("updated_utc"),
                        "source": state.get("source"),
                    },
                    "contract": {
                        "path": str(CONTRACT_PATH),
                        "sha256": sha256_file(CONTRACT_PATH),
                    },
                },
                "facts": facts,
                "facts_index": fx,
            }

            # Views/levels from FULL state
            candidate_bundle["views"] = build_views_v2(state)

            status["sources"] = candidate_bundle.get("sources", {})

            # Aggregate view issues into status
            for sym, v in (candidate_bundle.get("views") or {}).items():
                if isinstance(v, dict):
                    if "error" in v:
                        status["errors"].append(f"{sym}: {v.get('error')}")
                    iss = v.get("issues") or {}
                    for w in (iss.get("warnings") or []):
                        status["warnings"].append(f"{sym}: {w}")
                    for e in (iss.get("errors") or []):
                        status["errors"].append(f"{sym}: {e}")

            # compute sha for bundle (without self hashes first)
            tmp = dict(candidate_bundle)
            tmp.pop("bundle_sha256", None)
            tmp.pop("report_sha256", None)
            bundle_bytes = json.dumps(tmp, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
            candidate_bundle_sha = sha256_bytes(bundle_bytes)
            candidate_bundle["bundle_sha256"] = candidate_bundle_sha

            # Render report and compute sha
            candidate_report = render_report(candidate_bundle)
            candidate_report_sha = sha256_bytes(candidate_report.encode("utf-8"))
            candidate_bundle["report_sha256"] = candidate_report_sha

            # Self-verify candidate before deciding publish target
            _verify_candidate(contract, candidate_bundle, candidate_report)

        except Exception as e:
            status["errors"].append(f"Build exception: {e}")

    _set_quality()

    # Decide publish target per root (docs/)
    for root in OUT_ROOTS:
        has_last_good = (root / rel_bundle_latest).exists() and (root / rel_report_latest).exists()

        if candidate_bundle is None or candidate_report is None or candidate_bundle_sha is None or candidate_report_sha is None:
            # Could not build candidate: keep last good, just publish status
            status["kept_last_good"] = bool(has_last_good)
            status["candidate"] = None
            if has_last_good:
                status["published"] = {
                    "bundle_rel": str(rel_bundle_latest),
                    "report_rel": str(rel_report_latest),
                    "bundle_sha256": sha256_file(root / rel_bundle_latest),
                    "report_sha256": sha256_bytes((root / rel_report_latest).read_text(encoding="utf-8").encode("utf-8")),
                }
            else:
                status["published"] = None

            write_json(root / rel_status, status)
            continue

        if status["quality"] == "FAIL":
            # Write BAD candidate, keep latest untouched
            status["kept_last_good"] = bool(has_last_good)
            status["candidate"] = {
                "bundle_rel": str(rel_bundle_bad),
                "report_rel": str(rel_report_bad),
                "bundle_sha256": candidate_bundle_sha,
                "report_sha256": candidate_report_sha,
            }
            if has_last_good:
                status["published"] = {
                    "bundle_rel": str(rel_bundle_latest),
                    "report_rel": str(rel_report_latest),
                    "bundle_sha256": sha256_file(root / rel_bundle_latest),
                    "report_sha256": sha256_bytes((root / rel_report_latest).read_text(encoding="utf-8").encode("utf-8")),
                }
            else:
                status["published"] = None

            write_json(root / rel_bundle_bad, candidate_bundle)
            write_text(root / rel_report_bad, candidate_report)
            write_text(root / rel_bundle_bad_sha, f"{candidate_bundle_sha}  {rel_bundle_bad.name}\n")
            write_text(root / rel_report_bad_sha, f"{candidate_report_sha}  {rel_report_bad.name}\n")

        else:
            # OK/WARN -> publish as latest
            status["kept_last_good"] = False
            status["candidate"] = {
                "bundle_rel": str(rel_bundle_latest),
                "report_rel": str(rel_report_latest),
                "bundle_sha256": candidate_bundle_sha,
                "report_sha256": candidate_report_sha,
            }
            status["published"] = dict(status["candidate"])

            write_json(root / rel_bundle_latest, candidate_bundle)
            write_text(root / rel_report_latest, candidate_report)
            write_text(root / rel_bundle_latest_sha, f"{candidate_bundle_sha}  {rel_bundle_latest.name}\n")
            write_text(root / rel_report_latest_sha, f"{candidate_report_sha}  {rel_report_latest.name}\n")

        # Always write status (last)
        write_json(root / rel_status, status)


if __name__ == "__main__":
    main()