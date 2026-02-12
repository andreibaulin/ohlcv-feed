#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""build_ta_state.py

Консервативный (надёжный) генератор уровней/зон для BTCUSDT/ETHUSDT.

Идея: НЕ "прогноз", а воспроизводимый слепок структуры/зон:
- Структура: W1 + D1 (пивоты/свинг)
- Зоны: кластера пивотов -> диапазоны шириной k*ATR(TF)
- Режим: trend / range / chop (по пивотам и воле)

Вход: локальные артефакты из scripts/gen_pack_btc_eth.py
  ohlcv/binance/{SYMBOL}_{TF}_tail{N}_chunks.json + parts p###.json

Выход:
  ta/binance/state_btc_eth_latest.json
  docs/ta/binance/state_btc_eth_latest.json
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Tuple


OUT_ROOTS = [Path("."), Path("docs")]
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]

TFS_STRUCT = ["W1", "D1"]
TF_WORK = "H4"

TAIL_N = {"H4": 1500, "D1": 2000, "W1": 520}

PIVOT_W = {"W1": 2, "D1": 2}  # окно пивота (слева/справа)

# Консервативные коэффициенты
ATR_PERIOD = 14
MERGE_K = {"W1": 0.40, "D1": 0.35}   # порог слияния уровней (в ATR)
ZONE_K = {"W1": 0.85, "D1": 0.65}    # полу-ширина зоны (в ATR)
WORK_K = 0.75                           # полу-ширина рабочей зоны (в ATR_H4)

# Сколько зон отдаём (минимум шума)
MAX_STRUCT_ZONES_PER_SIDE = 3


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def write_json_compact(path: Path, obj: Any) -> None:
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")


def parse_symbols_env() -> List[str]:
    raw = os.environ.get("TA_SYMBOLS", "").strip()
    if not raw:
        return DEFAULT_SYMBOLS
    out: List[str] = []
    for x in raw.split(","):
        s = x.strip().upper()
        if s:
            out.append(s)
    return out or DEFAULT_SYMBOLS


def load_rows_from_chunks(base_dir: Path, symbol: str, tf: str, tail_n: int) -> List[List[Any]]:
    """Read rows from *_chunks.json manifest and its p### parts."""
    manifest = base_dir / f"{symbol}_{tf}_tail{tail_n}_chunks.json"
    if not manifest.exists():
        raise FileNotFoundError(str(manifest))

    man = json.loads(manifest.read_text(encoding="utf-8"))
    parts = man.get("parts") or []
    rows: List[List[Any]] = []
    for p in parts:
        fn = p.get("file")
        if not fn:
            continue
        pp = base_dir / fn
        part_rows = json.loads(pp.read_text(encoding="utf-8"))
        if isinstance(part_rows, list):
            rows.extend(part_rows)
    return rows


@dataclass
class Series:
    t: List[int]
    o: List[float]
    h: List[float]
    l: List[float]
    c: List[float]
    v: List[float]
    ct: List[int]


def to_series(rows: List[List[Any]]) -> Series:
    t: List[int] = []
    o: List[float] = []
    h: List[float] = []
    l: List[float] = []
    c: List[float] = []
    v: List[float] = []
    ct: List[int] = []
    for r in rows:
        if not isinstance(r, list) or len(r) < 7:
            continue
        try:
            t.append(int(r[0]))
            o.append(float(r[1]))
            h.append(float(r[2]))
            l.append(float(r[3]))
            c.append(float(r[4]))
            v.append(float(r[5]))
            ct.append(int(r[6]))
        except Exception:
            continue
    if not t:
        raise ValueError("empty series")
    return Series(t=t, o=o, h=h, l=l, c=c, v=v, ct=ct)


def atr14(s: Series, period: int = ATR_PERIOD) -> float:
    if len(s.c) < 2:
        return 0.0
    trs: List[float] = []
    for i in range(1, len(s.c)):
        tr = max(
            s.h[i] - s.l[i],
            abs(s.h[i] - s.c[i - 1]),
            abs(s.l[i] - s.c[i - 1]),
        )
        trs.append(tr)
    if not trs:
        return 0.0
    if len(trs) >= period:
        window = trs[-period:]
        return sum(window) / float(period)
    return sum(trs) / float(len(trs))


def pivots(s: Series, w: int) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
    """Return pivot highs and pivot lows as (index, price)."""
    ph: List[Tuple[int, float]] = []
    pl: List[Tuple[int, float]] = []
    n = len(s.c)
    if n < (2 * w + 3):
        return ph, pl
    for i in range(w, n - w):
        hh = s.h[i]
        ll = s.l[i]
        left_h = s.h[i - w : i]
        right_h = s.h[i + 1 : i + w + 1]
        left_l = s.l[i - w : i]
        right_l = s.l[i + 1 : i + w + 1]
        if all(hh > x for x in left_h) and all(hh > x for x in right_h):
            ph.append((i, hh))
        if all(ll < x for x in left_l) and all(ll < x for x in right_l):
            pl.append((i, ll))
    return ph, pl


def infer_trend(ph: List[Tuple[int, float]], pl: List[Tuple[int, float]]) -> str:
    """Conservative trend inference by last two pivot highs/lows."""
    if len(ph) < 2 or len(pl) < 2:
        return "range"
    h1, h2 = ph[-2][1], ph[-1][1]
    l1, l2 = pl[-2][1], pl[-1][1]
    if h2 > h1 and l2 > l1:
        return "up"
    if h2 < h1 and l2 < l1:
        return "down"
    return "range"


def vol_flag(atr_d1: float, last_close: float) -> str:
    if last_close <= 0:
        return "unknown"
    rel = atr_d1 / last_close
    # Консервативные пороги (универсально для BTC/ETH на D1)
    if rel >= 0.04:
        return "high"
    if rel <= 0.015:
        return "low"
    return "normal"


def clamp_zone(center: float, half: float) -> Tuple[float, float]:
    lo = center - half
    hi = center + half
    if lo > hi:
        lo, hi = hi, lo
    return (lo, hi)


def cluster_levels(levels: List[Tuple[int, float]], merge_tol: float) -> List[Dict[str, Any]]:
    """Cluster levels by price distance, keep indices for recency."""
    if not levels:
        return []
    # sort by price
    lv = sorted(levels, key=lambda x: x[1])
    clusters: List[List[Tuple[int, float]]] = []
    for idx, price in lv:
        if not clusters:
            clusters.append([(idx, price)])
            continue
        cur = clusters[-1]
        center = median([p for _, p in cur])
        if abs(price - center) <= merge_tol:
            cur.append((idx, price))
        else:
            clusters.append([(idx, price)])

    out: List[Dict[str, Any]] = []
    for c in clusters:
        prices = [p for _, p in c]
        center = float(median(prices))
        last_idx = max(i for i, _ in c)
        out.append({"center": center, "count": len(c), "last_idx": last_idx})
    return out


def zone_stats(s: Series, zone: Tuple[float, float], side: str, lookback: int) -> Dict[str, Any]:
    lo, hi = zone
    start = max(0, len(s.c) - lookback)
    touches = 0
    rejects = 0
    last_touch_i: Optional[int] = None

    for i in range(start, len(s.c)):
        if s.l[i] <= hi and s.h[i] >= lo:
            touches += 1
            last_touch_i = i
            if side == "R":
                if s.c[i] < lo:
                    rejects += 1
            else:  # "S"
                if s.c[i] > hi:
                    rejects += 1

    if last_touch_i is None:
        age = None
        last_touch_utc = None
    else:
        age = (len(s.c) - 1) - last_touch_i
        last_touch_utc = datetime.fromtimestamp(s.ct[last_touch_i] / 1000, tz=timezone.utc).isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        )

    score = touches + 2 * rejects
    strength = int(min(5, max(1, 1 + (score // 4))))

    return {
        "touches": touches,
        "rejections": rejects,
        "last_touch_utc": last_touch_utc,
        "age_bars": age,
        "strength": strength,
        "score": score,
    }


def pick_nearest_zones(zones: List[Dict[str, Any]], price: float, side: str, max_n: int) -> List[Dict[str, Any]]:
    """Pick nearest zones below (S) or above (R) current price."""
    filt: List[Tuple[float, Dict[str, Any]]] = []
    for z in zones:
        lo, hi = z["zone"]
        if side == "S":
            if hi <= price:
                dist = price - hi
            else:
                continue
        else:
            if lo >= price:
                dist = lo - price
            else:
                continue
        filt.append((dist, z))
    filt.sort(key=lambda x: x[0])
    return [z for _, z in filt[:max_n]]


def build_symbol_state(base_dir: Path, symbol: str) -> Dict[str, Any]:
    # Load series
    s_h4 = to_series(load_rows_from_chunks(base_dir, symbol, "H4", TAIL_N["H4"]))
    s_d1 = to_series(load_rows_from_chunks(base_dir, symbol, "D1", TAIL_N["D1"]))
    s_w1 = to_series(load_rows_from_chunks(base_dir, symbol, "W1", TAIL_N["W1"]))

    price = float(s_h4.c[-1])
    atr_h4 = atr14(s_h4)
    atr_d1 = atr14(s_d1)
    atr_w1 = atr14(s_w1)

    # Pivots
    ph_w1, pl_w1 = pivots(s_w1, PIVOT_W["W1"])
    ph_d1, pl_d1 = pivots(s_d1, PIVOT_W["D1"])

    trend_w1 = infer_trend(ph_w1, pl_w1)
    trend_d1 = infer_trend(ph_d1, pl_d1)

    # BOS flags (close break of latest pivot)
    last_close_d1 = float(s_d1.c[-1])
    last_close_w1 = float(s_w1.c[-1])

    last_ph_d1 = ph_d1[-1][1] if ph_d1 else None
    last_pl_d1 = pl_d1[-1][1] if pl_d1 else None
    bos_up_d1 = bool(last_ph_d1 is not None and last_close_d1 > float(last_ph_d1))
    bos_dn_d1 = bool(last_pl_d1 is not None and last_close_d1 < float(last_pl_d1))

    last_ph_w1 = ph_w1[-1][1] if ph_w1 else None
    last_pl_w1 = pl_w1[-1][1] if pl_w1 else None
    bos_up_w1 = bool(last_ph_w1 is not None and last_close_w1 > float(last_ph_w1))
    bos_dn_w1 = bool(last_pl_w1 is not None and last_close_w1 < float(last_pl_w1))

    # Zones from pivots
    zones_all: List[Dict[str, Any]] = []

    def add_tf_zones(tf: str, s: Series, ph: List[Tuple[int, float]], pl: List[Tuple[int, float]], atr_tf: float) -> None:
        if atr_tf <= 0:
            return
        merge_tol = MERGE_K[tf] * atr_tf
        half = ZONE_K[tf] * atr_tf

        # take last N pivots (reduce noise)
        ph_use = ph[-40:]
        pl_use = pl[-40:]

        for side, levels in [("R", ph_use), ("S", pl_use)]:
            clusters = cluster_levels(levels, merge_tol)
            for cl in clusters:
                center = float(cl["center"])
                zone = clamp_zone(center, half)
                st = zone_stats(s, zone, side, lookback=min(len(s.c), 400 if tf == "D1" else 260))
                zones_all.append(
                    {
                        "tf": tf,
                        "side": side,
                        "center": center,
                        "zone": [round(zone[0], 2), round(zone[1], 2)],
                        "cluster_count": int(cl["count"]),
                        "strength": st["strength"],
                        "touches": st["touches"],
                        "rejections": st["rejections"],
                        "last_touch_utc": st["last_touch_utc"],
                    }
                )

    add_tf_zones("W1", s_w1, ph_w1, pl_w1, atr_w1)
    add_tf_zones("D1", s_d1, ph_d1, pl_d1, atr_d1)

    # Separate and pick nearest above/below
    zones_r = [z for z in zones_all if z["side"] == "R"]
    zones_s = [z for z in zones_all if z["side"] == "S"]
    # prefer higher strength, then closeness (we'll pick nearest first but filter strength)
    zones_r.sort(key=lambda z: (-z["strength"], z["zone"][0]))
    zones_s.sort(key=lambda z: (-z["strength"], -z["zone"][1]))

    # Nearest zones relative to current price
    nearest_r = pick_nearest_zones(zones_r, price, "R", MAX_STRUCT_ZONES_PER_SIDE)
    nearest_s = pick_nearest_zones(zones_s, price, "S", MAX_STRUCT_ZONES_PER_SIDE)

    # Build working zones (H4 buffer) from nearest structural zones
    work_half = max(0.0, WORK_K * atr_h4)
    working: List[Dict[str, Any]] = []
    for z in (nearest_s + nearest_r):
        center = float(z["center"])
        lo, hi = clamp_zone(center, work_half)
        working.append(
            {
                "side": z["side"],
                "from_tf": z["tf"],
                "center": center,
                "zone": [round(lo, 2), round(hi, 2)],
                "strength": z["strength"],
            }
        )

    # Regime flag
    vf = vol_flag(atr_d1, price)
    regime = "chop" if vf == "high" else ("trend" if trend_d1 in ("up", "down") else "range")

    last_close_utc_h4 = datetime.fromtimestamp(s_h4.ct[-1] / 1000, tz=timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )

    return {
        "data": {
            "bars": {"H4": len(s_h4.c), "D1": len(s_d1.c), "W1": len(s_w1.c)},
            "price": round(price, 2),
            "last_close_utc_h4": last_close_utc_h4,
        },
        "vol": {
            "atr14": {
                "H4": round(atr_h4, 2),
                "D1": round(atr_d1, 2),
                "W1": round(atr_w1, 2),
            },
            "vol_flag_d1": vf,
        },
        "structure": {
            "W1": {
                "trend": trend_w1,
                "last_swing_high": round(float(last_ph_w1), 2) if last_ph_w1 is not None else None,
                "last_swing_low": round(float(last_pl_w1), 2) if last_pl_w1 is not None else None,
                "close_break_up": bos_up_w1,
                "close_break_down": bos_dn_w1,
            },
            "D1": {
                "trend": trend_d1,
                "last_swing_high": round(float(last_ph_d1), 2) if last_ph_d1 is not None else None,
                "last_swing_low": round(float(last_pl_d1), 2) if last_pl_d1 is not None else None,
                "close_break_up": bos_up_d1,
                "close_break_down": bos_dn_d1,
            },
            "regime": regime,
        },
        "zones": {
            "structural": {
                "supports": nearest_s,
                "resistances": nearest_r,
            },
            "working_h4": working,
        },
    }


def main() -> None:
    updated_utc = utc_now_iso()
    symbols = parse_symbols_env()

    # Prefer root data; if missing, fallback to docs
    base_dir_root = Path("ohlcv") / "binance"
    base_dir_docs = Path("docs") / "ohlcv" / "binance"
    base_dir = base_dir_root if base_dir_root.exists() else base_dir_docs

    out: Dict[str, Any] = {
        "updated_utc": updated_utc,
        "source": "ta_state_pivots_atr",
        "notes": {
            "what": "Conservative zones from W1/D1 pivots clustered and expanded by ATR; NOT a forecast.",
            "regime": "trend/range/chop is a risk filter; chop/high-vol => prefer WAIT.",
        },
        "symbols": {},
    }

    for sym in symbols:
        try:
            out["symbols"][sym] = build_symbol_state(base_dir, sym)
        except Exception as e:
            out["symbols"][sym] = {"error": str(e)}

    for root in OUT_ROOTS:
        d = root / "ta" / "binance"
        d.mkdir(parents=True, exist_ok=True)
        write_json_compact(d / "state_btc_eth_latest.json", out)


if __name__ == "__main__":
    main()
