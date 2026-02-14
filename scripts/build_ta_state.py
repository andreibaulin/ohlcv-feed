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
  ta/binance/state_btc_eth_latest.json          (SWING profile, default)
  ta/binance/state_btc_eth_full_latest.json     (FULL profile: includes H4 local zones)
  docs/ta/binance/state_btc_eth_latest.json
  docs/ta/binance/state_btc_eth_full_latest.json
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

# ---------------- SWING (D1) PROFILE ----------------
# Цель: разреженные, действительно "свинговые" зоны (без H4-шума).
# Дистанция между центрами зон задаётся через ATR(D1).
SWING_MAX_ZONES_PER_SIDE = 2
SWING_MIN_GAP_ATR_D1 = 1.8
SWING_MIN_STRENGTH = 2

# ---------------- MOVING AVERAGES ----------------
# Минимум "зоопарка": EMA200 на D1 и W1 как режимный фильтр.
EMA_PERIOD = 200

# ---------------- LOCAL (H4) ZONES ----------------
# Локальные уровни: ближайшие H4 зоны для входов/частичных; не обязаны совпадать со структурой.
PIVOT_W_LOCAL_H4 = 2
LOCAL_PIVOTS_LIMIT = 220   # сколько последних пивотов учитывать (сдерживаем шум)
LOCAL_RANGE_LOOKBACK = 60  # границы локального диапазона (H4)

# Толеранс слияния локальных уровней: max(% от цены, k*ATR)
LOCAL_MERGE_PCT = 0.0035     # 0.35%
LOCAL_MERGE_ATR_K = 0.60

# Полу-ширина локальной зоны: max(% от цены, k*ATR)
LOCAL_ZONE_PCT = 0.0020      # 0.20%
LOCAL_ZONE_ATR_K = 0.45

# Мягкий «зазор», чтобы локальная зона не совпала со структурной (если есть место)
LOCAL_AVOID_STRUCT_ATR_K = 0.15

# Локальную зону оцениваем на этом окне (H4)
LOCAL_STATS_LOOKBACK = 420


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


def ema(values: List[float], period: int) -> List[float]:
    """Return EMA series (same length as input). Uses standard alpha=2/(n+1)."""
    if not values:
        return []
    if period <= 1:
        return [float(x) for x in values]
    alpha = 2.0 / float(period + 1)
    out: List[float] = []
    e = float(values[0])
    out.append(e)
    for x in values[1:]:
        e = alpha * float(x) + (1.0 - alpha) * e
        out.append(e)
    return out


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


def cluster_levels_span(levels: List[Tuple[int, float]], merge_tol: float) -> List[Dict[str, Any]]:
    """Cluster levels by price distance, keep min/max span too (useful for local H4 zones)."""
    if not levels:
        return []
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
        out.append(
            {
                "center": center,
                "count": len(c),
                "last_idx": last_idx,
                "min": float(min(prices)),
                "max": float(max(prices)),
            }
        )
    return out


def zones_overlap(a: List[float], b: List[float]) -> bool:
    return not (a[1] < b[0] or b[1] < a[0])


def pick_best_by_strength(zones: List[Dict[str, Any]], price: float, side: str) -> Optional[Dict[str, Any]]:
    """Pick ONE zone on the correct side of price, prefer strength then distance."""
    cand: List[Tuple[int, float, int, Dict[str, Any]]] = []
    for z in zones:
        lo, hi = float(z["zone"][0]), float(z["zone"][1])
        if side == "S":
            if hi > price:
                continue
            dist = price - hi
        else:
            if lo < price:
                continue
            dist = lo - price
        tf = z.get("tf") or ""
        tf_rank = 0 if tf == "D1" else 1  # при равных — D1 чуть практичнее, W1 как 2-й приоритет
        cand.append((-int(z.get("strength", 1)), float(dist), tf_rank, z))
    cand.sort(key=lambda x: (x[0], x[1], x[2]))
    return cand[0][3] if cand else None


def build_local_h4_candidates(s_h4: Series, price: float, atr_h4: float) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (supports, resistances) candidates on H4."""
    if atr_h4 <= 0:
        return [], []

    merge_tol = max(LOCAL_MERGE_PCT * price, LOCAL_MERGE_ATR_K * atr_h4)
    zone_half = max(LOCAL_ZONE_PCT * price, LOCAL_ZONE_ATR_K * atr_h4)

    # pivots
    ph, pl = pivots(s_h4, PIVOT_W_LOCAL_H4)
    ph_use = ph[-LOCAL_PIVOTS_LIMIT:]
    pl_use = pl[-LOCAL_PIVOTS_LIMIT:]

    # range bounds
    lb = max(10, min(LOCAL_RANGE_LOOKBACK, len(s_h4.c)))
    rh = max(s_h4.h[-lb:])
    rl = min(s_h4.l[-lb:])
    last_i = len(s_h4.c) - 1

    r_lvls = list(ph_use) + [(last_i, float(rh))]
    s_lvls = list(pl_use) + [(last_i, float(rl))]

    def mk(side: str, levels: List[Tuple[int, float]]) -> List[Dict[str, Any]]:
        clusters = cluster_levels_span(levels, merge_tol)
        out: List[Dict[str, Any]] = []
        for cl in clusters:
            center = float(cl["center"])
            # span-aware zone: min/max +/- half
            lo = float(cl["min"]) - zone_half
            hi = float(cl["max"]) + zone_half
            if lo > hi:
                lo, hi = hi, lo
            z = [round(lo, 2), round(hi, 2)]
            st = zone_stats(s_h4, (lo, hi), side, lookback=min(len(s_h4.c), LOCAL_STATS_LOOKBACK))

            basis: List[str] = ["pivot"]
            if side == "R" and abs(rh - center) <= merge_tol:
                basis.append("range")
            if side == "S" and abs(rl - center) <= merge_tol:
                basis.append("range")

            out.append(
                {
                    "tf": "H4",
                    "side": side,
                    "center": round(center, 2),
                    "zone": z,
                    "cluster_count": int(cl["count"]),
                    "touches": st["touches"],
                    "rejections": st["rejections"],
                    "last_touch_utc": st["last_touch_utc"],
                    "age_bars": st["age_bars"],
                    "strength": st["strength"],
                    "score": st["score"],
                    "basis": basis,
                }
            )

        # sort: nearest-to-price on the correct side first, then strength/score
        def _dist(z: Dict[str, Any]) -> float:
            lo_, hi_ = float(z["zone"][0]), float(z["zone"][1])
            if side == "S":
                return (price - hi_) if hi_ <= price else 1e18
            return (lo_ - price) if lo_ >= price else 1e18

        out.sort(key=lambda x: (_dist(x), -int(x.get("strength", 1)), -int(x.get("score", 0))))
        return out

    supports = mk("S", s_lvls)
    resistances = mk("R", r_lvls)
    return supports, resistances


def select_local_zone(
    cands: List[Dict[str, Any]],
    price: float,
    side: str,
    structural_zone: Optional[List[float]],
    atr_h4: float,
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """Select one local zone below/above price; try to avoid overlapping structural zone."""
    if not cands:
        return None, False

    gap = max(0.0, LOCAL_AVOID_STRUCT_ATR_K * atr_h4)

    def ok_side(z: Dict[str, Any]) -> bool:
        lo, hi = float(z["zone"][0]), float(z["zone"][1])
        if side == "S":
            return hi <= price
        return lo >= price

    def ok_avoid(z: Dict[str, Any]) -> bool:
        if structural_zone is None:
            return True
        slo, shi = float(structural_zone[0]), float(structural_zone[1])
        lo, hi = float(z["zone"][0]), float(z["zone"][1])
        if side == "S":
            # хотим локальную поддержку ВЫШЕ структурной (если есть место)
            return hi > (shi + gap)
        # хотим локальное сопротивление НИЖЕ структурного
        return lo < (slo - gap)

    # 1) строгий вариант: правильная сторона + избегаем структурной
    for z in cands:
        if ok_side(z) and ok_avoid(z):
            confl = structural_zone is not None and zones_overlap(list(z["zone"]), structural_zone)
            return z, confl

    # 2) если не нашли — берём лучшее по силе на правильной стороне, даже если совпадает
    for z in cands:
        if ok_side(z):
            confl = structural_zone is not None and zones_overlap(list(z["zone"]), structural_zone)
            return z, confl

    return None, False


def pick_local_selected(
    cands: List[Dict[str, Any]],
    price: float,
    side: str,
    atr_h4: float,
    structural_zone: Optional[List[float]],
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """Pick one local zone; try to avoid overlapping/near-equal with structural if possible.

    Returns (selected, confluence_with_structural).
    """
    if not cands:
        return None, False

    gap = max(0.0, LOCAL_AVOID_STRUCT_ATR_K * atr_h4)

    def ok_side(z: Dict[str, Any]) -> bool:
        lo, hi = float(z["zone"][0]), float(z["zone"][1])
        return (hi <= price) if side == "S" else (lo >= price)

    def ok_avoid(z: Dict[str, Any]) -> bool:
        if not structural_zone:
            return True
        lo, hi = float(z["zone"][0]), float(z["zone"][1])
        slo, shi = float(structural_zone[0]), float(structural_zone[1])
        if side == "S":
            # хотим локальную поддержку ВЫШЕ структурной поддержки
            return hi > (shi + gap)
        else:
            # хотим локальное сопротивление НИЖЕ структурного сопротивления
            return lo < (slo - gap)

    # 1) строгий выбор (правильная сторона + избегаем структуры)
    best: Optional[Dict[str, Any]] = None
    for z in cands:
        if not ok_side(z):
            continue
        if not ok_avoid(z):
            continue
        best = z
        break

    # 2) fallback: правильная сторона без avoid
    if best is None:
        for z in cands:
            if ok_side(z):
                best = z
                break

    if best is None:
        return None, False

    confl = False
    if structural_zone and zones_overlap(best["zone"], structural_zone):
        confl = True
    return best, confl


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


def pick_w1_bracket(zones_w1: List[Dict[str, Any]], price: float) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Pick nearest/strong W1 support below and resistance above to form a macro range bracket."""
    z_s = [z for z in zones_w1 if z.get("side") == "S"]
    z_r = [z for z in zones_w1 if z.get("side") == "R"]
    sup = pick_best_by_strength(z_s, price, "S")
    res = pick_best_by_strength(z_r, price, "R")
    return sup, res


def nms_swing_zones(
    zones: List[Dict[str, Any]],
    price: float,
    side: str,
    atr_d1: float,
    max_n: int = SWING_MAX_ZONES_PER_SIDE,
    min_strength: int = SWING_MIN_STRENGTH,
    min_gap_atr: float = SWING_MIN_GAP_ATR_D1,
) -> List[Dict[str, Any]]:
    """Non-max suppression for swing zones: keep only strong zones, spaced by k*ATR(D1)."""
    if atr_d1 <= 0:
        return []

    min_gap = float(min_gap_atr) * float(atr_d1)

    def _dist(z: Dict[str, Any]) -> float:
        lo, hi = float(z["zone"][0]), float(z["zone"][1])
        if side == "S":
            return (price - hi) if hi <= price else 1e18
        return (lo - price) if lo >= price else 1e18

    cands: List[Dict[str, Any]] = []
    for z in zones:
        if int(z.get("strength", 1)) < int(min_strength):
            continue
        if _dist(z) >= 1e18:
            continue
        cands.append(z)

    # rank: strength, score, then closeness
    cands.sort(key=lambda z: (-int(z.get("strength", 1)), -int(z.get("score", 0)), _dist(z)))

    picked: List[Dict[str, Any]] = []
    for z in cands:
        if len(picked) >= int(max_n):
            break
        cz = float(z.get("center", 0.0))
        ok = True
        for p in picked:
            cp = float(p.get("center", 0.0))
            if abs(cz - cp) < min_gap:
                ok = False
                break
        if ok:
            picked.append(z)

    # If we picked nothing (rare), relax strength filter
    if not picked and zones:
        cands2 = [z for z in zones if _dist(z) < 1e18]
        cands2.sort(key=lambda z: (-int(z.get("strength", 1)), -int(z.get("score", 0)), _dist(z)))
        for z in cands2:
            if len(picked) >= int(max_n):
                break
            picked.append(z)

    # final order: nearest to price
    picked.sort(key=lambda z: _dist(z))
    return picked


def build_symbol_state(base_dir: Path, symbol: str) -> Dict[str, Any]:
    # Load series
    s_h4 = to_series(load_rows_from_chunks(base_dir, symbol, "H4", TAIL_N["H4"]))
    s_d1 = to_series(load_rows_from_chunks(base_dir, symbol, "D1", TAIL_N["D1"]))
    s_w1 = to_series(load_rows_from_chunks(base_dir, symbol, "W1", TAIL_N["W1"]))

    price = float(s_h4.c[-1])
    atr_h4 = atr14(s_h4)
    atr_d1 = atr14(s_d1)
    atr_w1 = atr14(s_w1)

    # Moving averages (EMA200) as a regime filter (no indicator zoo)
    ema_d1 = None
    ema_w1 = None
    ema_d1_dir = None
    ema_w1_dir = None
    ema_d1_pos = None
    ema_w1_pos = None

    e_d1 = ema([float(x) for x in s_d1.c], EMA_PERIOD)
    if e_d1:
        ema_d1 = float(e_d1[-1])
        back = e_d1[-6] if len(e_d1) >= 6 else e_d1[0]
        slope = ema_d1 - float(back)
        flat_eps = max(1e-9, 0.0010 * ema_d1)  # 0.10%
        ema_d1_dir = "flat" if abs(slope) <= flat_eps else ("up" if slope > 0 else "down")
        rel = abs(price - ema_d1) / ema_d1 if ema_d1 > 0 else 0.0
        ema_d1_pos = "near" if rel <= 0.0020 else ("above" if price > ema_d1 else "below")

    e_w1 = ema([float(x) for x in s_w1.c], EMA_PERIOD)
    if e_w1:
        ema_w1 = float(e_w1[-1])
        back = e_w1[-3] if len(e_w1) >= 3 else e_w1[0]
        slope = ema_w1 - float(back)
        flat_eps = max(1e-9, 0.0010 * ema_w1)
        ema_w1_dir = "flat" if abs(slope) <= flat_eps else ("up" if slope > 0 else "down")
        rel = abs(price - ema_w1) / ema_w1 if ema_w1 > 0 else 0.0
        ema_w1_pos = "near" if rel <= 0.0020 else ("above" if price > ema_w1 else "below")

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
                        "score": st["score"],
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

    # Selected structural zones: 1 support + 1 resistance (сильные, на своей стороне)
    sel_struct_s = pick_best_by_strength(zones_s, price, "S")
    sel_struct_r = pick_best_by_strength(zones_r, price, "R")

    # -------- SWING (D1) zones (default profile) --------
    zones_d1 = [z for z in zones_all if z.get("tf") == "D1"]
    d1_s = [z for z in zones_d1 if z.get("side") == "S"]
    d1_r = [z for z in zones_d1 if z.get("side") == "R"]

    swing_s = nms_swing_zones(d1_s, price, "S", atr_d1, max_n=SWING_MAX_ZONES_PER_SIDE)
    swing_r = nms_swing_zones(d1_r, price, "R", atr_d1, max_n=SWING_MAX_ZONES_PER_SIDE)
    swing_sel_s = swing_s[0] if swing_s else None
    swing_sel_r = swing_r[0] if swing_r else None

    # -------- RANGE bracket from W1 zones --------
    zones_w1 = [z for z in zones_all if z.get("tf") == "W1"]
    w1_sup, w1_res = pick_w1_bracket(zones_w1, price)
    range_obj: Dict[str, Any] = {
        "tf": "W1",
        "support": w1_sup,
        "resistance": w1_res,
        "discount_edge": None,
        "premium_edge": None,
        "mid": None,
        "corridor": None,
        "price_location": None,
    }
    if isinstance(w1_sup, dict) and w1_sup.get("zone"):
        range_obj["discount_edge"] = float(w1_sup["zone"][1])
    if isinstance(w1_res, dict) and w1_res.get("zone"):
        range_obj["premium_edge"] = float(w1_res["zone"][0])
    de = range_obj.get("discount_edge")
    pe = range_obj.get("premium_edge")
    if isinstance(de, (int, float)) and isinstance(pe, (int, float)) and float(de) < float(pe):
        range_obj["mid"] = round((float(de) + float(pe)) / 2.0, 2)
        range_obj["corridor"] = [round(float(de), 2), round(float(pe), 2)]
        # Where price is relative to corridor
        if price < float(de):
            range_obj["price_location"] = "below_discount"
        elif price > float(pe):
            range_obj["price_location"] = "above_premium"
        else:
            # inside
            pct = (price - float(de)) / (float(pe) - float(de))
            if pct <= 0.35:
                range_obj["price_location"] = "discount"
            elif pct >= 0.65:
                range_obj["price_location"] = "premium"
            else:
                range_obj["price_location"] = "mid"

    struct_s_zone = list(sel_struct_s["zone"]) if isinstance(sel_struct_s, dict) and sel_struct_s.get("zone") else None
    struct_r_zone = list(sel_struct_r["zone"]) if isinstance(sel_struct_r, dict) and sel_struct_r.get("zone") else None

    # Local H4 zones (for entries/partials)
    local_s_cands, local_r_cands = build_local_h4_candidates(s_h4, price, atr_h4)
    sel_local_s, local_s_confl = select_local_zone(local_s_cands, price, "S", struct_s_zone, atr_h4)
    sel_local_r, local_r_confl = select_local_zone(local_r_cands, price, "R", struct_r_zone, atr_h4)

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

    # Forecast-4 zones mapping (compatible with "S1/S2/R1/R2")
    forecast_4 = {
        "S1_local_h4": sel_local_s,
        "S2_structural": sel_struct_s,
        "R1_local_h4": sel_local_r,
        "R2_structural": sel_struct_r,
        "confluence": {
            "S1_with_S2": bool(local_s_confl),
            "R1_with_R2": bool(local_r_confl),
        },
    }

    return {
        "data": {
            "bars": {"H4": len(s_h4.c), "D1": len(s_d1.c), "W1": len(s_w1.c)},
            "price": round(price, 2),
            "last_close_utc_h4": last_close_utc_h4,
        },
        "ma": {
            "ema200": {
                "D1": round(float(ema_d1), 2) if ema_d1 is not None else None,
                "W1": round(float(ema_w1), 2) if ema_w1 is not None else None,
            },
            "ema200_dir": {"D1": ema_d1_dir, "W1": ema_w1_dir},
            "price_vs_ema200": {"D1": ema_d1_pos, "W1": ema_w1_pos},
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
                "selected": {
                    "support": sel_struct_s,
                    "resistance": sel_struct_r,
                },
            },
            "range_w1": range_obj,
            "swing_d1": {
                "supports": swing_s,
                "resistances": swing_r,
                "selected": {"support": swing_sel_s, "resistance": swing_sel_r},
                "params": {
                    "max_zones_per_side": SWING_MAX_ZONES_PER_SIDE,
                    "min_gap_atr_d1": SWING_MIN_GAP_ATR_D1,
                    "min_strength": SWING_MIN_STRENGTH,
                },
            },
            "working_h4": working,
            "local_h4": {
                "supports": local_s_cands[:6],
                "resistances": local_r_cands[:6],
                "selected": {
                    "support": sel_local_s,
                    "resistance": sel_local_r,
                },
            },
            "forecast_4": forecast_4,
        },
    }


def main() -> None:
    updated_utc = utc_now_iso()
    symbols = parse_symbols_env()

    # Prefer root data; if missing, fallback to docs
    base_dir_root = Path("ohlcv") / "binance"
    base_dir_docs = Path("docs") / "ohlcv" / "binance"
    base_dir = base_dir_root if base_dir_root.exists() else base_dir_docs

    out_full: Dict[str, Any] = {
        "updated_utc": updated_utc,
        "profile": "full",
        "source": "ta_state_pivots_atr",
        "notes": {
            "what": "Conservative zones from W1/D1 pivots clustered and expanded by ATR; NOT a forecast.",
            "profiles": {
                "swing": "Default. D1 swing zones (spaced by k*ATR(D1)) + W1 range bracket + EMA200 filter.",
                "full": "Includes everything from swing + local H4 zones and working H4 buffer.",
            },
            "regime": "trend/range/chop is a risk filter; chop/high-vol => prefer WAIT.",
        },
        "symbols": {},
    }

    for sym in symbols:
        try:
            out_full["symbols"][sym] = build_symbol_state(base_dir, sym)
        except Exception as e:
            out_full["symbols"][sym] = {"error": str(e)}

    # Derive SWING output from FULL
    out_swing: Dict[str, Any] = {
        "updated_utc": updated_utc,
        "profile": "swing",
        "source": "ta_state_pivots_atr",
        "notes": out_full.get("notes"),
        "symbols": {},
    }

    for sym, payload in out_full.get("symbols", {}).items():
        if not isinstance(payload, dict) or payload.get("error") is not None:
            out_swing["symbols"][sym] = payload
            continue

        zones = payload.get("zones") or {}
        swing_d1 = zones.get("swing_d1")
        range_w1 = zones.get("range_w1")
        sel = (swing_d1 or {}).get("selected") if isinstance(swing_d1, dict) else None
        forecast_swing = {
            "S1_swing_d1": (sel or {}).get("support") if isinstance(sel, dict) else None,
            "R1_swing_d1": (sel or {}).get("resistance") if isinstance(sel, dict) else None,
            "range_w1": range_w1,
        }

        out_swing["symbols"][sym] = {
            "data": payload.get("data"),
            "ma": payload.get("ma"),
            "vol": payload.get("vol"),
            "structure": payload.get("structure"),
            "zones": {
                "range_w1": range_w1,
                "swing_d1": swing_d1,
                "forecast_swing": forecast_swing,
            },
        }

    for root in OUT_ROOTS:
        d = root / "ta" / "binance"
        d.mkdir(parents=True, exist_ok=True)

        # Default: SWING
        write_json_compact(d / "state_btc_eth_latest.json", out_swing)
        # Optional: FULL (H4 locals)
        write_json_compact(d / "state_btc_eth_full_latest.json", out_full)


if __name__ == "__main__":
    main()
