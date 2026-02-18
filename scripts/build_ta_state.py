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


OUT_ROOTS = [Path("docs")]
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

# ---------------- SWING / RANGE EXECUTION BANDS ----------------
# "Операционные полосы" (execution bands) — узкие зоны у края макро-зон, рассчитанные от волатильности.
# Это НЕ уровни "на глаз", а детерминированные полосы допуска под свинг/рендж.
EXEC_BAND_K_ATR_D1 = 0.50
EXEC_BAND_FALLBACK_ATR_H4_K = 2.0

# Ограничители ширины, чтобы полосы были практичными в $ (не раздувались в экстремальной воле)
# Можно менять без пересборки логики.
EXEC_BAND_MINMAX = {
    "BTCUSDT": (1500.0, 6000.0),
    "ETHUSDT": (60.0, 250.0),
}

# Метрика реакции зоны (по H4): сколько раз тестировали полосу и насколько часто был отбой
EXEC_BAND_LOOKBACK_H4 = 720     # ~120 дней H4
EXEC_REACT_FWD_H4 = 18          # окно реакции (~3 суток)
EXEC_REACT_THR_ATR_H4_K = 1.0   # порог реакции: >= 1×ATR(H4)
EXEC_FAIL_THR_ATR_H4_K = 0.5    # порог "пробоя": >= 0.5×ATR(H4) за край полосы



def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def write_json_pretty(path: Path, obj: Any) -> None:
    atomic_write_text(
        path,
        json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )


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



def ema_series(values: List[float], period: int) -> List[Optional[float]]:
    """EMA series; returns list aligned to input (None until enough bars)."""
    n = len(values)
    if n < period or period <= 1:
        return [None] * n
    alpha = 2.0 / (period + 1.0)
    out: List[Optional[float]] = [None] * n
    sma = sum(values[:period]) / float(period)
    ema = sma
    out[period - 1] = ema
    for i in range(period, n):
        ema = alpha * float(values[i]) + (1.0 - alpha) * ema
        out[i] = ema
    return out


def ema_last(values: List[float], period: int) -> Optional[float]:
    s = ema_series(values, period)
    return s[-1] if s else None


def slope_tag(curr: Optional[float], prev: Optional[float], eps: float) -> Optional[str]:
    if curr is None or prev is None:
        return None
    d = curr - prev
    if abs(d) <= eps:
        return "flat"
    return "up" if d > 0 else "down"


def clamp_f(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def exec_band_width(symbol: str, atr_d1: float, atr_h4: float) -> float:
    """Width of execution band in $; deterministic, volatility-based, with clamps."""
    if atr_d1 and atr_d1 > 0:
        w = EXEC_BAND_K_ATR_D1 * float(atr_d1)
    else:
        w = EXEC_BAND_FALLBACK_ATR_H4_K * float(atr_h4)
    mn, mx = EXEC_BAND_MINMAX.get(symbol, (0.0, 1e18))
    w = clamp_f(w, mn, mx)
    return float(round(w, 2))


def band_reaction_stats(
    s_h4: Series,
    band: Tuple[float, float],
    side: str,
    atr_h4: float,
    lookback: int = EXEC_BAND_LOOKBACK_H4,
    fwd: int = EXEC_REACT_FWD_H4,
    thr_k: float = EXEC_REACT_THR_ATR_H4_K,
    fail_k: float = EXEC_FAIL_THR_ATR_H4_K,
    now_dt: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Reaction stats for a band on H4.
    Test = candle overlaps band.
    Reaction(S) = within next fwd bars, move up >= thr_k*ATR(H4).
    Reaction(R) = within next fwd bars, move down >= thr_k*ATR(H4).
    Failure(S)  = within next fwd bars, min_low < band_low - fail_k*ATR(H4).
    Failure(R)  = within next fwd bars, max_high > band_high + fail_k*ATR(H4).
    """
    if atr_h4 <= 0 or len(s_h4.c) < 50:
        return {"tests": 0, "reactions": 0, "reaction_rate": None, "failures": 0, "failure_rate": None}

    lo, hi = float(band[0]), float(band[1])
    n = len(s_h4.c)
    start = max(0, n - lookback)
    end = max(start, n - fwd - 1)

    thr = thr_k * atr_h4
    fail_thr = fail_k * atr_h4

    tests = 0
    reactions = 0
    failures = 0
    mfe_atr: List[float] = []
    mae_atr: List[float] = []
    t_react: List[int] = []

    last_test_utc: Optional[str] = None
    last_reaction_utc: Optional[str] = None

    if now_dt is None:
        now_dt = datetime.now(timezone.utc)


    for i in range(start, end):
        # overlap with band?
        if float(s_h4.l[i]) <= hi and float(s_h4.h[i]) >= lo:
            tests += 1
            last_test_utc = datetime.fromtimestamp(s_h4.ct[i] / 1000, tz=timezone.utc).isoformat(timespec="seconds").replace(
                "+00:00", "Z"
            )

            base = float(s_h4.c[i])
            nxt_h = max(float(x) for x in s_h4.h[i + 1 : i + 1 + fwd])
            nxt_l = min(float(x) for x in s_h4.l[i + 1 : i + 1 + fwd])

            if side == "S":
                mfe = (nxt_h - base) / atr_h4
                mae = (base - nxt_l) / atr_h4
                mfe_atr.append(mfe)
                mae_atr.append(mae)

                # failure
                if nxt_l < lo - fail_thr:
                    failures += 1

                # reaction
                if (nxt_h - base) >= thr:
                    reactions += 1
                    # time-to-reaction
                    for j in range(1, fwd + 1):
                        if float(s_h4.h[i + j]) - base >= thr:
                            t_react.append(j)
                            break
                    last_reaction_utc = datetime.fromtimestamp(
                        s_h4.ct[i] / 1000, tz=timezone.utc
                    ).isoformat(timespec="seconds").replace("+00:00", "Z")

            else:  # "R"
                mfe = (base - nxt_l) / atr_h4
                mae = (nxt_h - base) / atr_h4
                mfe_atr.append(mfe)
                mae_atr.append(mae)

                if nxt_h > hi + fail_thr:
                    failures += 1

                if (base - nxt_l) >= thr:
                    reactions += 1
                    for j in range(1, fwd + 1):
                        if base - float(s_h4.l[i + j]) >= thr:
                            t_react.append(j)
                            break
                    last_reaction_utc = datetime.fromtimestamp(
                        s_h4.ct[i] / 1000, tz=timezone.utc
                    ).isoformat(timespec="seconds").replace("+00:00", "Z")

    def age_days(iso: Optional[str]) -> Optional[float]:
        if not iso:
            return None
        try:
            dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
        except Exception:
            return None
        return float(round((now_dt - dt).total_seconds() / 86400.0, 2))

    rr = (reactions / tests) if tests > 0 else None
    fr = (failures / tests) if tests > 0 else None

    def med(x: List[float]) -> Optional[float]:
        if not x:
            return None
        x2 = sorted(x)
        mid = len(x2) // 2
        if len(x2) % 2 == 1:
            return float(round(x2[mid], 3))
        return float(round((x2[mid - 1] + x2[mid]) / 2.0, 3))

    def med_i(x: List[int]) -> Optional[int]:
        if not x:
            return None
        x2 = sorted(x)
        return int(x2[len(x2) // 2])

    return {
        "tests": int(tests),
        "reactions": int(reactions),
        "reaction_rate": float(round(rr, 3)) if rr is not None else None,
        "failures": int(failures),
        "failure_rate": float(round(fr, 3)) if fr is not None else None,
        "median_mfe_atr_h4": med(mfe_atr),
        "median_mae_atr_h4": med(mae_atr),
        "median_bars_to_reaction_h4": med_i(t_react),
        "lookback_h4_bars": int(min(lookback, len(s_h4.c))),
        "fwd_h4_bars": int(fwd),
        "threshold_atr_h4": float(round(thr_k, 2)),
        "last_test_utc": last_test_utc,
        "last_reaction_utc": last_reaction_utc,
        "days_since_last_test": age_days(last_test_utc),
        "days_since_last_reaction": age_days(last_reaction_utc),
    }

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
    """Pick nearest zones below (S) or above (R) current price, avoiding overlaps.

    Why: wide structural zones can overlap if built from close clusters; this makes the forecast unreadable.
    We keep only non-overlapping nearest zones.
    """

    def _norm(z: Dict[str, Any]) -> Tuple[float, float]:
        lo, hi = z.get("zone", [0.0, 0.0])
        lo_f, hi_f = float(lo), float(hi)
        return (lo_f, hi_f) if lo_f <= hi_f else (hi_f, lo_f)

    def _overlap(a: Tuple[float, float], b: Tuple[float, float], eps: float = 0.0) -> bool:
        return max(a[0], b[0]) <= min(a[1], b[1]) + eps

    filt: List[Tuple[float, float, float, Dict[str, Any]]] = []
    for z in zones:
        lo, hi = _norm(z)
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

        strength = float(z.get("strength") or 0.0)
        width = max(0.0, hi - lo)
        # sort: distance, then stronger, then tighter
        filt.append((dist, -strength, width, z))

    filt.sort(key=lambda x: (x[0], x[1], x[2]))

    out: List[Dict[str, Any]] = []
    for _, _, _, z in filt:
        zr = _norm(z)
        if any(_overlap(zr, _norm(s)) for s in out):
            continue
        out.append(z)
        if len(out) >= max_n:
            break
    return out

def build_symbol_state(base_dir: Path, symbol: str) -> Dict[str, Any]:
    # Load series
    s_h4 = to_series(load_rows_from_chunks(base_dir, symbol, "H4", TAIL_N["H4"]))
    s_d1 = to_series(load_rows_from_chunks(base_dir, symbol, "D1", TAIL_N["D1"]))
    s_w1 = to_series(load_rows_from_chunks(base_dir, symbol, "W1", TAIL_N["W1"]))

    price = float(s_h4.c[-1])
    atr_h4 = atr14(s_h4)
    atr_d1 = atr14(s_d1)
    atr_w1 = atr14(s_w1)

    # MA filter (EMA200) — только для режима/конфлюэнса, НЕ для генерации уровней
    ema200_d1_s = ema_series(s_d1.c, 200)
    ema200_w1_s = ema_series(s_w1.c, 200)
    ema200_d1 = ema200_d1_s[-1] if ema200_d1_s else None
    ema200_w1 = ema200_w1_s[-1] if ema200_w1_s else None

    ema200_d1_prev = ema200_d1_s[-6] if len(ema200_d1_s) >= 206 else None  # ~5 баров назад
    ema200_w1_prev = ema200_w1_s[-6] if len(ema200_w1_s) >= 206 else None

    ema200_slope_d1 = slope_tag(ema200_d1, ema200_d1_prev, eps=max(1e-9, 0.02 * atr_d1))
    ema200_slope_w1 = slope_tag(ema200_w1, ema200_w1_prev, eps=max(1e-9, 0.02 * atr_w1))

    ma = {
        "ema200": {"D1": round(float(ema200_d1), 2) if ema200_d1 is not None else None,
                   "W1": round(float(ema200_w1), 2) if ema200_w1 is not None else None},
        "ema200_slope": {"D1": ema200_slope_d1, "W1": ema200_slope_w1},
        "price_vs_ema200": {
            "D1": ("above" if ema200_d1 is not None and price > float(ema200_d1) else ("below" if ema200_d1 is not None else None)),
            "W1": ("above" if ema200_w1 is not None and price > float(ema200_w1) else ("below" if ema200_w1 is not None else None)),
        },
        "available": {"D1": bool(ema200_d1 is not None), "W1": bool(ema200_w1 is not None)},
        "period": 200,
    }


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
                    if not clusters:
                        continue

                    # IMPORTANT: prevent overlapping wide zones when cluster centers are close.
                    centers = [float(cl["center"]) for cl in clusters]
                    eps = max(atr_tf * 0.02, max(centers) * 1e-6)

                    for i, cl in enumerate(clusters):
                        center = float(cl["center"])

                        half_eff = half
                        if i > 0:
                            half_eff = min(half_eff, max(0.0, 0.5 * (centers[i] - centers[i - 1]) - eps))
                        if i < len(centers) - 1:
                            half_eff = min(half_eff, max(0.0, 0.5 * (centers[i + 1] - centers[i]) - eps))

                        zone = clamp_zone(center, half_eff)
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

    # Selected structural zones: 1 support + 1 resistance (сильные, на своей стороне)
    sel_struct_s = pick_best_by_strength(zones_s, price, "S")
    sel_struct_r = pick_best_by_strength(zones_r, price, "R")

    struct_s_zone = list(sel_struct_s["zone"]) if isinstance(sel_struct_s, dict) and sel_struct_s.get("zone") else None
    struct_r_zone = list(sel_struct_r["zone"]) if isinstance(sel_struct_r, dict) and sel_struct_r.get("zone") else None

    # Execution bands width (узкие "операционные полосы" у края макро-зон)
    band_w = exec_band_width(symbol, atr_d1, atr_h4)

    # Range map (W1): берём W1 поддержку ниже цены и W1 сопротивление выше цены (если есть)
    w1_supports = [z for z in zones_all if z.get("tf") == "W1" and z.get("side") == "S"]
    w1_resists = [z for z in zones_all if z.get("tf") == "W1" and z.get("side") == "R"]

    def pick_w1_bracket(zs: List[Dict[str, Any]], side: str) -> Optional[Dict[str, Any]]:
        cand: List[Tuple[int, int, float, Dict[str, Any]]] = []
        for z in zs:
            lo, hi = float(z["zone"][0]), float(z["zone"][1])
            if side == "S":
                ok = hi <= price
                dist = (price - hi) if ok else (hi - price)
            else:
                ok = lo >= price
                dist = (lo - price) if ok else (price - lo)
            pref = 0 if ok else 1
            cand.append((pref, -int(z.get("strength", 1)), float(dist), z))
        cand.sort(key=lambda x: (x[0], x[1], x[2]))
        return cand[0][3] if cand else None

    w1_s = pick_w1_bracket(w1_supports, "S") or (w1_supports[0] if w1_supports else None)
    w1_r = pick_w1_bracket(w1_resists, "R") or (w1_resists[0] if w1_resists else None)

    discount_edge = float(w1_s["zone"][1]) if w1_s else None
    premium_edge = float(w1_r["zone"][0]) if w1_r else None
    equilibrium = (discount_edge + premium_edge) / 2.0 if (discount_edge is not None and premium_edge is not None) else None

    discount_band = [round(discount_edge - band_w, 2), round(discount_edge, 2)] if discount_edge is not None else None
    premium_band = [round(premium_edge, 2), round(premium_edge + band_w, 2)] if premium_edge is not None else None

    range_w1 = {
        "support": w1_s,
        "resistance": w1_r,
        "discount_edge": round(discount_edge, 2) if discount_edge is not None else None,
        "premium_edge": round(premium_edge, 2) if premium_edge is not None else None,
        "equilibrium": round(equilibrium, 2) if equilibrium is not None else None,
        "bands": {
            "discount": discount_band,
            "premium": premium_band,
            "band_width": round(band_w, 2),
            "params": {
                "k_atr_d1": EXEC_BAND_K_ATR_D1,
                "fallback_atr_h4_k": EXEC_BAND_FALLBACK_ATR_H4_K,
                "minmax": EXEC_BAND_MINMAX.get(symbol),
            },
        },
        "reaction": {
            "discount": band_reaction_stats(s_h4, (discount_band[0], discount_band[1]), "S", atr_h4) if discount_band else None,
            "premium": band_reaction_stats(s_h4, (premium_band[0], premium_band[1]), "R", atr_h4) if premium_band else None,
        },
    }

    # Swing map (D1): берём D1 зоны на своей стороне цены; если нет — fallback на выбранные структурные
    d1_supports = [z for z in zones_all if z.get("tf") == "D1" and z.get("side") == "S"]
    d1_resists = [z for z in zones_all if z.get("tf") == "D1" and z.get("side") == "R"]

    sel_swing_s = pick_best_by_strength(d1_supports, price, "S") or sel_struct_s
    sel_swing_r = pick_best_by_strength(d1_resists, price, "R") or sel_struct_r

    swing_s_band = None
    swing_r_band = None
    if sel_swing_s and sel_swing_s.get("zone"):
        zlo, zhi = float(sel_swing_s["zone"][0]), float(sel_swing_s["zone"][1])
        hi = zhi
        lo = max(zlo, hi - band_w)
        swing_s_band = [round(lo, 2), round(hi, 2)]

    if sel_swing_r and sel_swing_r.get("zone"):
        zlo, zhi = float(sel_swing_r["zone"][0]), float(sel_swing_r["zone"][1])
        lo = zlo
        hi = min(zhi, lo + band_w)
        swing_r_band = [round(lo, 2), round(hi, 2)]

    swing_d1 = {
        "support": sel_swing_s,
        "resistance": sel_swing_r,
        "bands": {
            "support_entry": swing_s_band,
            "resistance_entry": swing_r_band,
            "band_width": round(band_w, 2),
        },
        "reaction": {
            "support_entry": band_reaction_stats(s_h4, (swing_s_band[0], swing_s_band[1]), "S", atr_h4) if swing_s_band else None,
            "resistance_entry": band_reaction_stats(s_h4, (swing_r_band[0], swing_r_band[1]), "R", atr_h4) if swing_r_band else None,
        },
    }

    # Forecast mapping for SWING (S1/S2/R1/R2)
    forecast_swing_4 = {
        "S1_swing_d1": sel_swing_s,
        "S2_range_w1": w1_s,
        "R1_swing_d1": sel_swing_r,
        "R2_range_w1": w1_r,
        "bands": {
            "S1_entry": swing_s_band,
            "R1_entry": swing_r_band,
            "S2_discount": discount_band,
            "R2_premium": premium_band,
        },
    }


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
        "ma": ma,
        "zones": {
            "structural": {
                "supports": nearest_s,
                "resistances": nearest_r,
                "selected": {
                    "support": sel_struct_s,
                    "resistance": sel_struct_r,
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
            "range_w1": range_w1,
            "swing_d1": swing_d1,
            "forecast_swing_4": forecast_swing_4,
            "forecast_4": forecast_4,
        },
    }


def to_swing_view(full_state: Dict[str, Any]) -> Dict[str, Any]:
    """Lightweight SWING view (default) from the full state."""
    return {
        "data": full_state.get("data", {}),
        "vol": full_state.get("vol", {}),
        "structure": full_state.get("structure", {}),
        "ma": full_state.get("ma", {}),
        "zones": {
            "range_w1": full_state.get("zones", {}).get("range_w1"),
            "swing_d1": full_state.get("zones", {}).get("swing_d1"),
            "forecast_swing_4": full_state.get("zones", {}).get("forecast_swing_4"),
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
        "source": "ta_state_pivots_atr_full",
        "notes": {
            "what": "FULL: W1/D1 structural + local/working H4 + MA (EMA200) + range/swing execution bands + reaction stats; NOT a forecast.",
            "regime": "trend/range/chop is a risk filter; chop/high-vol => prefer WAIT.",
        },
        "symbols": {},
    }

    out_swing: Dict[str, Any] = {
        "updated_utc": updated_utc,
        "source": "ta_state_pivots_atr_swing",
        "notes": {
            "what": "SWING (default): W1 range map (premium/discount edges + equilibrium) + D1 swing zones + EMA200 as filter + execution bands + reaction stats; NOT a forecast.",
            "how": "Use edges (bands) for entries; avoid trading the middle of chop.",
        },
        "symbols": {},
    }

    for sym in symbols:
        try:
            st_full = build_symbol_state(base_dir, sym)
            out_full["symbols"][sym] = st_full
            out_swing["symbols"][sym] = to_swing_view(st_full)
        except Exception as e:
            out_full["symbols"][sym] = {"error": str(e)}
            out_swing["symbols"][sym] = {"error": str(e)}

    for root in OUT_ROOTS:
        d = root / "ta" / "binance"
        d.mkdir(parents=True, exist_ok=True)
        # Default SWING
        write_json_pretty(d / "state_btc_eth_latest.json", out_swing)
        # FULL (on demand)
        write_json_pretty(d / "state_btc_eth_full_latest.json", out_full)


if __name__ == "__main__":
    main()
