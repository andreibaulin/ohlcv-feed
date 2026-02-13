#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from bisect import bisect_left
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

OUT_ROOTS = [Path("."), Path("docs")]
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]

FAPI_BASES = [
    "https://fapi.binance.com",
    # иногда в некоторых сетапах data-api проксирует и фьючи; если нет — просто фейлнется, но мы поймаем.
    "https://data-api.binance.vision",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def write_json_compact(path: Path, obj: Any) -> None:
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    atomic_write_bytes(path, s.encode("utf-8"))


def http_get_json(url: str, params: Dict[str, Any], timeout: int = 25) -> Any:
    full = url + ("?" + urlencode(params) if params else "")
    req = Request(full, headers={"User-Agent": "ohlcv-feed (GitHub Actions)", "Accept": "application/json"})
    with urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw.decode("utf-8"))


def try_get(path: str, params: Dict[str, Any], retries: int = 3) -> Any:
    last_err: Exception | None = None
    for base in FAPI_BASES:
        url = base.rstrip("/") + path
        for attempt in range(1, retries + 1):
            try:
                return http_get_json(url, params)
            except (HTTPError, URLError, TimeoutError, ValueError) as e:
                last_err = e
                time.sleep(0.6 * attempt)
    raise RuntimeError(f"fetch failed for {path}: {last_err}")


def parse_symbols_env() -> List[str]:
    raw = os.environ.get("DERIV_SYMBOLS", "").strip()
    if not raw:
        return DEFAULT_SYMBOLS
    out: List[str] = []
    for x in raw.split(","):
        s = x.strip().upper()
        if s:
            out.append(s)
    return out or DEFAULT_SYMBOLS


def to_f(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        # Binance часто даёт числа строками
        return float(x)
    except Exception:
        return None


def quantile(sorted_vals: List[float], q: float) -> Optional[float]:
    """q in [0,1], linear interpolation."""
    if not sorted_vals:
        return None
    if q <= 0:
        return float(sorted_vals[0])
    if q >= 1:
        return float(sorted_vals[-1])
    n = len(sorted_vals)
    pos = (n - 1) * q
    lo = int(pos)
    hi = min(n - 1, lo + 1)
    if hi == lo:
        return float(sorted_vals[lo])
    w = pos - lo
    return float(sorted_vals[lo] * (1.0 - w) + sorted_vals[hi] * w)


def percentile_rank(sorted_vals: List[float], v: float) -> Optional[float]:
    """Return percentile rank in [0,1]."""
    if not sorted_vals:
        return None
    i = bisect_left(sorted_vals, v)
    # i elements are < v
    return float(i) / float(len(sorted_vals))


def compute_oi_band_from_hist(hist: Any) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Build OI banding summary from openInterestHist response.

    Returns: (band_obj, metric_name) where metric_name is the metric we used.
    """
    if not isinstance(hist, list) or not hist:
        return None, None

    # pick metric: prefer sumOpenInterestValue (USDT), fallback to sumOpenInterest/openInterest
    metric: Optional[str] = None
    for m in ("sumOpenInterestValue", "sumOpenInterest", "openInterest"):
        if m in hist[-1]:
            metric = m
            break
    if metric is None:
        return None, None

    vals: List[Tuple[int, float]] = []
    for it in hist:
        if not isinstance(it, dict):
            continue
        ts = it.get("timestamp") or it.get("time") or it.get("T")
        t = int(ts) if ts is not None else 0
        v = to_f(it.get(metric))
        if v is None:
            continue
        if v <= 0:
            continue
        vals.append((t, float(v)))
    if len(vals) < 10:
        return None, metric

    series = [v for _, v in vals]
    sv = sorted(series)
    cur = series[-1]

    p20 = quantile(sv, 0.20)
    p80 = quantile(sv, 0.80)
    p95 = quantile(sv, 0.95)
    pct = percentile_rank(sv, cur)

    # 1d/7d changes (по этому же метрику)
    ch1 = None
    ch7 = None
    if len(series) >= 2 and series[-2] > 0:
        ch1 = (cur / series[-2] - 1.0) * 100.0
    if len(series) >= 8 and series[-8] > 0:
        ch7 = (cur / series[-8] - 1.0) * 100.0

    band = None
    if pct is not None:
        if pct < 0.20:
            band = "low"
        elif pct < 0.80:
            band = "normal"
        elif pct < 0.95:
            band = "elevated"
        else:
            band = "extreme"

    # маленький хвост для дебага (не раздуваем файл)
    tail = vals[-10:]
    tail_out = [{"t": t, "v": round(v, 6)} for t, v in tail]

    out = {
        "period": "1d",
        "window": len(series),
        "metric": metric,
        "value": round(cur, 6),
        "percentile": round(pct, 6) if pct is not None else None,
        "band": band,
        "p20": round(p20, 6) if p20 is not None else None,
        "p80": round(p80, 6) if p80 is not None else None,
        "p95": round(p95, 6) if p95 is not None else None,
        "change_1d_pct": round(ch1, 4) if ch1 is not None else None,
        "change_7d_pct": round(ch7, 4) if ch7 is not None else None,
        "tail": tail_out,
    }
    return out, metric


def main() -> None:
    updated_utc = utc_now_iso()
    symbols = parse_symbols_env()

    out: Dict[str, Any] = {
        "updated_utc": updated_utc,
        "source": "binance_usdtm_futures",
        "symbols": {},
    }

    for sym in symbols:
        entry: Dict[str, Any] = {"errors": []}

        # Funding snapshot (premiumIndex)
        try:
            prem = try_get("/fapi/v1/premiumIndex", {"symbol": sym})
            entry["funding"] = {
                "lastFundingRate": prem.get("lastFundingRate"),
                "nextFundingTime": prem.get("nextFundingTime"),
                "markPrice": prem.get("markPrice"),
                "indexPrice": prem.get("indexPrice"),
                "time": prem.get("time"),
            }
        except Exception as e:
            entry["funding"] = None
            entry["errors"].append(f"funding: {e}")

        # Open interest snapshot
        try:
            oi = try_get("/fapi/v1/openInterest", {"symbol": sym})
            entry["open_interest"] = {
                "openInterest": oi.get("openInterest"),
                "time": oi.get("time"),
            }
        except Exception as e:
            entry["open_interest"] = None
            entry["errors"].append(f"open_interest: {e}")

        # Open interest banding (history -> percentile bands)
        try:
            hist = try_get(
                "/futures/data/openInterestHist",
                {"symbol": sym, "period": "1d", "limit": 90},
            )
            band_obj, _metric = compute_oi_band_from_hist(hist)
            entry["open_interest_band"] = band_obj
            if band_obj is None:
                entry["errors"].append("open_interest_band: insufficient_hist")
        except Exception as e:
            entry["open_interest_band"] = None
            entry["errors"].append(f"open_interest_band: {e}")

        # Global long/short account ratio (last point)
        try:
            gls = try_get(
                "/futures/data/globalLongShortAccountRatio",
                {"symbol": sym, "period": "4h", "limit": 30},
            )
            if isinstance(gls, list) and gls:
                entry["global_long_short_account_ratio"] = gls[-1]
            else:
                entry["global_long_short_account_ratio"] = gls
            entry["global_long_short_account_ratio_note"] = "period=4h, last point"
        except Exception as e:
            entry["global_long_short_account_ratio"] = None
            entry["errors"].append(f"long_short: {e}")

        out["symbols"][sym] = entry

    for root in OUT_ROOTS:
        d = root / "deriv" / "binance"
        d.mkdir(parents=True, exist_ok=True)
        write_json_compact(d / "core5_latest.json", out)
        write_json_compact(d / "core10_latest.json", out)


if __name__ == "__main__":
    main()
