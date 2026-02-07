import os, json, time, shutil
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.parse import urlencode

# ================= CONFIG =================
OUT_ROOT = "docs"
OHLCV_DIR = os.path.join(OUT_ROOT, "ohlcv", "binance")
DERIV_DIR = os.path.join(OUT_ROOT, "deriv", "binance")

CORE5  = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "LINKUSDT"]
CORE10 = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "UNIUSDT", "ARBUSDT", "ADAUSDT"]

TFS = {"H1": "1h", "H4": "4h", "D1": "1d", "W1": "1w"}
LIMIT = {"H1": 720, "H4": 600, "D1": 520, "W1": 260}

BINANCE_BASE = "https://fapi.binance.com"  # primary
BYBIT_BASE   = "https://api.bybit.com"     # fallback

BYBIT_INTERVAL = {"H1": "60", "H4": "240", "D1": "D", "W1": "W"}
# =========================================

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def iso_utc(ms: int):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def write_text(path: str, text: str):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)

def write_json(path: str, obj):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))

def http_json(url: str, retries=3, timeout=30):
    last = None
    for i in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "ohlcv-feed/2.0"})
            with urlopen(req, timeout=timeout) as r:
                raw = r.read().decode("utf-8")
            return json.loads(raw)
        except Exception as e:
            last = e
            time.sleep(1.2 * (2 ** i))
    raise RuntimeError(f"HTTP failed: {url}; err={last}")

def b_url(path: str, params=None):
    u = BINANCE_BASE + path
    if params:
        u += "?" + urlencode(params)
    return u

def y_url(path: str, params=None):
    u = BYBIT_BASE + path
    if params:
        u += "?" + urlencode(params)
    return u

def tf_ms(tfk: str) -> int:
    return {"H1": 3600000, "H4": 14400000, "D1": 86400000, "W1": 604800000}[tfk]

# ---------------- OHLCV ----------------
def fetch_klines_binance(symbol: str, tfk: str):
    interval = TFS[tfk]
    lim = LIMIT.get(tfk, 500)
    data = http_json(b_url("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": lim}))
    if not isinstance(data, list) or not data:
        raise RuntimeError("empty binance klines")

    out = []
    now_ms = int(time.time() * 1000)
    for r in data:
        ot = int(r[0]); ct = int(r[6])
        if now_ms <= ct:  # не закрыта
            continue
        # [openTime, open, high, low, close, volume, closeTime]
        out.append([ot, r[1], r[2], r[3], r[4], r[5], ct])

    if not out:
        raise RuntimeError("no closed binance klines")
    return out

def fetch_klines_bybit(symbol: str, tfk: str):
    lim = min(1000, max(1, LIMIT.get(tfk, 500)))
    interval = BYBIT_INTERVAL[tfk]

    j = http_json(y_url("/v5/market/kline", {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": lim
    }))
    if not isinstance(j, dict) or j.get("retCode") != 0:
        raise RuntimeError(f"bybit retCode={j.get('retCode')} msg={j.get('retMsg')}")

    rows = ((j.get("result") or {}).get("list") or [])
    if not rows:
        raise RuntimeError("empty bybit klines")

    rows = sorted(rows, key=lambda x: int(x[0]))  # ascending by openTime
    ms = tf_ms(tfk)
    now_ms = int(time.time() * 1000)

    out = []
    for r in rows:
        ot = int(r[0])
        ct = ot + ms - 1
        if now_ms <= ct:  # не закрыта
            continue
        # bybit: [start, open, high, low, close, volume, turnover]
        out.append([ot, r[1], r[2], r[3], r[4], r[5], ct])

    if not out:
        raise RuntimeError("no closed bybit klines")
    return out

def fetch_klines(symbol: str, tfk: str):
    try:
        return "binance", fetch_klines_binance(symbol, tfk)
    except Exception:
        return "bybit", fetch_klines_bybit(symbol, tfk)

# -------------- DERIV (best-effort) --------------
def fetch_deriv_binance(symbol: str):
    premium = http_json(b_url("/fapi/v1/premiumIndex", {"symbol": symbol}))
    funding = http_json(b_url("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 200}))
    oi_now  = http_json(b_url("/fapi/v1/openInterest", {"symbol": symbol}))

    fs = []
    if isinstance(funding, list):
        for r in funding:
            try:
                fs.append([int(r["fundingTime"]), float(r["fundingRate"])])
            except Exception:
                pass

    return {
        "source": "binance",
        "premium_index": premium,
        "funding_8h_series": fs,
        "open_interest_now": oi_now
    }

def fetch_deriv_bybit(symbol: str):
    tick = http_json(y_url("/v5/market/tickers", {"category": "linear", "symbol": symbol}))
    if not isinstance(tick, dict) or tick.get("retCode") != 0:
        raise RuntimeError(f"bybit tickers retCode={tick.get('retCode')} msg={tick.get('retMsg')}")
    t0 = (((tick.get("result") or {}).get("list")) or [{}])[0]

    fund = http_json(y_url("/v5/market/funding/history", {"category": "linear", "symbol": symbol, "limit": 200}))
    fs = []
    if isinstance(fund, dict) and fund.get("retCode") == 0:
        for r in ((fund.get("result") or {}).get("list") or []):
            try:
                fs.append([int(r["fundingRateTimestamp"]), float(r["fundingRate"])])
            except Exception:
                pass

    return {
        "source": "bybit",
        "premium_index": {
            "markPrice": t0.get("markPrice"),
            "indexPrice": t0.get("indexPrice"),
            "fundingRate": t0.get("fundingRate"),
            "nextFundingTime": t0.get("nextFundingTime"),
            "basis": t0.get("basis"),
            "basisRate": t0.get("basisRate"),
        },
        "funding_8h_series": fs,
        "open_interest_now": {
            "openInterest": t0.get("openInterest"),
            "openInterestValue": t0.get("openInterestValue"),
        }
    }

def fetch_deriv(symbol: str):
    try:
        return fetch_deriv_binance(symbol)
    except Exception:
        return fetch_deriv_bybit(symbol)

# ---------------- BUILD ----------------
def build():
    gen = now_iso()

    # чистим только наши папки (без мусора/конфликтов)
    for p in [OHLCV_DIR, DERIV_DIR]:
        if os.path.exists(p):
            shutil.rmtree(p)

    ensure_dir(OUT_ROOT)
    ensure_dir(OHLCV_DIR)
    ensure_dir(DERIV_DIR)

    # OHLCV: CORE10 best-effort, CORE5 strict
    source_map = {}
    data10 = {}
    ok_symbols = []
    core5_errors = []
    other_errors = []

    for sym in CORE10:
        sym_ok = True
        data10[sym] = {}
        source_map[sym] = {}

        for tfk in TFS.keys():
            try:
                src, rows = fetch_klines(sym, tfk)
                source_map[sym][tfk] = src

                # txt (строго построчно)
                lines = []
                pack = []
                for r in rows:
                    ot, o, h, l, c, v, ct = int(r[0]), r[1], r[2], r[3], r[4], r[5], int(r[6])
                    lines.append(f"{ot},{o},{h},{l},{c},{v},{ct}")
                    pack.append([ot, float(o), float(h), float(l), float(c), float(v)])

                write_text(os.path.join(OHLCV_DIR, f"{sym}_{tfk}.txt"), "\n".join(lines) + "\n")

                data10[sym][tfk] = {
                    "tf": tfk,
                    "bars": len(pack),
                    "last_close_utc": iso_utc(rows[-1][6]),
                    "data": pack
                }

                time.sleep(0.08)
            except Exception as e:
                sym_ok = False
                if sym in CORE5:
                    core5_errors.append(f"{sym} {tfk}: {e}")
                else:
                    other_errors.append(f"{sym} {tfk}: {e}")
                break  # нет смысла добивать остальные TF

        if sym_ok:
            ok_symbols.append(sym)
        else:
            # если символ не ок — убираем заготовку, чтобы не было “полусимволов” в packs
            data10.pop(sym, None)
            source_map.pop(sym, None)

    if core5_errors:
        # CORE5 должны быть всегда, иначе лучше не публиковать обновление
        raise RuntimeError("CORE5 OHLCV failed (strict): " + " | ".join(core5_errors[:5]))

    # manifest: честно показываем доступные symbols + желаемые
    write_json(os.path.join(OHLCV_DIR, "symbols.json"), {
        "tfs": list(TFS.keys()),
        "updated_utc": gen,
        "symbols": ok_symbols,
        "desired_symbols": CORE10
    })

    # packs
    data5 = {k: data10[k] for k in CORE5 if k in data10}

    meta10 = {
        "timezone": "UTC",
        "generated_utc": gen,
        "tfs": list(TFS.keys()),
        "source_primary": "binance-usdm-public",
        "source_fallback": "bybit-linear-public",
        "ohlcv_source_map": source_map,
        "warnings": other_errors[:100]
    }
    meta5 = dict(meta10)
    meta5["ohlcv_source_map"] = {k: source_map[k] for k in CORE5}

    write_json(os.path.join(OHLCV_DIR, "core10_latest.json"), {"meta": meta10, "data": data10})
    write_json(os.path.join(OHLCV_DIR, "core5_latest.json"),  {"meta": meta5,  "data": data5})

    # Deriv packs (best-effort)
    d10 = {}
    dw = []
    for sym in ok_symbols:
        try:
            d10[sym] = fetch_deriv(sym)
        except Exception as e:
            d10[sym] = {"source": "none"}
            dw.append(f"{sym}: {e}")
        time.sleep(0.05)

    d5 = {k: d10.get(k, {"source":"none"}) for k in CORE5}
    dmeta = {"timezone": "UTC", "generated_utc": gen, "warnings": dw[:100]}

    write_json(os.path.join(DERIV_DIR, "core10_latest.json"), {"meta": dmeta, "data": d10})
    write_json(os.path.join(DERIV_DIR, "core5_latest.json"),  {"meta": dmeta, "data": d5})

    # one entrypoint (удобно для меня: 1 URL)
    write_json(os.path.join(OUT_ROOT, "feed.json"), {
        "updated_utc": gen,
        "entrypoints": {
            "manifest": "ohlcv/binance/symbols.json",
            "core5_ohlcv": "ohlcv/binance/core5_latest.json",
            "core10_ohlcv": "ohlcv/binance/core10_latest.json",
            "core5_deriv": "deriv/binance/core5_latest.json",
            "core10_deriv": "deriv/binance/core10_latest.json"
        }
    })

    # static site helpers
    write_text(os.path.join(OUT_ROOT, ".nojekyll"), "")
    write_text(
        os.path.join(OUT_ROOT, "index.html"),
        f'<!doctype html><meta charset="utf-8"><title>ohlcv-feed</title>'
        f'<h1>ohlcv-feed</h1><p>updated_utc: <code>{gen}</code></p>'
        f'<ul>'
        f'<li><a href="feed.json">feed.json</a></li>'
        f'<li><a href="ohlcv/binance/symbols.json">symbols.json</a></li>'
        f'<li><a href="ohlcv/binance/core5_latest.json">core5_latest.json</a></li>'
        f'<li><a href="deriv/binance/core5_latest.json">deriv core5_latest.json</a></li>'
        f'</ul>'
    )

    print("OK generated_utc =", gen)

if __name__ == "__main__":
    build()
