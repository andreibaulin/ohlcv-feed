import os, json, time, shutil
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError

# ================= CONFIG =================
OUT_ROOT = "docs"
TMP_ROOT = "_tmp_build"

OHLCV_DIR = os.path.join(TMP_ROOT, "ohlcv", "binance")
DERIV_DIR = os.path.join(TMP_ROOT, "deriv", "binance")

CORE5  = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "LINKUSDT"]
CORE10 = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "UNIUSDT", "ARBUSDT", "ADAUSDT"]

TFS = {"H1": "1h", "H4": "4h", "D1": "1d", "W1": "1w"}
STRICT_TFS = ["H4", "D1", "W1"]   # для CORE5 обязателен минимум
OPTIONAL_TFS = ["H1"]             # H1 — best-effort, не валим прогон

LIMIT = {"H1": 720, "H4": 600, "D1": 520, "W1": 260}

BINANCE_FUT_BASE = "https://fapi.binance.com"
BINANCE_SPOT_BASE = "https://api.binance.com"
OKX_BASE = "https://www.okx.com"

OKX_BAR = {"H1":"1H", "H4":"4H", "D1":"1D", "W1":"1W"}  # OKX candles bar
OKX_LIMIT_MAX = 300

DISABLED = {"binance_fut": False, "binance_spot": False, "okx": False}
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

def tf_ms(tfk: str) -> int:
    return {"H1": 3600000, "H4": 14400000, "D1": 86400000, "W1": 604800000}[tfk]

def http_json(url: str, provider: str, retries=3, timeout=30):
    last = None
    backoff = 1.0
    for i in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "ohlcv-feed/2.3"})
            with urlopen(req, timeout=timeout) as r:
                raw = r.read().decode("utf-8")
            return json.loads(raw)
        except HTTPError as e:
            last = e
            code = getattr(e, "code", None)
            # если 403/451/418/429 — считаем провайдера недоступным на прогон
            if code in (401, 403, 418, 429, 451):
                DISABLED[provider] = True
                raise RuntimeError(f"{provider} disabled due to HTTP {code}")
            time.sleep(backoff); backoff = min(10.0, backoff * 2)
        except (URLError, json.JSONDecodeError) as e:
            last = e
            time.sleep(backoff); backoff = min(10.0, backoff * 2)
    raise RuntimeError(f"HTTP failed: {url}; err={last}")

def b_fut_url(path: str, params=None):
    u = BINANCE_FUT_BASE + path
    if params: u += "?" + urlencode(params)
    return u

def b_spot_url(path: str, params=None):
    u = BINANCE_SPOT_BASE + path
    if params: u += "?" + urlencode(params)
    return u

def okx_url(path: str, params=None):
    u = OKX_BASE + path
    if params: u += "?" + urlencode(params)
    return u

# ---------------- OHLCV providers ----------------
def fetch_klines_binance_futures(symbol: str, tfk: str):
    interval = TFS[tfk]
    lim = LIMIT.get(tfk, 500)
    data = http_json(
        b_fut_url("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": lim}),
        provider="binance_fut",
        retries=2
    )
    if not isinstance(data, list) or not data:
        raise RuntimeError("empty binance futures klines")
    out = []
    now_ms = int(time.time() * 1000)
    for r in data:
        ot = int(r[0]); ct = int(r[6])
        if now_ms <= ct:
            continue
        out.append([ot, r[1], r[2], r[3], r[4], r[5], ct])
    if not out:
        raise RuntimeError("no closed binance futures klines")
    return out

def fetch_klines_binance_spot(symbol: str, tfk: str):
    interval = TFS[tfk]
    lim = min(1000, max(1, LIMIT.get(tfk, 500)))
    data = http_json(
        b_spot_url("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": lim}),
        provider="binance_spot",
        retries=2
    )
    if not isinstance(data, list) or not data:
        raise RuntimeError("empty binance spot klines")
    out = []
    now_ms = int(time.time() * 1000)
    for r in data:
        ot = int(r[0]); ct = int(r[6])
        if now_ms <= ct:
            continue
        out.append([ot, r[1], r[2], r[3], r[4], r[5], ct])
    if not out:
        raise RuntimeError("no closed binance spot klines")
    return out

def okx_inst(symbol: str) -> str:
    # BTCUSDT -> BTC-USDT
    base = symbol.replace("USDT", "")
    return f"{base}-USDT"

def fetch_klines_okx_spot(symbol: str, tfk: str):
    bar = OKX_BAR[tfk]
    lim = min(OKX_LIMIT_MAX, max(1, LIMIT.get(tfk, 300)))
    j = http_json(
        okx_url("/api/v5/market/candles", {"instId": okx_inst(symbol), "bar": bar, "limit": lim}),
        provider="okx",
        retries=2
    )
    if not isinstance(j, dict) or j.get("code") != "0":
        raise RuntimeError(f"okx code={j.get('code')} msg={j.get('msg')}")
    rows = j.get("data") or []
    if not rows:
        raise RuntimeError("empty okx candles")

    # OKX отдаёт в обратном порядке, развернём
    rows = sorted(rows, key=lambda x: int(x[0]))
    ms = tf_ms(tfk)
    now_ms = int(time.time() * 1000)

    out = []
    for r in rows:
        ot = int(r[0])
        ct = ot + ms - 1
        if now_ms <= ct:
            continue
        # r: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        out.append([ot, r[1], r[2], r[3], r[4], r[5], ct])

    if not out:
        raise RuntimeError("no closed okx candles")
    return out

def fetch_klines(symbol: str, tfk: str):
    # цепочка: binance futures -> binance spot -> okx
    if not DISABLED["binance_fut"]:
        try:
            return "binance_futures", fetch_klines_binance_futures(symbol, tfk)
        except Exception:
            pass
    if not DISABLED["binance_spot"]:
        try:
            return "binance_spot", fetch_klines_binance_spot(symbol, tfk)
        except Exception:
            pass
    if not DISABLED["okx"]:
        return "okx_spot", fetch_klines_okx_spot(symbol, tfk)
    raise RuntimeError("all providers disabled/unavailable")

# ---------------- Derivatives (best-effort, Binance Futures only) ----------------
def fetch_deriv_binance_futures(symbol: str):
    premium = http_json(b_fut_url("/fapi/v1/premiumIndex", {"symbol": symbol}), provider="binance_fut", retries=2)
    funding = http_json(b_fut_url("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 200}), provider="binance_fut", retries=2)
    oi_now  = http_json(b_fut_url("/fapi/v1/openInterest", {"symbol": symbol}), provider="binance_fut", retries=2)

    fs = []
    if isinstance(funding, list):
        for r in funding:
            try:
                fs.append([int(r["fundingTime"]), float(r["fundingRate"])])
            except Exception:
                pass

    return {"source":"binance_futures","premium_index":premium,"funding_8h_series":fs,"open_interest_now":oi_now}

def fetch_deriv(symbol: str):
    if DISABLED["binance_fut"]:
        return {"source":"none"}
    try:
        return fetch_deriv_binance_futures(symbol)
    except Exception:
        return {"source":"none"}

# ---------------- Build + atomic replace ----------------
def atomic_replace_docs():
    # переносим подпапки
    for rel in ["ohlcv/binance", "deriv/binance"]:
        src = os.path.join(TMP_ROOT, rel)
        dst = os.path.join(OUT_ROOT, rel)
        if os.path.exists(dst):
            shutil.rmtree(dst)
        if os.path.exists(src):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.move(src, dst)

    # корневые файлы
    for fn in ["feed.json", "index.html", ".nojekyll"]:
        src = os.path.join(TMP_ROOT, fn)
        dst = os.path.join(OUT_ROOT, fn)
        os.makedirs(OUT_ROOT, exist_ok=True)
        if os.path.exists(src):
            shutil.move(src, dst)

def build():
    gen = now_iso()

    # чистим tmp
    if os.path.exists(TMP_ROOT):
        shutil.rmtree(TMP_ROOT)
    os.makedirs(TMP_ROOT, exist_ok=True)

    ensure_dir(OHLCV_DIR)
    ensure_dir(DERIV_DIR)

    source_map = {}
    data_ok = {}
    ok_symbols = []
    warnings = []
    core5_strict_errors = []

    def write_symbol_tf(sym: str, tfk: str, src: str, rows):
        # txt строго построчно
        lines = []
        pack = []
        for r in rows:
            ot, o, h, l, c, v, ct = int(r[0]), r[1], r[2], r[3], r[4], r[5], int(r[6])
            lines.append(f"{ot},{o},{h},{l},{c},{v},{ct}")
            pack.append([ot, float(o), float(h), float(l), float(c), float(v)])
        write_text(os.path.join(OHLCV_DIR, f"{sym}_{tfk}.txt"), "\n".join(lines) + "\n")
        return {"tf": tfk, "bars": len(pack), "last_close_utc": iso_utc(rows[-1][6]), "data": pack}

    for sym in CORE10:
        try:
            sym_data = {}
            sym_src  = {}

            # STRICT_TFS: должны пройти для CORE5
            for tfk in STRICT_TFS:
                src, rows = fetch_klines(sym, tfk)
                sym_src[tfk] = src
                sym_data[tfk] = write_symbol_tf(sym, tfk, src, rows)
                time.sleep(0.10)

            # OPTIONAL_TFS: не валим прогон, если не получилось
            for tfk in OPTIONAL_TFS:
                try:
                    src, rows = fetch_klines(sym, tfk)
                    sym_src[tfk] = src
                    sym_data[tfk] = write_symbol_tf(sym, tfk, src, rows)
                    time.sleep(0.10)
                except Exception as e:
                    warnings.append(f"{sym} {tfk} optional failed: {e}")

            data_ok[sym] = sym_data
            source_map[sym] = sym_src
            ok_symbols.append(sym)

        except Exception as e:
            if sym in CORE5:
                core5_strict_errors.append(f"{sym} strict failed: {e}")
            else:
                warnings.append(f"{sym} strict failed: {e}")

    if core5_strict_errors:
        raise RuntimeError("CORE5 strict OHLCV failed: " + " | ".join(core5_strict_errors[:5]))

    # manifest
    write_json(os.path.join(OHLCV_DIR, "symbols.json"), {
        "tfs": list(TFS.keys()),
        "updated_utc": gen,
        "symbols": ok_symbols,
        "desired_symbols": CORE10
    })

    # packs
    data5 = {k: data_ok[k] for k in CORE5 if k in data_ok}
    meta10 = {
        "timezone":"UTC",
        "generated_utc": gen,
        "tfs": list(TFS.keys()),
        "sources_chain": ["binance_futures", "binance_spot", "okx_spot"],
        "ohlcv_source_map": source_map,
        "warnings": warnings[:200],
        "disabled": DISABLED
    }
    meta5 = dict(meta10)
    meta5["ohlcv_source_map"] = {k: source_map[k] for k in CORE5}

    write_json(os.path.join(OHLCV_DIR, "core10_latest.json"), {"meta": meta10, "data": data_ok})
    write_json(os.path.join(OHLCV_DIR, "core5_latest.json"),  {"meta": meta5,  "data": data5})

    # deriv best-effort
    d10, dw = {}, []
    for sym in ok_symbols:
        try:
            d10[sym] = fetch_deriv(sym)
        except Exception as e:
            d10[sym] = {"source":"none"}
            dw.append(f"{sym}: {e}")
        time.sleep(0.05)
    d5 = {k: d10.get(k, {"source":"none"}) for k in CORE5}
    dmeta = {"timezone":"UTC","generated_utc":gen,"warnings":dw[:200], "disabled": DISABLED}

    write_json(os.path.join(DERIV_DIR, "core10_latest.json"), {"meta": dmeta, "data": d10})
    write_json(os.path.join(DERIV_DIR, "core5_latest.json"),  {"meta": dmeta, "data": d5})

    # entrypoint
    write_json(os.path.join(TMP_ROOT, "feed.json"), {
        "updated_utc": gen,
        "entrypoints": {
            "manifest": "ohlcv/binance/symbols.json",
            "core5_ohlcv": "ohlcv/binance/core5_latest.json",
            "core10_ohlcv": "ohlcv/binance/core10_latest.json",
            "core5_deriv": "deriv/binance/core5_latest.json",
            "core10_deriv": "deriv/binance/core10_latest.json"
        }
    })

    write_text(os.path.join(TMP_ROOT, ".nojekyll"), "")
    write_text(os.path.join(TMP_ROOT, "index.html"),
               f'<!doctype html><meta charset="utf-8"><title>ohlcv-feed</title>'
               f'<h1>ohlcv-feed</h1><p>updated_utc: <code>{gen}</code></p>'
               f'<p>disabled: <code>{json.dumps(DISABLED)}</code></p>'
               f'<ul>'
               f'<li><a href="feed.json">feed.json</a></li>'
               f'<li><a href="ohlcv/binance/symbols.json">symbols.json</a></li>'
               f'<li><a href="ohlcv/binance/core5_latest.json">core5_latest.json</a></li>'
               f'</ul>')

    # атомарно обновляем docs
    atomic_replace_docs()

    # чистим tmp
    if os.path.exists(TMP_ROOT):
        shutil.rmtree(TMP_ROOT)

    print("OK generated_utc =", gen, "disabled=", DISABLED)

if __name__ == "__main__":
    build()
