import os, json, time, shutil
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.parse import urlencode

OUT_ROOT = "docs"
OHLCV_DIR = os.path.join(OUT_ROOT, "ohlcv", "binance")
DERIV_DIR = os.path.join(OUT_ROOT, "deriv", "binance")

CORE5  = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","LINKUSDT"]
CORE10 = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","AVAXUSDT","LINKUSDT","AAVEUSDT","UNIUSDT","ARBUSDT","ADAUSDT"]

TFS = {"H1":"1h","H4":"4h","D1":"1d","W1":"1w"}
LIMIT = {"H1": 720, "H4": 600, "D1": 520, "W1": 260}

BINANCE_BASE = "https://fapi.binance.com"   # primary
BYBIT_BASE   = "https://api.bybit.com"      # fallback

BYBIT_INTERVAL = {"H1":"60","H4":"240","D1":"D","W1":"W"}

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def iso_utc(ms: int):
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def write_text(path, text):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)

def write_json(path, obj):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))

def http_json(url, retries=3, timeout=30):
    last = None
    for i in range(retries):
        try:
            req = Request(url, headers={"User-Agent":"ohlcv-feed/1.0"})
            with urlopen(req, timeout=timeout) as r:
                raw = r.read().decode("utf-8")
            return json.loads(raw)
        except Exception as e:
            last = e
            time.sleep(1.2*(2**i))
    raise RuntimeError(f"HTTP failed: {url}; err={last}")

def b_url(path, params=None):
    u = BINANCE_BASE + path
    if params: u += "?" + urlencode(params)
    return u

def y_url(path, params=None):
    u = BYBIT_BASE + path
    if params: u += "?" + urlencode(params)
    return
