IRON-PROOF (НЕ РЕДАКТИРОВАТЬ)
- generated_utc: 2026-02-20T01:10:59Z
- generated_local: 2026-02-20 03:10:59 EET
- state.updated_utc: 2026-02-20T01:10:59.224815Z
- state.sha256: ca535b275012d2c63b7b134ae752cc7a6e1f986cb0ce110fa91067edaf7341c5
- bundle.sha256: f483c079f2d7304886c5c13feb3098ce57f80ae01824e91705746bf15b66b75e

Ссылки (рукопожатие):
https://andreibaulin.github.io/ohlcv-feed/ta/binance/chat_bundle_latest.json
https://andreibaulin.github.io/ohlcv-feed/ta/binance/chat_report_latest.md
https://andreibaulin.github.io/ohlcv-feed/ta/binance/state_btc_eth_latest.json
https://andreibaulin.github.io/ohlcv-feed/ta/binance/state_btc_eth_full_latest.json
https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/pack_btc_eth.txt
https://andreibaulin.github.io/ohlcv-feed/deriv/binance/core5_latest.json
https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=4h&limit=2
https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=2
https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=20
https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=4h&limit=2
https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=1h&limit=2
https://data-api.binance.vision/api/v3/klines?symbol=ETHUSDT&interval=15m&limit=20
https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30
https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30
https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT
https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT
https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30
https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30

## BTCUSDT
- price(state): 67 003.73
- regime: chop | W1: range | D1: range
- ATR(D1): 3 599.87 | ATR(H4): 981.75
- EMA200(D1): 93 090.44 | EMA200(W1): 68 067.71

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, mixed): CORE [65 314.21 – 66 311.79] | BUF [65 314.21 – 66 311.79] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S2 (oper, mixed): CORE [57 660.09 – 62 339.91] | BUF [56 843.99 – 62 339.91] 🔴 (силa=2/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S3 (struct, mixed): CORE [50 943.97 – 56 027.89] | BUF [50 740.68 – 56 843.99] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S4 (macro, mixed): CORE [45 392.61 – 50 537.39] | BUF [45 392.61 – 50 740.68] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
Сопротивления:
- R1 (oper, bounce): CORE [67 968.73 – 69 276.13] | BUF [67 968.73 – 69 276.13] 🟢 (силa=5/5, macro: tests=28, rr=0.964, fr=0.286)
- R2 (oper, bounce): CORE [69 551.86 – 71 001.80] | BUF [69 551.86 – 71 155.67] 🟢 (силa=5/5, macro: tests=28, rr=0.964, fr=0.286)
- R3 (struct, bounce): CORE [71 309.54 – 72 713.20] | BUF [71 155.67 – 75 634.41] 🟢 (силa=5/5, macro: tests=28, rr=0.964, fr=0.286)
- R4 (macro, bounce): CORE [78 918.21 – 79 801.79] | BUF [78 918.21 – 79 801.79] 🟢 (силa=5/5, macro: tests=28, rr=0.964, fr=0.286)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30

## ETHUSDT
- price(state): 1 949.08
- regime: chop | W1: down | D1: range
- ATR(D1): 129.53 | ATR(H4): 37.73
- EMA200(D1): 3 071.20 | EMA200(W1): 2 596.74

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, magnet): CORE [1 880.26 – 1 920.17] | BUF [1 880.26 – 1 920.17] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S2 (oper, magnet): CORE [1 645.84 – 1 834.16] | BUF [1 636.99 – 1 834.16] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S3 (struct, magnet): CORE [1 439.80 – 1 628.13] | BUF [1 439.80 – 1 636.99] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S4 (macro, magnet): CORE [1 044.63 – 1 335.37] | BUF [1 044.63 – 1 387.59] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
Сопротивления:
- R1 (oper, mixed): CORE [1 970.57 – 2 040.49] | BUF [1 970.57 – 2 043.08] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.595)
- R2 (oper, mixed): CORE [2 045.66 – 2 214.04] | BUF [2 045.66 – 2 222.37] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.595)
- R3 (struct, mixed): CORE [2 230.70 – 2 581.30] | BUF [2 222.37 – 2 590.15] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.595)
- R4 (macro, mixed): CORE [2 599.00 – 2 949.60] | BUF [2 590.15 – 2 949.60] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.595)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30
