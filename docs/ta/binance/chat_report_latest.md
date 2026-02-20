IRON-PROOF (НЕ РЕДАКТИРОВАТЬ)
- generated_utc: 2026-02-20T06:13:19Z
- generated_local: 2026-02-20 08:13:19 EET
- state.updated_utc: 2026-02-20T06:13:19.258226Z
- state.sha256: d180a4d164835cb82405346cbcba941b18892726b132b0dbf7554b6a02326031
- bundle.sha256: 392ff81afd82d586bf4467e00df4e7790af6f1c36894f440ff6c0793e9183ddf

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
- price(state): 67 366.25
- regime: chop | W1: range | D1: range
- ATR(D1): 3 599.87 | ATR(H4): 923.53
- EMA200(D1): 93 090.44 | EMA200(W1): 68 067.71

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, mixed): CORE [66 205.47 – 67 086.04] | BUF [66 205.47 – 67 086.04] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S2 (oper, mixed): CORE [64 702.41 – 66 047.42] | BUF [64 702.41 – 66 047.42] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S3 (struct, mixed): CORE [57 660.09 – 62 339.91] | BUF [56 843.99 – 62 339.91] 🔴 (силa=2/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S4 (macro, mixed): CORE [50 943.97 – 56 027.89] | BUF [45 392.61 – 56 843.99] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
Сопротивления:
- R1 (oper, bounce): CORE [67 994.93 – 69 249.93] | BUF [67 994.93 – 69 249.93] 🟢 (силa=5/5, macro: tests=28, rr=0.964, fr=0.286)
- R2 (oper, bounce): CORE [69 578.06 – 70 975.60] | BUF [69 578.06 – 71 155.67] 🟢 (силa=5/5, macro: tests=28, rr=0.964, fr=0.286)
- R3 (struct, bounce): CORE [71 335.74 – 72 687.00] | BUF [71 155.67 – 75 634.41] 🟢 (силa=5/5, macro: tests=28, rr=0.964, fr=0.286)
- R4 (macro, bounce): CORE [78 944.41 – 79 775.59] | BUF [78 944.41 – 79 775.59] 🟢 (силa=5/5, macro: tests=28, rr=0.964, fr=0.286)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30

## ETHUSDT
- price(state): 1 954.85
- regime: chop | W1: down | D1: range
- ATR(D1): 129.53 | ATR(H4): 34.58
- EMA200(D1): 3 071.20 | EMA200(W1): 2 596.74

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, magnet): CORE [1 881.68 – 1 922.56] | BUF [1 881.68 – 1 922.56] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S2 (oper, magnet): CORE [1 645.84 – 1 834.16] | BUF [1 636.99 – 1 834.16] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S3 (struct, magnet): CORE [1 439.80 – 1 628.13] | BUF [1 439.80 – 1 636.99] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S4 (macro, magnet): CORE [1 044.63 – 1 335.37] | BUF [1 044.63 – 1 387.59] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
Сопротивления:
- R1 (oper, mixed): CORE [1 971.99 – 2 039.07] | BUF [1 971.99 – 2 042.37] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.595)
- R2 (oper, mixed): CORE [2 045.66 – 2 214.04] | BUF [2 045.66 – 2 222.37] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.595)
- R3 (struct, mixed): CORE [2 230.70 – 2 581.30] | BUF [2 222.37 – 2 590.15] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.595)
- R4 (macro, mixed): CORE [2 599.00 – 2 949.60] | BUF [2 590.15 – 2 949.60] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.595)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30
