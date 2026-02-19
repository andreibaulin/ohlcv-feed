IRON-PROOF (НЕ РЕДАКТИРОВАТЬ)
- generated_utc: 2026-02-19T19:35:48Z
- generated_local: 2026-02-19 21:35:48 EET
- state.updated_utc: 2026-02-19T19:35:47.859637Z
- state.sha256: fd64c88da5dc566fb833690a0fefe04a979df5dd47039b3846e427cc3dcbd2f8
- bundle.sha256: 21193422ee3e5bc266e9283c3199feb91b240e8f79f1bf1c14eda80ca600a1f1

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
- price(state): 66 398.00
- regime: chop | W1: range | D1: range
- ATR(D1): 4 264.72 | ATR(H4): 1 032.69
- EMA200(D1): 93 352.62 | EMA200(W1): 68 067.71

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, mixed): CORE [65 291.29 – 66 334.71] | BUF [65 291.29 – 66 334.71] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S2 (oper, mixed): CORE [57 526.29 – 62 473.71] | BUF [56 777.09 – 62 473.71] 🔴 (силa=2/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S3 (struct, mixed): CORE [50 943.97 – 56 027.89] | BUF [50 740.68 – 56 777.09] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S4 (macro, mixed): CORE [45 392.61 – 50 537.39] | BUF [45 392.61 – 50 740.68] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
Сопротивления:
- R1 (oper, bounce): CORE [66 855.29 – 67 784.71] | BUF [66 855.29 – 67 784.71] 🟡 (силa=5/5, macro: tests=48, rr=0.979, fr=0.333)
- R2 (oper, bounce): CORE [67 945.81 – 69 299.05] | BUF [67 945.81 – 69 413.99] 🟡 (силa=5/5, macro: tests=48, rr=0.979, fr=0.333)
- R3 (struct, bounce): CORE [69 528.94 – 71 024.72] | BUF [69 413.99 – 71 155.67] 🟡 (силa=5/5, macro: tests=48, rr=0.979, fr=0.333)
- R4 (macro, bounce): CORE [71 286.62 – 72 736.12] | BUF [71 155.67 – 74 399.27] 🟡 (силa=5/5, macro: tests=48, rr=0.979, fr=0.333)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30

## ETHUSDT
- price(state): 1 926.30
- regime: chop | W1: down | D1: range
- ATR(D1): 149.17 | ATR(H4): 40.01
- EMA200(D1): 3 082.48 | EMA200(W1): 2 596.74

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, magnet): CORE [1 879.23 – 1 921.20] | BUF [1 879.23 – 1 921.20] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S2 (oper, magnet): CORE [1 645.84 – 1 834.16] | BUF [1 640.03 – 1 834.16] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S3 (struct, magnet): CORE [1 440.30 – 1 634.22] | BUF [1 440.30 – 1 640.03] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S4 (macro, magnet): CORE [1 044.63 – 1 335.37] | BUF [1 044.63 – 1 387.84] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
Сопротивления:
- R1 (oper, mixed): CORE [1 969.54 – 2 041.52] | BUF [1 969.54 – 2 048.60] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)
- R2 (oper, mixed): CORE [2 055.67 – 2 091.69] | BUF [2 048.60 – 2 110.70] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)
- R3 (struct, mixed): CORE [2 129.72 – 2 170.04] | BUF [2 110.70 – 2 200.37] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)
- R4 (macro, mixed): CORE [2 230.70 – 2 581.30] | BUF [2 200.37 – 2 949.60] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30
