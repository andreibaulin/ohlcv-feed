IRON-PROOF (НЕ РЕДАКТИРОВАТЬ)
- generated_utc: 2026-02-19T14:43:03Z
- generated_local: 2026-02-19 16:43:03 EET
- state.updated_utc: 2026-02-19T14:43:03.495348Z
- state.sha256: cdddf9aa6c6ebf0a0c5ba3a73a3149208101621a1671d305899c33332dee76ee
- bundle.sha256: b1126452eabc417a7164d40cf325e7bfc6db509706c68000de3f0cd502095fe8

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
- price(state): 66 496.02
- regime: chop | W1: range | D1: range
- ATR(D1): 4 264.72 | ATR(H4): 1 010.69
- EMA200(D1): 93 352.62 | EMA200(W1): 68 067.71

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, mixed): CORE [65 301.19 – 66 324.81] | BUF [65 301.19 – 66 324.81] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S2 (oper, mixed): CORE [57 526.29 – 62 473.71] | BUF [56 777.09 – 62 473.71] 🔴 (силa=2/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S3 (struct, mixed): CORE [50 943.97 – 56 027.89] | BUF [50 740.68 – 56 777.09] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S4 (macro, mixed): CORE [45 392.61 – 50 537.39] | BUF [45 392.61 – 50 740.68] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
Сопротивления:
- R1 (oper, bounce): CORE [67 955.71 – 69 289.15] | BUF [67 955.71 – 69 413.99] 🟡 (силa=5/5, macro: tests=47, rr=0.979, fr=0.34)
- R2 (oper, bounce): CORE [69 538.84 – 71 014.82] | BUF [69 413.99 – 71 155.67] 🟡 (силa=5/5, macro: tests=47, rr=0.979, fr=0.34)
- R3 (struct, bounce): CORE [71 296.52 – 72 726.22] | BUF [71 155.67 – 74 399.27] 🟡 (силa=5/5, macro: tests=47, rr=0.979, fr=0.34)
- R4 (macro, bounce): CORE [78 905.19 – 79 814.81] | BUF [78 905.19 – 79 814.81] 🟡 (силa=5/5, macro: tests=47, rr=0.979, fr=0.34)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30

## ETHUSDT
- price(state): 1 949.58
- regime: chop | W1: down | D1: range
- ATR(D1): 149.17 | ATR(H4): 39.23
- EMA200(D1): 3 082.48 | EMA200(W1): 2 596.74

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, magnet): CORE [1 879.58 – 1 920.85] | BUF [1 879.58 – 1 920.85] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S2 (oper, magnet): CORE [1 645.84 – 1 834.16] | BUF [1 640.03 – 1 834.16] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S3 (struct, magnet): CORE [1 440.30 – 1 634.22] | BUF [1 440.30 – 1 640.03] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S4 (macro, magnet): CORE [1 044.63 – 1 335.37] | BUF [1 044.63 – 1 387.84] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
Сопротивления:
- R1 (oper, mixed): CORE [1 983.76 – 2 041.17] | BUF [1 983.76 – 2 048.60] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)
- R2 (oper, mixed): CORE [2 056.02 – 2 091.34] | BUF [2 048.60 – 2 110.70] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)
- R3 (struct, mixed): CORE [2 130.07 – 2 169.69] | BUF [2 110.70 – 2 200.19] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)
- R4 (macro, mixed): CORE [2 230.70 – 2 581.30] | BUF [2 200.19 – 2 949.60] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30
