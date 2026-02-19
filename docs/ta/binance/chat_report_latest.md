IRON-PROOF (НЕ РЕДАКТИРОВАТЬ)
- generated_utc: 2026-02-19T21:56:09Z
- generated_local: 2026-02-19 23:56:09 EET
- state.updated_utc: 2026-02-19T21:56:09.665573Z
- state.sha256: 46239146009ab2bce20197368b65af9b996f799e1f032f9577f7fc77f6da36ab
- bundle.sha256: e1575389e375aef9e34e091c9425705aecbf11204c5c314b1c3acf212ba43c7d

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
- price(state): 67 050.00
- regime: chop | W1: range | D1: range
- ATR(D1): 4 264.72 | ATR(H4): 1 069.10
- EMA200(D1): 93 352.62 | EMA200(W1): 68 067.71

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, mixed): CORE [65 388.91 – 66 351.09] | BUF [65 388.91 – 66 351.09] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S2 (oper, mixed): CORE [57 526.29 – 62 473.71] | BUF [56 777.09 – 62 473.71] 🔴 (силa=2/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S3 (struct, mixed): CORE [50 943.97 – 56 027.89] | BUF [50 740.68 – 56 777.09] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
- S4 (macro, mixed): CORE [45 392.61 – 50 537.39] | BUF [45 392.61 – 50 740.68] 🔴 (силa=5/5, macro: tests=30, rr=0.4667, fr=0.5333)
Сопротивления:
- R1 (oper, bounce): CORE [67 929.43 – 69 315.43] | BUF [67 929.43 – 69 480.51] 🟡 (силa=5/5, macro: tests=49, rr=0.98, fr=0.327)
- R2 (oper, bounce): CORE [69 645.58 – 71 607.09] | BUF [69 480.51 – 74 399.27] 🟡 (силa=5/5, macro: tests=49, rr=0.98, fr=0.327)
- R3 (struct, bounce): CORE [78 878.91 – 79 841.09] | BUF [78 878.91 – 79 841.09] 🟡 (силa=5/5, macro: tests=49, rr=0.98, fr=0.327)
- R4 (macro, bounce): CORE [84 140.07 – 85 102.25] | BUF [84 140.07 – 96 054.50] 🟡 (силa=5/5, macro: tests=49, rr=0.98, fr=0.327)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30

## ETHUSDT
- price(state): 1 941.34
- regime: chop | W1: down | D1: range
- ATR(D1): 149.17 | ATR(H4): 40.82
- EMA200(D1): 3 082.48 | EMA200(W1): 2 596.74

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, magnet): CORE [1 878.87 – 1 921.56] | BUF [1 878.87 – 1 921.56] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S2 (oper, magnet): CORE [1 645.84 – 1 834.16] | BUF [1 640.03 – 1 834.16] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S3 (struct, magnet): CORE [1 440.30 – 1 634.22] | BUF [1 440.30 – 1 640.03] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
- S4 (macro, magnet): CORE [1 044.63 – 1 335.37] | BUF [1 044.63 – 1 387.84] 🔴 (силa=5/5, macro: tests=69, rr=0.3043, fr=0.6957)
Сопротивления:
- R1 (oper, mixed): CORE [1 969.18 – 2 041.88] | BUF [1 969.18 – 2 048.60] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)
- R2 (oper, mixed): CORE [2 055.31 – 2 092.05] | BUF [2 048.60 – 2 110.70] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)
- R3 (struct, mixed): CORE [2 129.36 – 2 170.40] | BUF [2 110.70 – 2 200.55] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)
- R4 (macro, mixed): CORE [2 230.70 – 2 581.30] | BUF [2 200.55 – 2 949.60] 🔴 (силa=5/5, macro: tests=42, rr=0.976, fr=0.571)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30
