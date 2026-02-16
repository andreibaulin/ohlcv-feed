IRON-PROOF (НЕ РЕДАКТИРОВАТЬ)
- generated_utc: 2026-02-16T19:51:14Z
- generated_local: 2026-02-16 21:51:14 EET
- state.updated_utc: 2026-02-16T19:51:13.907546Z
- state.sha256: 785c02def9b318939af81541d79155241fd578674c08fbcc05495898e826320c
- bundle.sha256: 7f5ed8e0a0b6fbd4c5e2a8b5f9b01fe2db86f5d93c7c6f046db7a96b72234a99

Ссылки (рукопожатие):
https://andreibaulin.github.io/ohlcv-feed/ta/binance/chat_bundle_latest.json
https://andreibaulin.github.io/ohlcv-feed/ta/binance/chat_report_latest.md
https://andreibaulin.github.io/ohlcv-feed/ta/binance/state_btc_eth_latest.json
https://andreibaulin.github.io/ohlcv-feed/ta/binance/state_btc_eth_full_latest.json
https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/pack_btc_eth.txt
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
- price(state): 67 539.52
- regime: chop | W1: range | D1: range
- ATR(D1): 4 837.69 | ATR(H4): 1 135.69
- EMA200(D1): 94 136.58 | EMA200(W1): 68 067.71

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (struct, magnet): CORE [50 952.84 – 66 939.16] | BUF [58 609.95 – 66 939.16] 🔴 (силa=5/5, macro: tests=66, rr=0.2576, fr=0.7424)
- S2 (oper, magnet): CORE [64 606.94 – 66 267.06] | BUF [63 875.72 – 63 875.72] 🔴 (силa=5/5, macro: tests=66, rr=0.2576, fr=0.7424)
- S3 (macro, magnet): CORE [56 855.50 – 63 144.50] | BUF [58 683.28 – 63 875.72] 🔴 (силa=2/5, macro: tests=66, rr=0.2576, fr=0.7424)
- S4 (oper, magnet): CORE [59 488.94 – 60 511.06] | BUF [45 492.77 – 58 683.28] 🔴 (силa=2/5, macro: tests=66, rr=0.2576, fr=0.7424)
Сопротивления:
- R1 (oper, mixed): CORE [67 899.46 – 69 345.40] | BUF [67 899.46 – 69 697.17] 🟡 (силa=5/5, macro: tests=31, rr=0.968, fr=0.419)
- R2 (oper, mixed): CORE [70 048.95 – 71 637.06] | BUF [69 697.17 – 75 243.00] 🟡 (силa=5/5, macro: tests=31, rr=0.968, fr=0.419)
- R3 (struct, mixed): CORE [78 848.94 – 79 871.06] | BUF [78 848.94 – 79 871.06] 🟡 (силa=5/5, macro: tests=31, rr=0.968, fr=0.419)
- R4 (macro, mixed): CORE [84 110.10 – 85 132.22] | BUF [81 990.58 – 96 758.59] 🟡 (силa=5/5, macro: tests=31, rr=0.968, fr=0.419)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30

## ETHUSDT
- price(state): 1 956.65
- regime: chop | W1: down | D1: range
- ATR(D1): 180.12 | ATR(H4): 46.82
- EMA200(D1): 3 116.00 | EMA200(W1): 2 596.74

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, magnet): CORE [1 907.81 – 1 949.95] | BUF [1 909.05 – 1 949.95] 🔴 (силa=5/5, macro: tests=101, rr=0.1584, fr=0.8416)
- S2 (struct, magnet): CORE [1 157.64 – 1 910.29] | BUF [1 511.26 – 1 909.05] 🔴 (силa=5/5, macro: tests=101, rr=0.1584, fr=0.8416)
- S3 (macro, magnet): CORE [1 630.72 – 1 864.88] | BUF [1 699.80 – 1 699.80] 🔴 (силa=5/5, macro: tests=101, rr=0.1584, fr=0.8416)
- S4 (oper, magnet): CORE [1 726.73 – 1 768.87] | BUF [1 157.64 – 1 699.80] 🔴 (силa=2/5, macro: tests=101, rr=0.1584, fr=0.8416)
Сопротивления:
- R1 (oper, mixed): CORE [1 980.35 – 2 036.88] | BUF [1 980.35 – 2 059.57] 🔴 (силa=5/5, macro: tests=30, rr=0.967, fr=0.567)
- R2 (oper, mixed): CORE [2 082.25 – 2 142.77] | BUF [2 059.57 – 2 232.76] 🔴 (силa=5/5, macro: tests=30, rr=0.967, fr=0.567)
- R3 (struct, mixed): CORE [2 322.75 – 2 364.89] | BUF [2 232.76 – 2 370.22] 🔴 (силa=5/5, macro: tests=30, rr=0.967, fr=0.567)
- R4 (macro, mixed): CORE [2 375.55 – 2 417.69] | BUF [2 370.22 – 3 150.62] 🔴 (силa=5/5, macro: tests=30, rr=0.967, fr=0.567)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30
