IRON-PROOF (НЕ РЕДАКТИРОВАТЬ)
- generated_utc: 2026-02-16T14:07:10Z
- generated_local: 2026-02-16 16:07:10 EET
- state.updated_utc: 2026-02-16T10:01:15.887598Z
- state.sha256: f49d41dce46936ec0270a2d7ad99d9421436dc48859da4088859eb551b35a990
- bundle.sha256: 18d6b6360a9d85f98ab01d08f0ce7d300e65123b8097225afa11c56a939b6495

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
- price(state): 68 587.90
- regime: chop | W1: range | D1: range
- ATR(D1): 4 837.69 | ATR(H4): 951.93
- EMA200(D1): 94 136.58 | EMA200(W1): 68 067.71

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, bounce): CORE [66 242.08 – 67 098.82] | BUF [66 213.23 – 68 262.50] 🟢 (силa=5/5, macro: tests=17, rr=1.0, fr=0.235)
- S2 (oper, bounce): CORE [65 327.63 – 66 184.37] | BUF [65 128.00 – 66 213.23] 🟢 (силa=5/5, macro: tests=17, rr=1.0, fr=0.235)
- S3 (struct, bounce): CORE [64 071.63 – 64 928.37] | BUF [62 250.00 – 65 128.00] 🟢 (силa=4/5, macro: tests=17, rr=1.0, fr=0.235)
- S4 (macro, bounce): CORE [59 571.63 – 60 428.37] | BUF [50 952.84 – 62 250.00] 🟢 (силa=4/5, macro: tests=17, rr=1.0, fr=0.235)
Сопротивления:
- R1 (oper, mixed): CORE [69 054.60 – 70 422.02] | BUF [69 054.60 – 70 872.49] 🟡 (силa=5/5, macro: tests=31, rr=0.968, fr=0.419)
- R2 (oper, mixed): CORE [71 322.96 – 72 699.78] | BUF [70 872.49 – 75 415.91] 🟡 (силa=5/5, macro: tests=31, rr=0.968, fr=0.419)
- R3 (struct, mixed): CORE [78 931.63 – 79 788.37] | BUF [78 931.63 – 79 788.37] 🟡 (силa=5/5, macro: tests=31, rr=0.968, fr=0.419)
- R4 (macro, mixed): CORE [84 192.79 – 85 049.53] | BUF [81 990.58 – 96 758.59] 🟡 (силa=4/5, macro: tests=31, rr=0.968, fr=0.419)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30

## ETHUSDT
- price(state): 1 971.13
- regime: chop | W1: down | D1: range
- ATR(D1): 180.12 | ATR(H4): 40.02
- EMA200(D1): 3 116.00 | EMA200(W1): 2 596.74

### 4 поддержки / 4 сопротивления (CORE = точка реакции, BUFFER = зона допуска)
Поддержки:
- S1 (oper, magnet): CORE [1 910.87 – 1 946.89] | BUF [1 910.58 – 1 946.89] 🔴 (силa=5/5, macro: tests=101, rr=0.1584, fr=0.8416)
- S2 (macro, magnet): CORE [1 157.64 – 1 910.29] | BUF [1 520.89 – 1 910.29] 🔴 (силa=5/5, macro: tests=101, rr=0.1584, fr=0.8416)
- S3 (oper, magnet): CORE [1 848.12 – 1 884.14] | BUF [1 806.96 – 1 806.96] 🔴 (силa=2/5, macro: tests=101, rr=0.1584, fr=0.8416)
- S4 (struct, magnet): CORE [1 729.79 – 1 765.81] | BUF [1 157.64 – 1 806.96] 🔴 (силa=2/5, macro: tests=101, rr=0.1584, fr=0.8416)
Сопротивления:
- R1 (oper, magnet): CORE [1 983.41 – 2 033.82] | BUF [1 983.41 – 2 044.74] 🔴 (силa=5/5, macro: tests=33, rr=0.939, fr=0.758)
- R2 (oper, magnet): CORE [2 055.67 – 2 091.69] | BUF [2 044.74 – 2 110.70] 🔴 (силa=5/5, macro: tests=33, rr=0.939, fr=0.758)
- R3 (struct, magnet): CORE [2 129.72 – 2 170.04] | BUF [2 110.70 – 2 247.93] 🔴 (силa=5/5, macro: tests=33, rr=0.939, fr=0.758)
- R4 (macro, magnet): CORE [2 325.81 – 2 361.83] | BUF [2 247.93 – 3 150.62] 🔴 (силa=5/5, macro: tests=33, rr=0.939, fr=0.758)

### Деривативы (live ссылки Binance FAPI)
- premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT
- openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT
- openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30
- fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30
