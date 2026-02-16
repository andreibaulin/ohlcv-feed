# ohlcv-feed (Binance) — OHLCV + TA State + IRON chat bundle

Этот репозиторий генерирует данные на GitHub Pages:

1) **OHLCV pack** (BTC/ETH по умолчанию)
2) **TA state** (консервативные зоны/уровни по W1/D1 + рабочий буфер H4)
3) **IRON chat bundle** (готовый отчёт: 4 поддержки + 4 сопротивления)


## 1) Самые важные ссылки

### OHLCV pack (главное)
`pack_btc_eth.txt` — список ссылок для загрузки данных:

https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/pack_btc_eth.txt



### Деривативы (live, без GitHub)
Если нужны funding/OI/premium — тяни напрямую из Binance FAPI:

- BTC premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
- BTC openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
- BTC openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=1h&limit=30
- BTC fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=30
- ETH premiumIndex: https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT
- ETH openInterest: https://fapi.binance.com/fapi/v1/openInterest?symbol=ETHUSDT
- ETH openInterestHist(1h×30): https://fapi.binance.com/futures/data/openInterestHist?symbol=ETHUSDT&period=1h&limit=30
- ETH fundingRate(×30): https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=30


### TA state (уровни/зоны)

**SWING (по умолчанию):** W1 range (premium/discount + equilibrium) + D1 swing + EMA200 + execution bands + реакция зоны (tests/reaction_rate/failure_rate + days_since_last_test/days_since_last_reaction)

https://andreibaulin.github.io/ohlcv-feed/ta/binance/state_btc_eth_latest.json

**FULL (по запросу):** включает локальные H4 зоны (`local_h4`) и рабочий буфер (`working_h4`) + всё из SWING

https://andreibaulin.github.io/ohlcv-feed/ta/binance/state_btc_eth_full_latest.json

## 2) Как это обновляется

Workflow: `.github/workflows/binance_all.yml`

Шаги:
1) `scripts/gen_pack_btc_eth.py` → OHLCV хвосты (chunks + parts) + pack
2) `scripts/build_deriv_binance.py` → funding/OI/long-short (USDⓈ-M futures)
3) `scripts/build_ta_state.py` → уровни/зоны (пивоты + ATR)

## 3) Настройка символов (опционально)

По умолчанию генерируются только **BTCUSDT** и **ETHUSDT**.

Можно расширить через переменные окружения в GitHub Actions:

- `OHLCV_SYMBOLS` (пример: `BTCUSDT,ETHUSDT,SOLUSDT`)
- `TA_SYMBOLS` (пример: `BTCUSDT,ETHUSDT`)

## 4) Примечания

- `*_tail*_chunks.json` + `*_p###.json` — специально сделаны так, чтобы части были **многострочными JSON** (читаются стабильнее в браузере/клиентах).
- TA state — **не прогноз**, а “слепок” зон/структуры для дальнейшей работы.
