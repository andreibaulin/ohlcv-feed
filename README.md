# ohlcv-feed (Binance) — OHLCV + Derivatives + TA State

Этот репозиторий генерирует данные на GitHub Pages:

1) **OHLCV pack** (BTC/ETH по умолчанию)
2) **Derivatives snapshot** (funding + open interest + long/short)
3) **TA state** (консервативные зоны/уровни по W1/D1 + рабочий буфер H4)

## 1) Самые важные ссылки

### OHLCV pack (главное)
`pack_btc_eth.txt` — список ссылок для загрузки данных:

https://andreibaulin.github.io/ohlcv-feed/ohlcv/binance/pack_btc_eth.txt

### Derivatives (funding / OI / long-short)

https://andreibaulin.github.io/ohlcv-feed/deriv/binance/core5_latest.json

### TA state (уровни/зоны)

https://andreibaulin.github.io/ohlcv-feed/ta/binance/state_btc_eth_latest.json

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
- `DERIV_SYMBOLS` (пример: `BTCUSDT,ETHUSDT`)
- `TA_SYMBOLS` (пример: `BTCUSDT,ETHUSDT`)

## 4) Примечания

- `*_tail*_chunks.json` + `*_p###.json` — специально сделаны так, чтобы части были **многострочными JSON** (читаются стабильнее в браузере/клиентах).
- TA state — **не прогноз**, а “слепок” зон/структуры для дальнейшей работы.
