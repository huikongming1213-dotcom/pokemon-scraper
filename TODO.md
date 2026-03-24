# TODO — 正式版待做

## 下版本新增 Sources

### SNKR Dunk（Playwright）
- URL: `https://snkrdunk.com/en/pokemon-cards?q={card_name}`
- 爬最近成交價或 listing 價
- 結果 JPY → HKD
- 檔案：`scrapers/snkr_dunk.py`

### Card Rush（Playwright）
- URL: `https://www.cardrush-pokemon.jp/`
- 爬收購價（買入價）
- 結果 JPY → HKD
- 檔案：`scrapers/card_rush.py`

### PokemonPriceTracker
- 需要確認 API endpoint（RapidAPI 或直接 API）
- 需要 API key：`POKEMON_PRICE_TRACKER_KEY`
- 目標：market_price, PSA 9/10 均價, 30日走勢
- 檔案：`apis/pokemon_price_tracker.py`

## 其他優化
- [ ] 加 `.dockerignore`（排除 `.env`, `TODO.md`, `__pycache__`）
- [ ] 加 `/health` endpoint
- [ ] 考慮加 response caching（同一張卡 5 分鐘內唔重複爬）
