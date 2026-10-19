# SCALPEX — ICT + Hidden Bullish Divergence (1H)

## Start
1. Extract this ZIP to a writable folder.
2. Run `START_HERE.bat` on Windows.
3. Log in using configured credentials (fresh local install defaults: `admin` / `Scalpex@2026`).
4. Start either strategy from its own controls. Both strategies use the shared Binance Spot market-data feed.

## Shared Master Scanner
- Loads eligible Binance Spot USDT trading pairs from Binance exchange information.
- Loads up to 230 recent 1H candles per pair, then subscribes to live 1H kline updates.
- History requests retry transient network/rate-limit/server errors.
- Dashboard reports loaded pair count and current feed status. Actual pair counts depend on Binance's current eligible markets and successful requests.

## ICT Method 1
- Existing ICT-based setup logic is retained.
- Existing final-15-minute watch and manual result/report workflow remain.

## Hidden Bullish Divergence — 1H
- Uses the shared 1H candle cache; no separate duplicate history loader.
- RSI period: 14; price low is taken from the candle wick.
- First Low is the latest confirmed swing low used as the reference.
- The new lower wick must occur 3–30 candles after First Low.
- Only the current live candle that makes a fresh lower low (below First Low and below intervening candles since First Low) can appear in Watching List during the final 15 minutes of the 1H candle. This prevents old, already-formed lows from being shown again.
- At candle close, price must remain a Lower Low and the closing RSI(14) must be strictly higher than RSI at First Low. Otherwise the watch is removed.
- Valid setups move to Identified Setups and use the next candle open when available as entry price.
- Identified setups include manual TP/SL fields, manual trade close date/time, TP HIT / SL HIT, Skip Trade, and Reports.
- Signals are technical observations, not guarantees; no automatic Binance orders are placed.

## Controls and safety
- ICT and Hidden Bullish have independent strategy controls but share market data. The feed remains active while either strategy is running.
- Use the dashboard theme button to switch Light/Dark.
- Internet access and packages in `requirements.txt` are required.
- Change default login credentials and `SECRET_KEY` in `.env` before exposing the app to any network.
