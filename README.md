# TRMNL X — Binance Perpetual 4H OI & Price Monitor

This package builds the two-panel TRMNL X dashboard designed for Leo:

- Left: current OI USD, rolling 4h OI change, rolling 4h mark-price change, and position-building signal.
- Right: rolling 4h price gainers and losers.
- Universe: all actively trading Binance USDⓈ-M, COIN-M and TradFi perpetual contracts.
- Restart-safe: every run queries Binance's recent 5-minute history. The Mac does not need to stay on for four hours.
- No Binance API key is required; only public market-data endpoints are used.

## What the fallback does

1. Each run selects the closest Binance observation to `now - 4h` within ±15 minutes.
2. A temporarily failed symbol may use a local cache no older than 30 minutes.
3. If fewer than 60% of the universe succeeds, nothing is pushed. TRMNL keeps the last good screen.
4. New contracts without four hours of history are omitted instead of showing a fake percentage.
5. TradFi symbols are collected but excluded from rankings while their underlying market is closed.

## Files

- `binance_dashboard.py` — collector, calculation, fallback and webhook push.
- `trmnl_full_markup.html` — paste into the TRMNL Private Plugin **Full** markup tab.
- `config.example.json` — local Mac configuration template.
- `.github/workflows/update-trmnl.yml` — cloud schedule every 10 minutes.
- `install_mac_schedule.sh` — optional local `launchd` fallback.
- `SETUP_GUIDE.html` — detailed Traditional Chinese setup guide with copy buttons.

## Quick local test

```bash
cd "$HOME/Documents/temnl/trmnl-binance"
cp config.example.json config.json
open -e config.json
```

Paste the TRMNL webhook URL into `config.json`, then run:

```bash
python3 binance_dashboard.py --config config.json --dry-run --max-symbols 30
```

After that succeeds, push the full universe:

```bash
python3 binance_dashboard.py --config config.json
```

The full run makes roughly two public Binance requests per perpetual contract and can take a few minutes.

## Run tests

```bash
python3 -m unittest discover -s tests -v
```

## Data interpretation

| OI | Price | Signal |
| --- | --- | --- |
| Up | Up | Long Build |
| Up | Down | Short Build |
| Down | Up | Short Cover |
| Down | Down | Long Unwind |

The left panel ranks by absolute change in OI USD, not merely percentage. The default `$5M` OI floor prevents tiny contracts from dominating the price-mover lists.
