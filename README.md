# Polymarket Market Collector

Read-only collector for Polymarket event markets and CLOB orderbooks.

The default config tracks the active Elon Musk tweet-count market, but the
collector is query/filter driven so more subjects can be added without changing
code.

## What It Does

- Searches Polymarket Gamma public search for configured subjects.
- Falls back to Gamma `/events` search when public search is unavailable.
- Normalizes event and market metadata.
- Fetches CLOB orderbooks from the public `/book?token_id=...` endpoint.
- Persists JSONL snapshots by UTC date.
- Does not load wallet keys, sign messages, or place orders.

## Output

Default output directory:

```text
data/
  tweets_markets/
    elon-musk/
      latest_markets.json
      markets/YYYY-MM-DD.jsonl
      orderbooks/YYYY-MM-DD.jsonl
  mrbeast/
    will-mrbeast-hit-billion-views-by-may-31/
      latest_markets.json
      markets/YYYY-MM-DD.jsonl
      orderbooks/YYYY-MM-DD.jsonl
  weather_temperature/
    london/
      latest_markets.json
      markets/YYYY-MM-DD.jsonl
      orderbooks/YYYY-MM-DD.jsonl
state/
  run_state.json
```

Each `orderbooks/*.jsonl` row includes event metadata, market metadata, token id,
normalized `asks` and `bids`, source, and any fetch error.

## Local Run

```bash
cd /Users/cyberpunk/Code/polymarket_market_collector
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp config.example.json config.json
./.venv/bin/python collector.py --config config.json --once
```

Run continuously:

```bash
./.venv/bin/python collector.py --config config.json
```

## Add Another Subject

Edit `config.json`:

```json
{
  "name": "fed_rates",
  "query": "fed rates",
  "active": true,
  "include_terms": ["fed"],
  "exclude_terms": [],
  "limit_per_type": 20,
  "collect_outcomes": ["yes"]
}
```

`collect_outcomes` supports:

- `["yes"]`: only first CLOB token id, matching the existing trading pipeline.
- `["yes", "no"]`: first and second token ids.
- `["all"]`: all token ids returned by Gamma.

For Weather Temperature events, the default config uses the Gamma
`daily-temperature` tag and writes each city under its own folder, e.g.
`data/weather_temperature/london/`.

The default config also collects:

- `tweets_markets`: each personality/account in a separate folder, including Elon.
- `mrbeast`: each active MrBeast event in its own folder.
- `stocks_hit_price`: stock "What will ... hit ..." events, grouped by ticker.
- `indices_hit_price`: index hit-price events, grouped by index/ticker.
- `commodities_high_price`: commodity hit/above events, grouped by commodity.
- `forex_high_price`: FX hit/above events, grouped by currency pair.
- `crypto_daily`, `crypto_weekly`, `crypto_monthly`: grouped by crypto asset.

## EC2 Deployment

```bash
cd ~
git clone <repo-url> polymarket_market_collector
cd polymarket_market_collector
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp config.example.json config.json
./.venv/bin/python collector.py --config config.json --once
```

Install as systemd:

```bash
sudo cp polymarket-collector.service /etc/systemd/system/polymarket-collector.service
sudo systemctl daemon-reload
sudo systemctl enable polymarket-collector
sudo systemctl start polymarket-collector
journalctl -u polymarket-collector -f
```

If you deploy under a different path or user, edit `polymarket-collector.service`
before copying it.
