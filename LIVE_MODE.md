# Live Data Mode - Architecture & Usage

## Overview

Metrex now supports **live data mode** in addition to the existing offline mode. You can fetch real-time OHLCV data from cryptocurrency exchanges (via CCXT) and compute market metrics and rankings continuously.

## Architecture

### Files Structure

```
metrex/
├── cli.py                 # CLI interface with live/offline mode support
├── live_runner.py         # NEW: Live data fetching and scheduling
├── processor.py           # Shared metric/ranking computation logic
├── io.py                  # Data loading/saving utilities
├── timeutils.py           # Time range filtering utilities
└── metrics/               # Market metric implementations
    ├── __init__.py
    ├── base.py
    ├── breadth_sma50.py
    ├── btc_trend_slope.py
    ├── market_vol_regime.py
    └── ...
```

### Key Design Principles

1. **Shared Logic**: Both live and offline modes use the same `run_metrics()` and `rank_pairs()` functions from `processor.py`
2. **Schema Compatibility**: Live data from `get_live_data()` returns a DataFrame matching the offline schema: `['date', 'pair', 'open', 'high', 'low', 'close', 'volume']`
3. **No Duplication**: Metric computation logic is implemented once and reused
4. **Append-Only**: Live mode appends new data to existing files, avoiding duplicates

### Core Functions

#### `metrex/live_runner.py`

- **`get_live_data(symbols, timeframe, limit, exchange_id)`**: Fetches OHLCV data from exchange
- **`run_live_metrics(...)`**: Continuously fetches data and computes metrics
- **`run_live_ranking(...)`**: Continuously fetches data and computes rankings
- **`run_live_combined(...)`**: Runs both metrics and rankings in one loop

## Installation

Install the updated dependencies including CCXT:

```bash
pip install -e .
```

Or manually:

```bash
pip install ccxt>=4.0.0
```

## Usage

### 1. Offline Mode (Original Functionality)

Works exactly as before with local `.feather` files:

```bash
# Compute metrics from local data
metrex metrics \
  --datafolder ./data \
  --timeframe 1h \
  --timerange 20230101-20231231 \
  --all-metrics \
  --output metrics.feather

# Generate rankings from local data
metrex rank \
  --datafolder ./data \
  --timeframe 1h \
  --timerange 20230101-20231231 \
  --outputfolder ./ranks
```

### 2. Live Metrics Mode

Fetch live data and compute metrics continuously:

```bash
# Fetch live data and compute all metrics every 60 seconds
metrex metrics \
  --live \
  --symbols BTC/USDT,ETH/USDT,BNB/USDT \
  --timeframe 1m \
  --interval 60 \
  --all-metrics \
  --output live_metrics.feather

# Compute specific metrics
metrex metrics \
  --live \
  --symbols BTC/USDT,ETH/USDT \
  --timeframe 5m \
  --interval 300 \
  --metrics breadth_sma50,market_vol_regime \
  --output live_metrics.feather
```

**Output**: Appends new metric rows to `live_metrics.feather` each cycle.

### 3. Live Ranking Mode

Fetch live data and compute rankings continuously:

```bash
# Compute rankings every 120 seconds
metrex rank \
  --live \
  --symbols BTC/USDT,ETH/USDT,XRP/USDT,ADA/USDT \
  --timeframe 1m \
  --interval 120 \
  --outputfolder ./live_ranks
```

**Output**: Creates/updates per-pair feather files in `./live_ranks/` with columns:
- `date`, `open`, `high`, `low`, `close`, `volume`
- `pairsCount`, `changePercentage24h`
- `topGainerRank`, `topLooserRank`
- `volumeInCurrency`, `volumeInCurrency24`
- `topVolumeRank`, `bottomVolumeRank`

### 4. Combined Live Mode (Recommended)

Run both metrics and rankings in a single command:

```bash
metrex live \
  --symbols BTC/USDT,ETH/USDT,XRP/USDT,ADA/USDT,SOL/USDT \
  --timeframe 1m \
  --interval 60 \
  --all-metrics \
  --metrics-output live_metrics.feather \
  --ranks-output live_ranks
```

**Output**:
- Metrics saved to `live_metrics.feather`
- Rankings saved to `live_ranks/<PAIR>-<TIMEFRAME>.feather`
- Console displays latest metrics and top movers each cycle

### Options Reference

#### Common Options

- `--timeframe`: Candle timeframe (`1m`, `5m`, `15m`, `1h`, `4h`, `1d`, etc.)
- `--live`: Enable live mode (flag)
- `--symbols`: Comma-separated trading pairs (format: `BTC/USDT,ETH/USDT`)
- `--interval`: Update interval in seconds (default: 60 for metrics/live, 120 for rank)
- `--exchange`: Exchange ID (default: `binance`, also supports `coinbase`, `kraken`, etc.)

#### Metrics Options

- `--metrics`: Comma-separated metric names
- `--all-metrics`: Compute all registered metrics (flag)
- `--output`: Output file path

#### Ranking Options

- `--outputfolder`: Output folder for per-pair rank files

#### Offline Options

- `--datafolder`: Path to local `.feather` data
- `--timerange`: Time range (`YYYYMMDD-YYYYMMDD` or `latest-YYYYMMDD`)

## Available Metrics

List all registered metrics:

```bash
metrex list
```

Current metrics include:
- `breadth_sma50` - % of pairs trading above 50-period SMA
- `btc_trend_slope` - BTC price trend slope
- `market_vol_regime` - Market volatility regime classification
- `adv_decline` - Advance/decline ratio
- `new_highs_lows` - New highs vs new lows
- `volume_surge_ratio` - Volume surge detection
- `avg_correlation_btc` - Average correlation with BTC
- `market_return_ma` - Market return moving average

## Console Output

Live mode provides rich console logging:

```
==============================================================
Iteration 1 - 2025-10-14 12:34:56 UTC
==============================================================
Fetched 1000 candles for BTC/USDT
Fetched 1000 candles for ETH/USDT
Total: fetched 2000 rows for 2 pairs
📊 Appended 1 new metric rows

📊 Latest Metrics:
  breadth_above_sma_50: 65.0000
  market_vol_regime: 2.0000
  btc_trend_slope: 0.0123

📈 Top Movers (24h):
  BTCUSDT | 24h: +2.34% | Gainer Rank: 1 | Volume Rank: 1
  ETHUSDT | 24h: +1.87% | Gainer Rank: 2 | Volume Rank: 2

⏱️  Next update in 60.0s...
```

## Advanced Usage

### Custom Exchange

Use a different exchange (must be supported by CCXT):

```bash
metrex live \
  --symbols BTC/USD,ETH/USD \
  --timeframe 1h \
  --interval 300 \
  --exchange coinbase \
  --all-metrics
```

### Different Timeframes

Match exchange's available timeframes:

```bash
# 1-minute candles (high frequency)
metrex live --symbols BTC/USDT,ETH/USDT --timeframe 1m --interval 60 --all-metrics

# 5-minute candles
metrex live --symbols BTC/USDT,ETH/USDT --timeframe 5m --interval 300 --all-metrics

# 1-hour candles
metrex live --symbols BTC/USDT,ETH/USDT --timeframe 1h --interval 3600 --all-metrics
```

### Background Execution

Run in background with output logging:

```bash
nohup metrex live \
  --symbols BTC/USDT,ETH/USDT,XRP/USDT \
  --timeframe 1m \
  --interval 60 \
  --all-metrics \
  > metrex_live.log 2>&1 &
```

Stop with:
```bash
ps aux | grep metrex
kill <PID>
```

## Data Schema

### Market Data (Input to Metrics)

```python
{
    'date': pd.Timestamp,      # UTC timestamp
    'pair': str,                # e.g., 'BTCUSDT'
    'open': float,
    'high': float,
    'low': float,
    'close': float,
    'volume': float
}
```

### Metrics Output

```python
{
    'date': pd.Timestamp,               # UTC timestamp
    'breadth_above_sma_50': float,      # 0-100 percentage
    'market_vol_regime': float,         # Classification value
    'btc_trend_slope': float,           # Slope coefficient
    # ... other metrics
}
```

### Rankings Output (per pair)

```python
{
    'date': pd.Timestamp,
    'open': float,
    'high': float,
    'low': float,
    'close': float,
    'volume': float,
    'pairsCount': int,                  # Number of pairs in comparison
    'changePercentage24h': float,       # 24h price change %
    'topGainerRank': int,               # Rank by gain (1 = best)
    'topLooserRank': int,               # Rank by loss (1 = worst)
    'volumeInCurrency': float,          # volume * close
    'volumeInCurrency24': float,        # 24h rolling sum
    'topVolumeRank': int,               # Rank by volume (1 = highest)
    'bottomVolumeRank': int             # Rank by volume (1 = lowest)
}
```

## Error Handling

Live mode includes robust error handling:

1. **Exchange Errors**: Logged but don't stop execution
2. **Symbol Failures**: Individual symbols that fail are skipped
3. **Rate Limiting**: CCXT handles rate limiting automatically
4. **Keyboard Interrupt**: Gracefully stops on Ctrl+C

## Performance Notes

- **Interval**: Set based on exchange rate limits and data needs
- **Symbol Count**: More symbols = longer fetch time
- **Historical Limit**: Default 1000 candles provides good context for metrics
- **Timeframe**: Lower timeframes (1m, 5m) require more frequent updates

## Backward Compatibility

All existing offline mode commands work exactly as before:

```bash
# Original commands still work unchanged
metrex metrics --datafolder ./data --timeframe 1h --timerange 20230101-20231231 --all-metrics --output metrics.feather
metrex rank --datafolder ./data --timeframe 1h --timerange 20230101-20231231 --outputfolder ./ranks
metrex list
```

## Testing

To test the live mode locally:

```bash
# Quick test with 3 iterations
python -c "
from metrex.live_runner import run_live_combined
from pathlib import Path

run_live_combined(
    symbols=['BTC/USDT', 'ETH/USDT'],
    timeframe='1m',
    metric_names=['breadth_sma50', 'market_vol_regime'],
    interval=10,
    metrics_output=Path('test_metrics.feather'),
    ranks_output=Path('test_ranks'),
    max_iterations=3
)
"
```

## Troubleshooting

### Import Error: ccxt

```bash
pip install ccxt>=4.0.0
```

### Symbol Not Found

Verify symbol format matches exchange (use `/` separator: `BTC/USDT`)

### Rate Limit Errors

Increase `--interval` value or reduce number of `--symbols`

### No Data Returned

Check if exchange supports the requested timeframe and symbols

## Future Enhancements

Potential additions:
- WebSocket streaming for real-time updates
- Multiple exchange support in single run
- Configurable historical depth
- Alert system for metric thresholds
- REST API server mode
