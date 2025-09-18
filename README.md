# metrex
External market metrics for trading strategies — breadth, volatility, and trend context from your local data.

## Overview

Metrex is a Python CLI tool that processes Freqtrade `.feather` candle data to compute essential market metrics for trading strategies. It filters data by timeframe and timerange, then calculates three key metrics:

- **breadth_above_sma_50**: Percentage of symbols trading above their 50-period Simple Moving Average
- **market_vol_regime**: Market volatility regime classification (high/medium/low)
- **btc_trend_slope**: Bitcoin trend slope using 20-period linear regression

## Installation

Install metrex directly from the repository:

```bash
pip install -e .
```

## Usage

```bash
# Market-level metrics
metrex metrics --datafolder <path> --timeframe <tf> --timerange <range> \
               --metrics <name1,name2,...> --output <file>

# Or run all registered metrics
metrex metrics --datafolder <path> --timeframe <tf> --timerange <range> \
               --all-metrics --output <file>

# Per-pair ranking rolls (one feather per pair)
metrex rank --datafolder <path> --timeframe <tf> --timerange <range> \
            --outputfolder <dir>

# List available metric names
metrex list
```

### Arguments

- `--datafolder`: Directory containing Freqtrade `.feather` candle files
- `--timeframe`: Timeframe to filter by (e.g., `1h`, `4h`, `1d`)
- `--timerange`: Time range in format `YYYYMMDD-YYYYMMDD`|`latest-YYYYMMDD` (e.g., `20230101-20231231`, `latest-20231231`), in case of sending `latest` instead of the start date, the system shall use the end date from the corresponding output file, if the corresponding file does not exist, the system shall use the start date from the input file.
- `--output`: Output path for results `.feather` file

### Example

```bash
# Process 1-hour candles
metrex rank --datafolder ./data/candles \
       --timeframe 1h \
       --timerange yyyyMMdd-yyyyMMdd \
       --outputfolder ./results

metrex rank --datafolder ./data/candles \
       --timeframe 1m \
       --timerange latest-yyyyMMdd \
       --outputfolder ./results

metrex metrics --datafolder ./data/candles \
       --timeframe 1h \
       --timerange yyyyMMdd-yyyyMMdd \
       --all-metrics \
       --output ./results/market_metrics.feather

python -c "import pandas as pd; df = pd.read_feather('./results/market_metrics.feather'); df.to_csv('./results/market_metrics.csv', index=False)"
```

## Input Data Format

Metrex expects Freqtrade `.feather` files with the naming convention:
```
SYMBOL-TIMEFRAME.feather
```

Examples:
- `BTC_USDT-1h.feather`
- `ETH_USDT-4h.feather`
- `ADA_USDT-1d.feather`

Each file should contain OHLCV data with columns:
- `timestamp` (or `date`): DateTime index
- `open`: Opening price
- `high`: High price  
- `low`: Low price
- `close`: Closing price
- `volume`: Trading volume

## Output Format

Results are saved as a `.feather` file containing a `date` column plus the
columns produced by the selected metrics. Examples include:

| Column | Description |
|--------|-------------|
| `breadth_above_sma_50` | Percentage (0-100) of symbols above SMA50 |
| `market_vol_regime` | Volatility regime: 'low', 'medium', or 'high' |
| `vol_zscore` | Z-score of BTC realized volatility |
| `btc_trend_slope` | BTC trend slope from 20-period linear regression |
| `avg_corr_btc` | Cross-sectional average corr to BTC (rolling 50) |
| `new_highs_50` / `new_lows_50` | Count of new 50-period highs/lows |
| `mkt_ret` / `mkt_ret_sma20` | Mean market return and its 20SMA |

## Metrics Details

### Available Metrics

These names can be listed with `metrex list` and used with `--metrics`:

- breadth_sma50: Emits `breadth_above_sma_50` (% above 50SMA)
- market_vol_regime: Emits `market_vol_regime`, `vol_zscore`
- btc_trend_slope: Emits `btc_trend_slope`
- adv_decline: Emits `adv_count`, `decl_count`, `adv_decline_diff`, `adv_decline_line`
- new_highs_lows: Emits `new_highs_50`, `new_lows_50`
- volume_surge_ratio: Emits `volume_surge_ratio`
- avg_correlation_btc: Emits `avg_corr_btc`
- market_return_ma: Emits `mkt_ret`, `mkt_ret_sma20`

### Breadth Above SMA 50
Calculates the percentage of symbols trading above their 50-period Simple Moving Average at each timestamp. Values range from 0% to 100%, providing insight into overall market strength.

### Market Volatility Regime
Classifies market volatility into three regimes based on historical percentiles:
- **Low**: Below 33rd percentile of historical volatility
- **Medium**: Between 33rd and 67th percentiles  
- **High**: Above 67th percentile

Uses BTC volatility as a proxy for overall market conditions.

### BTC Trend Slope
Computes the slope of Bitcoin price trend using 20-period linear regression. Positive values indicate uptrend, negative values indicate downtrend. Magnitude indicates trend strength.

## Error Handling

Metrex includes comprehensive error handling for:
- Invalid date formats in timerange
- Missing or inaccessible data directories
- No matching `.feather` files for specified timeframe
- Insufficient data for metric calculations
- Missing required columns in input data

## Dependencies

- pandas >= 1.3.0
- pyarrow >= 5.0.0
- numpy >= 1.21.0
- click >= 8.0.0

## Contributing

See `CONTRIBUTING.md` for setup, coding guidelines, and how to add new metrics. Issue and PR templates are available under `.github/`.
