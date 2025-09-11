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
metrex --datafolder <path> --timeframe <tf> --timerange <range> --output <file>
```

### Arguments

- `--datafolder`: Directory containing Freqtrade `.feather` candle files
- `--timeframe`: Timeframe to filter by (e.g., `1h`, `4h`, `1d`)
- `--timerange`: Time range in format `YYYYMMDD-YYYYMMDD` (e.g., `20230101-20231231`)
- `--output`: Output path for results `.feather` file

### Example

```bash
# Process 1-hour candles for Q2 2023
metrex --datafolder ./data/candles \
       --timeframe 1h \
       --timerange 20230401-20230630 \
       --output ./results/Q2_2023_metrics.feather
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

Results are saved as a `.feather` file containing:

| Column | Description |
|--------|-------------|
| `timestamp` | DateTime index |
| `breadth_above_sma_50` | Percentage (0-100) of symbols above SMA50 |
| `market_vol_regime` | Volatility regime: 'low', 'medium', or 'high' |
| `btc_trend_slope` | BTC trend slope from 20-period linear regression |

## Metrics Details

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
