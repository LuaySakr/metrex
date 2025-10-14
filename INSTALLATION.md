# Quick Installation & Setup Guide

## Prerequisites

- Python 3.8 or higher
- pip package manager

## Installation

### 1. Install Metrex

From the repository root:

```bash
cd /Users/luaysakr/Documents/Luay/github/metrex
pip install -e .
```

This will install all dependencies including:
- pandas >= 1.3.0
- pyarrow >= 5.0.0
- numpy >= 1.21.0
- click >= 8.0.0
- ccxt >= 4.0.0

### 2. Verify Installation

```bash
# Check if metrex command is available
metrex --help

# List available metrics
metrex list

# Run test suite
python3 test_live_mode.py
```

Expected output:
```
============================================================
METREX LIVE MODE TEST SUITE
============================================================
Testing imports...
✅ All imports successful
...
============================================================
TEST RESULTS
============================================================
Passed: 7/7

🎉 All tests passed!
```

## Quick Start

### Offline Mode (Original)

If you have local `.feather` candle data:

```bash
metrex metrics \
  --datafolder ./data \
  --timeframe 1h \
  --timerange 20230101-20231231 \
  --all-metrics \
  --output metrics.feather
```

### Live Mode (New)

Fetch data from Binance:

```bash
# Simple test (stop with Ctrl+C after a few cycles)
metrex live \
  --symbols BTC/USDT,ETH/USDT \
  --timeframe 1m \
  --interval 60 \
  --all-metrics
```

Expected output:
```
🔴 Starting COMBINED LIVE mode (metrics + ranking)...
Symbols: BTC/USDT, ETH/USDT
Timeframe: 1m
Metrics: breadth_sma50, btc_trend_slope, market_vol_regime, ...
Interval: 60s
Press Ctrl+C to stop.

==============================================================
Iteration 1 - 2025-10-14 12:34:56 UTC
==============================================================
Fetched 1000 candles for BTC/USDT
Fetched 1000 candles for ETH/USDT
...
```

## Troubleshooting

### Issue: `ccxt` module not found

**Solution:**
```bash
pip install ccxt>=4.0.0
```

### Issue: `metrex` command not found

**Solution:**
```bash
# Reinstall in editable mode
pip install -e .

# Or add to PATH (if using --user)
export PATH="$HOME/.local/bin:$PATH"
```

### Issue: Exchange returns error

**Solution:**
- Check internet connection
- Verify symbol format: use `BTC/USDT` (with slash), not `BTCUSDT`
- Try a different exchange: `--exchange coinbase`
- Increase interval to avoid rate limits: `--interval 300`

### Issue: Symbol not found

**Solution:**
Exchange may not support that symbol. Check available symbols:
```python
import ccxt
exchange = ccxt.binance()
markets = exchange.load_markets()
print([m for m in markets if 'USDT' in m])
```

## Examples

### Example 1: Simple Live Metrics
```bash
metrex metrics --live \
  --symbols BTC/USDT,ETH/USDT \
  --timeframe 1m \
  --interval 60 \
  --metrics breadth_sma50,market_vol_regime \
  --output live_metrics.feather
```

### Example 2: Live Ranking
```bash
metrex rank --live \
  --symbols BTC/USDT,ETH/USDT,XRP/USDT,ADA/USDT \
  --timeframe 5m \
  --interval 300 \
  --outputfolder ./live_ranks
```

### Example 3: Production Live Mode
```bash
# Run in background with logging
nohup metrex live \
  --symbols BTC/USDT,ETH/USDT,BNB/USDT,XRP/USDT,ADA/USDT,SOL/USDT \
  --timeframe 1m \
  --interval 60 \
  --all-metrics \
  --metrics-output ./production/live_metrics.feather \
  --ranks-output ./production/live_ranks \
  > metrex.log 2>&1 &

# Check logs
tail -f metrex.log

# Stop
ps aux | grep metrex
kill <PID>
```

### Example 4: Programmatic Usage
```bash
# Run the example script
python3 example_live_usage.py
```

## Next Steps

1. **Read Full Documentation**
   - [LIVE_MODE.md](LIVE_MODE.md) - Complete live mode guide
   - [README.md](README.md) - Main project documentation
   - [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) - Technical details

2. **Test with Your Symbols**
   ```bash
   metrex live --symbols YOUR/SYMBOLS --timeframe 1m --interval 60 --all-metrics
   ```

3. **Integrate with Your Strategy**
   - Read metrics from `live_metrics.feather`
   - Read rankings from `live_ranks/*.feather`
   - Use in your trading bot logic

4. **Set Up Production**
   - Use process manager (systemd, supervisor, pm2)
   - Configure logging and monitoring
   - Set appropriate intervals based on your needs

## Support

For issues or questions:
1. Check [LIVE_MODE.md](LIVE_MODE.md) troubleshooting section
2. Run `python3 test_live_mode.py` to diagnose
3. Check CCXT documentation for exchange-specific issues: https://docs.ccxt.com/

## Summary

You're all set! 🎉

- ✅ Metrex installed with live mode support
- ✅ All CLI commands available
- ✅ Tests passing
- ✅ Ready to fetch live data from exchanges

Start with a simple test:
```bash
metrex live --symbols BTC/USDT,ETH/USDT --timeframe 1m --interval 60 --all-metrics
```

Stop with `Ctrl+C` when done.
