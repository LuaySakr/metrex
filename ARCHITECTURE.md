# Metrex Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USER INTERFACE                                  │
│                                                                              │
│  CLI Commands:                                                               │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  ┌──────────┐ │
│  │ metrex metrics │  │  metrex rank   │  │  metrex live   │  │metrex list│ │
│  └────────┬───────┘  └────────┬───────┘  └────────┬───────┘  └──────────┘ │
└───────────┼──────────────────┼──────────────────┼─────────────────────────┘
            │                  │                  │
            │                  │                  │
┌───────────▼──────────────────▼──────────────────▼─────────────────────────┐
│                           CLI LAYER (cli.py)                                │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ Routing Logic:                                                       │  │
│  │ • Parse flags (--live, --datafolder, --symbols, --interval)        │  │
│  │ • Validate mode-specific requirements                               │  │
│  │ • Route to offline or live processing                               │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└────────────┬─────────────────────────────────────┬──────────────────────────┘
             │                                     │
    ┌────────▼─────────┐                 ┌────────▼────────┐
    │  OFFLINE MODE    │                 │   LIVE MODE     │
    │    (Local)       │                 │  (Exchange)     │
    └────────┬─────────┘                 └────────┬────────┘
             │                                     │
             │                                     │
┌────────────▼─────────────────┐    ┌─────────────▼──────────────────────────┐
│     DATA SOURCE LAYER        │    │      DATA SOURCE LAYER                  │
│                              │    │                                         │
│  io.py: load_feathers()      │    │  live_runner.py: get_live_data()       │
│  ┌────────────────────────┐  │    │  ┌──────────────────────────────────┐  │
│  │ • Read .feather files  │  │    │  │ • Initialize CCXT exchange      │  │
│  │ • Parse pair names     │  │    │  │ • Fetch OHLCV via API           │  │
│  │ • Filter by timeframe  │  │    │  │ • Convert timestamps            │  │
│  │ • Standardize schema   │  │    │  │ • Normalize pair names          │  │
│  └────────────────────────┘  │    │  │ • Handle errors gracefully      │  │
│                              │    │  └──────────────────────────────────┘  │
└────────────┬─────────────────┘    └─────────────┬──────────────────────────┘
             │                                     │
             │                                     │
             └──────────────┬──────────────────────┘
                            │
                            │ DataFrame['date', 'pair', 'open', 'high', 'low', 'close', 'volume']
                            │
┌───────────────────────────▼─────────────────────────────────────────────────┐
│                      PROCESSING LAYER (processor.py)                         │
│                                                                              │
│  ┌──────────────────────────┐         ┌────────────────────────────┐       │
│  │   run_metrics()          │         │   rank_pairs()             │       │
│  │  ┌────────────────────┐  │         │  ┌──────────────────────┐  │       │
│  │  │ • Group by pair    │  │         │  │ • Compute 24h change │  │       │
│  │  │ • Compute metrics  │  │         │  │ • Cross-sectional    │  │       │
│  │  │ • Merge results    │  │         │  │   ranking            │  │       │
│  │  │ • Fill forward     │  │         │  │ • Volume metrics     │  │       │
│  │  │ • Sort by date     │  │         │  │ • Per-pair outputs   │  │       │
│  │  └────────────────────┘  │         │  └──────────────────────┘  │       │
│  └──────────┬───────────────┘         └──────────┬─────────────────┘       │
└─────────────┼──────────────────────────────────┼─────────────────────────┘
              │                                  │
              │                                  │
┌─────────────▼──────────────────────────────────┼─────────────────────────┐
│                   METRICS LAYER (metrics/)                                  │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │
│  │ breadth_sma50   │  │ btc_trend_slope │  │market_vol_regime│          │
│  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │          │
│  │ │• Compute   │ │  │ │• Linear reg │ │  │ │• Vol zscore │ │          │
│  │ │  SMA50     │ │  │ │  on BTC     │ │  │ │• Percentile │ │          │
│  │ │• Above %   │ │  │ │• Slope coef │ │  │ │  classify   │ │          │
│  │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │          │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘          │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │
│  │  adv_decline    │  │ new_highs_lows  │  │avg_correlation  │          │
│  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │          │
│  │ │• Count up  │ │  │ │• 50-period  │ │  │ │• Rolling    │ │          │
│  │ │• Count down│ │  │ │  highs/lows │ │  │ │  correlation│ │          │
│  │ │• A/D line  │ │  │ │• Ratios     │ │  │ │  with BTC   │ │          │
│  │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │          │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘          │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐                                 │
│  │volume_surge_ratio│  │market_return_ma │                                 │
│  │ ┌─────────────┐ │  │ ┌─────────────┐ │                                 │
│  │ │• Detect    │ │  │ │• Mean ret   │ │                                 │
│  │ │  anomalies │ │  │ │• SMA smooth │ │                                 │
│  │ └─────────────┘ │  │ └─────────────┘ │                                 │
│  └─────────────────┘  └─────────────────┘                                 │
│                                                                             │
│                    All metrics implement MetricProtocol                     │
│                 .compute(df, ctx) -> DataFrame['date', ...]                │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
                              │ Results DataFrame
                              │
┌─────────────────────────────▼──────────────────────────────────────────────┐
│                        OUTPUT LAYER (io.py)                                 │
│                                                                             │
│  save() function:                                                           │
│  ┌───────────────────────────────────────────────────────────────────┐    │
│  │ • Check existing files                                            │    │
│  │ • Deduplicate by date                                             │    │
│  │ • Append new rows                                                 │    │
│  │ • Sort by date                                                    │    │
│  │ • Write .feather/.parquet/.csv                                    │    │
│  └───────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────────┐
                    │   OUTPUT FILES       │
                    │                      │
                    │ • metrics.feather    │
                    │ • live_metrics.feather│
                    │ • ranks/*.feather    │
                    │ • live_ranks/*.feather│
                    └──────────────────────┘


LIVE MODE SCHEDULING:
═════════════════════

┌────────────────────────────────────────────────────────────────────┐
│  run_live_metrics() / run_live_ranking() / run_live_combined()    │
│                                                                     │
│  while True:                                                        │
│    ┌─────────────────────────────────────────────────────┐        │
│    │ 1. Fetch live data via get_live_data()             │        │
│    │    └─> CCXT API call                               │        │
│    │                                                     │        │
│    │ 2. Run processor functions                         │        │
│    │    └─> run_metrics() and/or rank_pairs()          │        │
│    │                                                     │        │
│    │ 3. Save results with deduplication                 │        │
│    │    └─> Append to .feather files                   │        │
│    │                                                     │        │
│    │ 4. Display console output                          │        │
│    │    └─> Print latest metrics and top movers        │        │
│    │                                                     │        │
│    │ 5. Wait for next interval                          │        │
│    │    └─> time.sleep(interval - elapsed)             │        │
│    └─────────────────────────────────────────────────────┘        │
│                                                                     │
│  Graceful shutdown on KeyboardInterrupt (Ctrl+C)                   │
└────────────────────────────────────────────────────────────────────┘


DATA FLOW COMPARISON:
═════════════════════

OFFLINE:
  Local Files → load_feathers() → DataFrame → processor → save() → Output

LIVE:
  Exchange API → get_live_data() → DataFrame → processor → save() → Output
                        ↓
              (repeat every interval)


KEY DESIGN PRINCIPLES:
══════════════════════

1. ✅ Single Responsibility
   - Each module has one clear purpose
   - live_runner: data fetching & scheduling
   - processor: metric computation & ranking
   - metrics: individual metric implementations
   - io: file operations

2. ✅ Dependency Inversion
   - processor depends on abstractions (DataFrame schema)
   - Not on concrete data sources (files vs API)
   - Both modes feed identical DataFrames

3. ✅ Open/Closed Principle
   - New metrics added by creating new class in metrics/
   - No changes needed to processor, cli, or live_runner
   - Automatic registration via REGISTRY

4. ✅ DRY (Don't Repeat Yourself)
   - Zero duplication of metric logic
   - Single source of truth for each metric
   - Shared processor functions

5. ✅ Separation of Concerns
   - CLI: user interface & routing
   - Data sources: fetching & normalization
   - Processing: computation & transformation
   - Output: persistence & formatting

6. ✅ Error Handling
   - Graceful degradation (skip failed symbols)
   - Detailed logging at each layer
   - Try/except blocks around critical sections
   - User-friendly error messages
```

## File Structure

```
metrex/
├── cli.py                    # CLI interface (327 lines)
│   ├── metrics command       # Supports --live and offline
│   ├── rank command          # Supports --live and offline
│   ├── live command          # NEW: Combined live mode
│   └── list command          # Show available metrics
│
├── live_runner.py           # NEW: Live data functionality (489 lines)
│   ├── get_live_data()      # Fetch OHLCV from exchange
│   ├── run_live_metrics()   # Continuous metrics computation
│   ├── run_live_ranking()   # Continuous ranking computation
│   └── run_live_combined()  # Combined metrics + ranking
│
├── processor.py             # Core processing logic (218 lines)
│   ├── load_market()        # Load data (calls io.load_feathers)
│   ├── run_metrics()        # Compute and merge metrics
│   ├── process()            # Full pipeline: load → filter → compute → save
│   └── rank_pairs()         # Cross-sectional ranking with 24h stats
│
├── io.py                    # I/O operations (33 lines)
│   ├── load_feathers()      # Read .feather files into DataFrame
│   └── save()               # Write DataFrame to .feather/.parquet/.csv
│
├── timeutils.py             # Time range filtering
│   └── filter_timerange()   # Filter DataFrame by date range
│
├── metrics/                 # Metric implementations
│   ├── __init__.py          # Registry and registration system
│   ├── base.py              # MetricProtocol interface
│   ├── breadth_sma50.py     # % pairs above SMA50
│   ├── btc_trend_slope.py   # BTC linear regression slope
│   ├── market_vol_regime.py # Volatility classification
│   ├── adv_decline.py       # Advance/decline metrics
│   ├── new_highs_lows.py    # New highs/lows counts
│   ├── volume_surge_ratio.py# Volume anomaly detection
│   ├── avg_correlation_btc.py# Cross-sectional BTC correlation
│   └── market_return_ma.py  # Market return averages
│
└── core.py                  # Legacy (not used by current CLI)

Documentation:
├── README.md                # Main project documentation
├── LIVE_MODE.md            # Complete live mode guide
├── IMPLEMENTATION_SUMMARY.md# Technical implementation details
├── INSTALLATION.md         # Installation & setup guide
└── ARCHITECTURE.md         # This file

Examples & Tests:
├── example_usage.py        # Original example (offline mode)
├── example_live_usage.py   # NEW: Live mode examples
└── test_live_mode.py       # NEW: Automated test suite

Configuration:
├── pyproject.toml          # Project metadata & dependencies
├── CONTRIBUTING.md         # Contribution guidelines
└── LICENSE                 # MIT License
```

## Component Interaction Matrix

```
┌──────────────┬─────┬──────┬──────────┬──────┬─────────┬────────┐
│ Component    │ CLI │  IO  │Processor │Metrics│LiveRunner│ CCXT  │
├──────────────┼─────┼──────┼──────────┼──────┼─────────┼────────┤
│ CLI          │  -  │  ✓   │    ✓     │  ✓   │    ✓    │   -    │
│ IO           │  -  │  -   │    -     │  -   │    ✓    │   -    │
│ Processor    │  -  │  ✓   │    -     │  ✓   │    ✓    │   -    │
│ Metrics      │  -  │  -   │    -     │  -   │    -    │   -    │
│ Live Runner  │  -  │  ✓   │    ✓     │  -   │    -    │   ✓    │
│ CCXT         │  -  │  -   │    -     │  -   │    -    │   -    │
└──────────────┴─────┴──────┴──────────┴──────┴─────────┴────────┘

✓ = Depends on / Imports from
- = No direct dependency
```

## Data Schema

### Input DataFrame (from both offline and live sources)
```python
{
    'date': pd.Timestamp,       # UTC timestamp
    'pair': str,                # e.g., 'BTCUSDT'
    'open': float,              # Opening price
    'high': float,              # High price
    'low': float,               # Low price
    'close': float,             # Closing price
    'volume': float             # Trading volume
}
```

### Metrics Output DataFrame
```python
{
    'date': pd.Timestamp,                   # UTC timestamp
    'breadth_above_sma_50': float,          # 0-100 percentage
    'btc_trend_slope': float,               # Slope coefficient
    'market_vol_regime': float,             # 0/1/2 (low/med/high)
    'vol_zscore': float,                    # Z-score
    'adv_count': int,                       # Advancing pairs
    'decl_count': int,                      # Declining pairs
    'adv_decline_diff': int,                # adv - decl
    'adv_decline_line': float,              # Cumulative A/D
    'new_highs_50': int,                    # New 50-period highs
    'new_lows_50': int,                     # New 50-period lows
    'volume_surge_ratio': float,            # Volume anomaly metric
    'avg_corr_btc': float,                  # Mean correlation to BTC
    'mkt_ret': float,                       # Mean market return
    'mkt_ret_sma20': float                  # 20-period SMA of returns
}
```

### Rankings Output DataFrame (per pair)
```python
{
    'date': pd.Timestamp,                   # UTC timestamp
    'open': float,
    'high': float,
    'low': float,
    'close': float,
    'volume': float,
    'pairsCount': int,                      # Number of pairs
    'changePercentage24h': float,           # 24h price change %
    'topGainerRank': int,                   # Rank by gain (1=best)
    'topLooserRank': int,                   # Rank by loss (1=worst)
    'volumeInCurrency': float,              # volume * close
    'volumeInCurrency24': float,            # 24h rolling sum
    'topVolumeRank': int,                   # Rank by volume (1=highest)
    'bottomVolumeRank': int                 # Rank by volume (1=lowest)
}
```

---

**Last Updated**: October 14, 2025  
**Version**: 1.0.0 (with live mode support)
