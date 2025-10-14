#!/usr/bin/env python3
"""
Example usage of metrex live mode features.

This script demonstrates how to use the live data functionality
programmatically (without the CLI).
"""

from pathlib import Path
from metrex.live_runner import (
    get_live_data,
    run_live_metrics,
    run_live_ranking,
    run_live_combined
)
from metrex.metrics import all_names


def example_1_fetch_live_data():
    """Example 1: Fetch live data and print basic stats."""
    print("Example 1: Fetching live data from Binance...")
    print("=" * 60)
    
    symbols = ['BTC/USDT', 'ETH/USDT', 'XRP/USDT']
    timeframe = '1m'
    
    try:
        df = get_live_data(symbols, timeframe, limit=100)
        
        print(f"\nFetched {len(df)} rows for {df['pair'].nunique()} pairs")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"\nFirst few rows:")
        print(df.head())
        print(f"\nColumns: {list(df.columns)}")
        print(f"\nLatest prices:")
        latest = df.groupby('pair').tail(1)[['pair', 'date', 'close']]
        print(latest)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Note: This requires CCXT to be installed: pip install ccxt>=4.0.0")


def example_2_live_metrics_short_run():
    """Example 2: Run live metrics for 3 iterations (30 seconds)."""
    print("\n\nExample 2: Running live metrics (3 iterations)...")
    print("=" * 60)
    
    symbols = ['BTC/USDT', 'ETH/USDT']
    timeframe = '1m'
    metric_names = ['breadth_sma50', 'market_vol_regime']
    output = Path('./example_live_metrics.feather')
    
    try:
        run_live_metrics(
            symbols=symbols,
            timeframe=timeframe,
            metric_names=metric_names,
            interval=10,  # 10 seconds between updates
            output=output,
            max_iterations=3  # Stop after 3 iterations
        )
        
        print(f"\n✅ Results saved to {output}")
        
        # Read and display results
        import pandas as pd
        df = pd.read_feather(output)
        print(f"\nFinal results ({len(df)} rows):")
        print(df)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Note: This requires CCXT to be installed: pip install ccxt>=4.0.0")


def example_3_live_ranking_short_run():
    """Example 3: Run live ranking for 2 iterations (20 seconds)."""
    print("\n\nExample 3: Running live ranking (2 iterations)...")
    print("=" * 60)
    
    symbols = ['BTC/USDT', 'ETH/USDT', 'XRP/USDT']
    timeframe = '1m'
    outputfolder = Path('./example_live_ranks')
    
    try:
        run_live_ranking(
            symbols=symbols,
            timeframe=timeframe,
            interval=10,  # 10 seconds between updates
            outputfolder=outputfolder,
            max_iterations=2  # Stop after 2 iterations
        )
        
        print(f"\n✅ Results saved to {outputfolder}/")
        
        # Read and display one of the rank files
        import pandas as pd
        rank_files = list(outputfolder.glob('*.feather'))
        if rank_files:
            first_file = rank_files[0]
            df = pd.read_feather(first_file)
            print(f"\nSample rank file ({first_file.name}):")
            print(df[['date', 'close', 'changePercentage24h', 'topGainerRank', 'topVolumeRank']].tail())
        
    except Exception as e:
        print(f"Error: {e}")
        print("Note: This requires CCXT to be installed: pip install ccxt>=4.0.0")


def example_4_combined_mode():
    """Example 4: Run combined metrics + ranking for 2 iterations."""
    print("\n\nExample 4: Running combined mode (2 iterations)...")
    print("=" * 60)
    
    symbols = ['BTC/USDT', 'ETH/USDT']
    timeframe = '1m'
    metric_names = all_names()[:3]  # First 3 metrics
    metrics_output = Path('./example_combined_metrics.feather')
    ranks_output = Path('./example_combined_ranks')
    
    try:
        run_live_combined(
            symbols=symbols,
            timeframe=timeframe,
            metric_names=metric_names,
            interval=10,
            metrics_output=metrics_output,
            ranks_output=ranks_output,
            max_iterations=2
        )
        
        print(f"\n✅ Metrics saved to {metrics_output}")
        print(f"✅ Rankings saved to {ranks_output}/")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Note: This requires CCXT to be installed: pip install ccxt>=4.0.0")


def example_5_cli_usage():
    """Example 5: Demonstrate CLI commands (printed, not executed)."""
    print("\n\nExample 5: CLI Command Examples")
    print("=" * 60)
    
    commands = [
        "# List all available metrics",
        "metrex list",
        "",
        "# Live metrics mode (run continuously)",
        "metrex metrics --live --symbols BTC/USDT,ETH/USDT --timeframe 1m --interval 60 --all-metrics --output live_metrics.feather",
        "",
        "# Live ranking mode",
        "metrex rank --live --symbols BTC/USDT,ETH/USDT --timeframe 1m --interval 120 --outputfolder live_ranks",
        "",
        "# Combined live mode (recommended)",
        "metrex live --symbols BTC/USDT,ETH/USDT,XRP/USDT --timeframe 1m --interval 60 --all-metrics",
        "",
        "# Offline mode (original functionality)",
        "metrex metrics --datafolder ./data --timeframe 1h --timerange 20230101-20231231 --all-metrics --output metrics.feather",
        "",
        "# Stop any live command with Ctrl+C",
    ]
    
    for cmd in commands:
        print(cmd)


def main():
    """Run all examples."""
    print("\n")
    print("╔══════════════════════════════════════════════════════════╗")
    print("║         METREX LIVE MODE - USAGE EXAMPLES                ║")
    print("╚══════════════════════════════════════════════════════════╝")
    
    # Example 5 always works (just prints commands)
    example_5_cli_usage()
    
    # Check if we can run live examples
    try:
        import ccxt
        print(f"\n✅ CCXT {ccxt.__version__} is installed")
        print("✅ Live examples can run\n")
        
        # Uncomment to run live examples:
        # WARNING: These will make actual API calls to exchanges
        
        # example_1_fetch_live_data()
        # example_2_live_metrics_short_run()
        # example_3_live_ranking_short_run()
        # example_4_combined_mode()
        
        print("\n" + "="*60)
        print("To run the live examples, uncomment the function calls in")
        print("the main() function and run this script again.")
        print("="*60)
        
    except ImportError:
        print("\n⚠️  CCXT is not installed")
        print("Live examples are disabled")
        print("\nTo enable live mode:")
        print("  pip install ccxt>=4.0.0")
        print("\nOnly CLI examples are shown above.")


if __name__ == '__main__':
    main()
