#!/usr/bin/env python3
"""
Example usage of metrex CLI tool.

This script demonstrates how to use metrex to process Freqtrade candle data
and compute market metrics.
"""

import subprocess
import sys
from pathlib import Path
import pandas as pd


def run_metrex_example():
    """Run a complete metrex example."""
    
    print("Metrex CLI Tool Example")
    print("=" * 50)
    
    # Example data path (adjust as needed)
    data_folder = "/tmp/test_data"  # Use test data we created
    
    # Check if test data exists
    if not Path(data_folder).exists():
        print(f"❌ Test data not found at {data_folder}")
        print("Please run the test data creation script first.")
        return
    
    # Example parameters
    timeframe = "1h"
    timerange = "20230601-20230615"  # Two week period
    output_file = "/tmp/example_metrics.feather"
    
    # Build command
    cmd = [
        "metrex",
        "--datafolder", data_folder,
        "--timeframe", timeframe,
        "--timerange", timerange,
        "--output", output_file
    ]
    
    print(f"Running command:")
    print(f"  {' '.join(cmd)}")
    print()
    
    try:
        # Run metrex
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Metrex completed successfully!")
            print(result.stdout)
            
            # Load and display results
            print("\nResults Preview:")
            print("-" * 30)
            
            if Path(output_file).exists():
                df = pd.read_feather(output_file)
                print(f"Shape: {df.shape}")
                print(f"Columns: {list(df.columns)}")
                print(f"\nFirst 5 rows:")
                print(df.head())
                print(f"\nMetrics Summary:")
                print(f"  Breadth Above SMA50: {df['breadth_above_sma_50'].mean():.1f}% avg")
                print(f"  Volatility Regimes: {df['market_vol_regime'].value_counts().to_dict()}")
                print(f"  BTC Trend Slope: {df['btc_trend_slope'].mean():.2f} avg")
            else:
                print("❌ Output file not created")
        
        else:
            print("❌ Metrex failed!")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            
    except Exception as e:
        print(f"❌ Error running metrex: {e}")


def show_help():
    """Show metrex help."""
    print("\nMetrex Help:")
    print("-" * 20)
    subprocess.run(["metrex", "--help"])


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        show_help()
    else:
        run_metrex_example()