"""
Legacy core processing (not used by the current CLI).
See `metrex/processor.py` for the active pipeline.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re
from datetime import datetime


class MetrexProcessor:
    """Process Freqtrade candle data and compute market metrics."""
    
    def __init__(self, datafolder: Path):
        self.datafolder = Path(datafolder)
        if not self.datafolder.exists():
            raise ValueError(f"Data folder does not exist: {datafolder}")
    
    def _parse_timerange(self, timerange: str) -> Tuple[datetime, datetime]:
        """Parse timerange string like '20230101-20231231' into datetime objects."""
        if '-' not in timerange:
            raise ValueError(f"Invalid timerange format. Expected 'YYYYMMDD-YYYYMMDD', got: {timerange}")
        
        start_str, end_str = timerange.split('-', 1)
        
        try:
            start_date = datetime.strptime(start_str, '%Y%m%d')
            end_date = datetime.strptime(end_str, '%Y%m%d')
        except ValueError as e:
            raise ValueError(f"Invalid date format in timerange. Expected 'YYYYMMDD-YYYYMMDD': {e}")
        
        if start_date > end_date:
            raise ValueError(f"Start date must be before end date in timerange: {timerange}")
        
        return start_date, end_date
    
    def _load_feather_files(self, timeframe: str) -> Dict[str, pd.DataFrame]:
        """Load all .feather files matching the timeframe."""
        pattern = f"*-{timeframe}.feather"
        feather_files = list(self.datafolder.glob(pattern))
        
        if not feather_files:
            raise ValueError(f"No .feather files found for timeframe '{timeframe}' in {self.datafolder}")
        
        data = {}
        for file_path in feather_files:
            # Extract symbol from filename (e.g., BTC_USDT-1h.feather -> BTC_USDT)
            symbol = file_path.stem.split('-')[0]
            try:
                df = pd.read_feather(file_path)
                # Ensure timestamp column exists and is datetime
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                elif 'date' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['date'])
                    df = df.drop('date', axis=1)
                else:
                    # Try to find a date-like column
                    date_cols = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
                    if date_cols:
                        df['timestamp'] = pd.to_datetime(df[date_cols[0]])
                        if date_cols[0] != 'timestamp':
                            df = df.drop(date_cols[0], axis=1)
                    else:
                        raise ValueError(f"No timestamp column found in {file_path}")
                
                df = df.set_index('timestamp')
                data[symbol] = df
                
            except Exception as e:
                print(f"Warning: Failed to load {file_path}: {e}")
                continue
        
        if not data:
            raise ValueError(f"No valid .feather files could be loaded for timeframe '{timeframe}'")
        
        return data
    
    def _filter_by_timerange(self, data: Dict[str, pd.DataFrame], timerange: str) -> Dict[str, pd.DataFrame]:
        """Filter data by the specified timerange."""
        start_date, end_date = self._parse_timerange(timerange)
        # Convert to UTC-aware datetimes for comparison
        start_date = pd.Timestamp(start_date).tz_localize('UTC')
        end_date = pd.Timestamp(end_date).tz_localize('UTC')

        filtered_data = {}
        for symbol, df in data.items():
            # Filter data within the timerange
            mask = (df.index >= start_date) & (df.index <= end_date)
            filtered_df = df.loc[mask]

            if not filtered_df.empty:
                filtered_data[symbol] = filtered_df
            else:
                print(f"Warning: No data for {symbol} in timerange {timerange}")

        if not filtered_data:
            raise ValueError(f"No data found for any symbols in timerange {timerange}")

        return filtered_data
    
    def _calculate_sma(self, prices: pd.Series, window: int = 50) -> pd.Series:
        """Calculate Simple Moving Average."""
        return prices.rolling(window=window, min_periods=window).mean()
    
    def _calculate_breadth_above_sma_50(self, data: Dict[str, pd.DataFrame]) -> pd.Series:
        """Calculate percentage of symbols trading above their 50-period SMA."""
        breadth_data = []
        
        # Get all timestamps across all symbols
        all_timestamps = set()
        for df in data.values():
            all_timestamps.update(df.index)
        all_timestamps = sorted(all_timestamps)
        
        breadth_series = pd.Series(index=all_timestamps, dtype=float)
        
        for timestamp in all_timestamps:
            symbols_above_sma = 0
            total_symbols = 0
            
            for symbol, df in data.items():
                if timestamp in df.index:
                    # Get price data up to this timestamp
                    price_data = df.loc[df.index <= timestamp, 'close']
                    
                    if len(price_data) >= 50:  # Need at least 50 periods for SMA
                        current_price = price_data.iloc[-1]
                        sma_50 = price_data.rolling(window=50).mean().iloc[-1]
                        
                        if current_price > sma_50:
                            symbols_above_sma += 1
                        total_symbols += 1
            
            if total_symbols > 0:
                breadth_series[timestamp] = (symbols_above_sma / total_symbols) * 100
        
        return breadth_series.dropna()
    
    def _calculate_market_vol_regime(self, data: Dict[str, pd.DataFrame]) -> pd.Series:
        """Calculate market volatility regime (high/medium/low)."""
        # Use overall market volatility - can use BTC or a market average
        btc_data = None
        for symbol in ['BTC_USDT', 'BTCUSDT', 'BTC']:
            if symbol in data:
                btc_data = data[symbol]
                break
        
        if btc_data is None:
            # Use the first available symbol as proxy
            btc_data = next(iter(data.values()))
        
        # Calculate rolling volatility (20-period)
        returns = btc_data['close'].pct_change()
        volatility = returns.rolling(window=20).std() * np.sqrt(24 * 365)  # Annualized volatility
        
        # Define volatility regimes based on percentiles
        vol_regime = pd.Series(index=volatility.index, dtype=str)
        
        for timestamp in volatility.index:
            current_vol = volatility[timestamp]
            if pd.isna(current_vol):
                continue
                
            # Get historical context
            hist_data = volatility.loc[volatility.index <= timestamp]
            if len(hist_data) < 50:
                vol_regime[timestamp] = 'medium'
                continue
            
            # Calculate percentiles from historical data
            hist_vol_clean = hist_data.dropna()
            if len(hist_vol_clean) < 20:
                vol_regime[timestamp] = 'medium'
                continue
                
            p33 = hist_vol_clean.quantile(0.33)
            p67 = hist_vol_clean.quantile(0.67)
            
            if current_vol <= p33:
                vol_regime[timestamp] = 'low'
            elif current_vol >= p67:
                vol_regime[timestamp] = 'high'
            else:
                vol_regime[timestamp] = 'medium'
        
        return vol_regime.dropna()
    
    def _calculate_btc_trend_slope(self, data: Dict[str, pd.DataFrame]) -> pd.Series:
        """Calculate BTC trend slope using linear regression."""
        # Find BTC data
        btc_data = None
        for symbol in ['BTC_USDT', 'BTCUSDT', 'BTC']:
            if symbol in data:
                btc_data = data[symbol]
                break
        
        if btc_data is None:
            raise ValueError("BTC data not found. Please ensure BTC_USDT.feather file is present.")
        
        prices = btc_data['close']
        trend_slope = pd.Series(index=prices.index, dtype=float)
        
        # Calculate rolling 20-period trend slope
        window = 20
        for i in range(window - 1, len(prices)):
            price_window = prices.iloc[i - window + 1:i + 1]
            x = np.arange(len(price_window))
            
            # Linear regression to get slope
            if len(price_window) == window and not price_window.isna().any():
                slope = np.polyfit(x, price_window.values, 1)[0]
                trend_slope.iloc[i] = slope
        
        return trend_slope.dropna()
    
    def process(self, timeframe: str, timerange: str) -> pd.DataFrame:
        """Process data and compute all metrics."""
        # Load data
        data = self._load_feather_files(timeframe)
        print(f"Loaded {len(data)} symbols")
        
        # Filter by timerange
        data = self._filter_by_timerange(data, timerange)
        print(f"Filtered to {len(data)} symbols with data in timerange")
        
        # Calculate metrics
        print("Calculating breadth_above_sma_50...")
        breadth_above_sma_50 = self._calculate_breadth_above_sma_50(data)
        
        print("Calculating market_vol_regime...")
        market_vol_regime = self._calculate_market_vol_regime(data)
        
        print("Calculating btc_trend_slope...")
        btc_trend_slope = self._calculate_btc_trend_slope(data)
        
        # Combine results into a single DataFrame
        results = pd.DataFrame({
            'breadth_above_sma_50': breadth_above_sma_50,
            'market_vol_regime': market_vol_regime,
            'btc_trend_slope': btc_trend_slope
        })
        
        # Forward fill missing values and drop rows where all values are NaN
        results = results.ffill().dropna(how='all')
        
        return results
    
    def save_results(self, results: pd.DataFrame, output_path: Path) -> None:
        """Save results to a .feather file."""
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Reset index to make timestamp a column for feather format
        results_to_save = results.reset_index()
        results_to_save.to_feather(output_path, compression_level=9, compression="lz4")
        
        print(f"Results saved with {len(results)} rows and {len(results.columns)} columns")
