"""
Live data runner: fetches OHLCV from Binance (via CCXT) and schedules periodic updates.
"""
import pandas as pd
import ccxt
from typing import List, Dict, Any, Optional
from datetime import datetime
import time
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_live_data(
    symbols: List[str],
    timeframe: str = '1m',
    limit: int = 1000,
    exchange_id: str = 'binance'
) -> pd.DataFrame:
    """
    Fetch live OHLCV data from exchange (default: Binance) for the given symbols.
    
    Args:
        symbols: List of trading pairs (e.g., ['BTC/USDT', 'ETH/USDT'])
        timeframe: Candle timeframe ('1m', '5m', '1h', '4h', '1d', etc.)
        limit: Number of historical candles to fetch (default: 1000)
        exchange_id: Exchange identifier for CCXT (default: 'binance')
    
    Returns:
        DataFrame with columns: date, pair, open, high, low, close, volume
        Matches the schema expected by metrex metrics pipeline.
    """
    try:
        # Initialize exchange
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({
            'enableRateLimit': True,
        })
        
        all_data = []
        
        for symbol in symbols:
            try:
                # Fetch OHLCV data
                # Returns: [[timestamp, open, high, low, close, volume], ...]
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                
                if not ohlcv:
                    logger.warning(f"No data returned for {symbol}")
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(
                    ohlcv,
                    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                )
                
                # Convert timestamp to datetime
                df['date'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
                
                # Normalize pair name (remove slash for consistency: BTC/USDT -> BTCUSDT)
                pair_name = symbol.replace('/', '')
                df['pair'] = pair_name
                
                # Select and order columns to match offline schema
                df = df[['date', 'pair', 'open', 'high', 'low', 'close', 'volume']]
                
                all_data.append(df)
                logger.info(f"Fetched {len(df)} candles for {symbol}")
                
            except Exception as e:
                logger.error(f"Failed to fetch data for {symbol}: {e}")
                continue
        
        if not all_data:
            raise ValueError("No data fetched for any symbol")
        
        # Combine all pairs into single DataFrame
        result = pd.concat(all_data, ignore_index=True)
        result = result.sort_values(['date', 'pair']).reset_index(drop=True)
        
        logger.info(f"Total: fetched {len(result)} rows for {len(all_data)} pairs")
        return result
        
    except Exception as e:
        logger.error(f"Error in get_live_data: {e}")
        raise


def run_live_metrics(
    symbols: List[str],
    timeframe: str,
    metric_names: List[str],
    interval: int,
    output: Path,
    ctx: Dict[str, Any] = None,
    max_iterations: Optional[int] = None
) -> None:
    """
    Run metrics computation continuously on live data.
    
    Args:
        symbols: List of trading pairs
        timeframe: Candle timeframe
        metric_names: List of metric names to compute
        interval: Update interval in seconds
        output: Path to save results
        ctx: Optional context dict for metrics
        max_iterations: Maximum number of iterations (None = infinite)
    """
    from .processor import run_metrics
    from .io import save
    
    if ctx is None:
        ctx = {}
    
    iteration = 0
    logger.info(f"Starting live metrics mode: {', '.join(metric_names)}")
    logger.info(f"Symbols: {', '.join(symbols)}, Timeframe: {timeframe}, Interval: {interval}s")
    
    try:
        while max_iterations is None or iteration < max_iterations:
            iteration += 1
            start_time = time.time()
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Iteration {iteration} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info(f"{'='*60}")
            
            try:
                # Fetch live data
                df = get_live_data(symbols, timeframe)
                
                # Compute metrics
                result = run_metrics(df, metric_names, ctx)
                
                # Save results (append mode)
                if output.exists():
                    try:
                        existing = pd.read_feather(output)
                        existing['date'] = pd.to_datetime(existing['date'])
                        result['date'] = pd.to_datetime(result['date'])
                        
                        # Avoid duplicate dates
                        existing_dates = set(existing['date'].astype('int64'))
                        result = result[~result['date'].astype('int64').isin(existing_dates)]
                        
                        if not result.empty:
                            combined = pd.concat([existing, result], ignore_index=True)
                            combined = combined.sort_values('date')
                            save(combined, output)
                            logger.info(f"Appended {len(result)} new rows to {output}")
                        else:
                            logger.info("No new data to append")
                    except Exception as e:
                        logger.error(f"Error appending data: {e}")
                        save(result, output)
                else:
                    save(result, output)
                    logger.info(f"Created new output: {output}")
                
                # Display latest metrics
                if not result.empty:
                    logger.info("\n📊 Latest Metrics:")
                    latest = result.iloc[-1]
                    for col in result.columns:
                        if col != 'date':
                            logger.info(f"  {col}: {latest[col]:.4f}")
                
            except Exception as e:
                logger.error(f"Error in iteration {iteration}: {e}")
            
            # Wait for next iteration
            elapsed = time.time() - start_time
            sleep_time = max(0, interval - elapsed)
            
            if max_iterations is None or iteration < max_iterations:
                logger.info(f"\n⏱️  Next update in {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        logger.info("\n\n⚠️  Live mode interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in live mode: {e}")
        raise


def run_live_ranking(
    symbols: List[str],
    timeframe: str,
    interval: int,
    outputfolder: Path,
    max_iterations: Optional[int] = None
) -> None:
    """
    Run ranking computation continuously on live data.
    
    Args:
        symbols: List of trading pairs
        timeframe: Candle timeframe
        interval: Update interval in seconds
        outputfolder: Folder to save per-pair ranking files
        max_iterations: Maximum number of iterations (None = infinite)
    """
    from .processor import rank_pairs
    from .io import save
    import tempfile
    
    outputfolder = Path(outputfolder)
    outputfolder.mkdir(parents=True, exist_ok=True)
    
    iteration = 0
    logger.info(f"Starting live ranking mode")
    logger.info(f"Symbols: {', '.join(symbols)}, Timeframe: {timeframe}, Interval: {interval}s")
    
    try:
        while max_iterations is None or iteration < max_iterations:
            iteration += 1
            start_time = time.time()
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Iteration {iteration} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info(f"{'='*60}")
            
            try:
                # Fetch live data
                df = get_live_data(symbols, timeframe)
                
                # Save to temporary folder (required by rank_pairs)
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmpdir_path = Path(tmpdir)
                    
                    # Write per-pair feather files
                    for pair, g in df.groupby('pair'):
                        pair_file = tmpdir_path / f"{pair}-{timeframe}.feather"
                        save(g[['date', 'open', 'high', 'low', 'close', 'volume']], pair_file)
                    
                    # Get latest date for timerange
                    latest_date = df['date'].max()
                    earliest_date = df['date'].min()
                    timerange = f"{earliest_date.strftime('%Y%m%d')}-{latest_date.strftime('%Y%m%d')}"
                    
                    # Compute rankings
                    rank_pairs(tmpdir_path, timeframe, timerange, outputfolder)
                
                # Display top gainers/losers
                logger.info("\n📈 Top Movers (24h):")
                for pair in symbols:
                    pair_name = pair.replace('/', '')
                    rank_file = outputfolder / f"{pair_name}-{timeframe}.feather"
                    if rank_file.exists():
                        try:
                            rank_df = pd.read_feather(rank_file)
                            if not rank_df.empty:
                                latest = rank_df.iloc[-1]
                                change = latest.get('changePercentage24h', None)
                                gainer_rank = latest.get('topGainerRank', None)
                                volume_rank = latest.get('topVolumeRank', None)
                                
                                info = f"  {pair_name}"
                                if change is not None:
                                    info += f" | 24h: {change:+.2f}%"
                                if gainer_rank is not None:
                                    info += f" | Gainer Rank: {int(gainer_rank)}"
                                if volume_rank is not None:
                                    info += f" | Volume Rank: {int(volume_rank)}"
                                
                                logger.info(info)
                        except Exception as e:
                            logger.error(f"Error reading rank file for {pair_name}: {e}")
                
            except Exception as e:
                logger.error(f"Error in iteration {iteration}: {e}")
            
            # Wait for next iteration
            elapsed = time.time() - start_time
            sleep_time = max(0, interval - elapsed)
            
            if max_iterations is None or iteration < max_iterations:
                logger.info(f"\n⏱️  Next update in {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        logger.info("\n\n⚠️  Live mode interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in live ranking mode: {e}")
        raise


def run_live_combined(
    symbols: List[str],
    timeframe: str,
    metric_names: List[str],
    interval: int,
    metrics_output: Path,
    ranks_output: Path,
    ctx: Dict[str, Any] = None,
    max_iterations: Optional[int] = None
) -> None:
    """
    Run both metrics and ranking continuously on live data.
    
    Args:
        symbols: List of trading pairs
        timeframe: Candle timeframe
        metric_names: List of metric names to compute
        interval: Update interval in seconds
        metrics_output: Path to save metrics results
        ranks_output: Folder to save per-pair ranking files
        ctx: Optional context dict for metrics
        max_iterations: Maximum number of iterations (None = infinite)
    """
    from .processor import run_metrics, rank_pairs
    from .io import save
    import tempfile
    
    if ctx is None:
        ctx = {}
    
    ranks_output = Path(ranks_output)
    ranks_output.mkdir(parents=True, exist_ok=True)
    
    iteration = 0
    logger.info(f"Starting combined live mode (metrics + ranking)")
    logger.info(f"Metrics: {', '.join(metric_names)}")
    logger.info(f"Symbols: {', '.join(symbols)}, Timeframe: {timeframe}, Interval: {interval}s")
    
    try:
        while max_iterations is None or iteration < max_iterations:
            iteration += 1
            start_time = time.time()
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Iteration {iteration} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info(f"{'='*60}")
            
            try:
                # Fetch live data
                df = get_live_data(symbols, timeframe)
                
                # 1. Compute and save metrics
                result = run_metrics(df, metric_names, ctx)
                
                if metrics_output.exists():
                    try:
                        existing = pd.read_feather(metrics_output)
                        existing['date'] = pd.to_datetime(existing['date'])
                        result['date'] = pd.to_datetime(result['date'])
                        
                        existing_dates = set(existing['date'].astype('int64'))
                        result = result[~result['date'].astype('int64').isin(existing_dates)]
                        
                        if not result.empty:
                            combined = pd.concat([existing, result], ignore_index=True)
                            combined = combined.sort_values('date')
                            save(combined, metrics_output)
                            logger.info(f"📊 Appended {len(result)} new metric rows")
                    except Exception as e:
                        logger.error(f"Error appending metrics: {e}")
                        save(result, metrics_output)
                else:
                    save(result, metrics_output)
                    logger.info(f"📊 Created metrics file")
                
                # Display latest metrics
                if not result.empty:
                    logger.info("\n📊 Latest Metrics:")
                    latest = result.iloc[-1]
                    for col in result.columns:
                        if col != 'date':
                            logger.info(f"  {col}: {latest[col]:.4f}")
                
                # 2. Compute and save rankings
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmpdir_path = Path(tmpdir)
                    
                    for pair, g in df.groupby('pair'):
                        pair_file = tmpdir_path / f"{pair}-{timeframe}.feather"
                        save(g[['date', 'open', 'high', 'low', 'close', 'volume']], pair_file)
                    
                    latest_date = df['date'].max()
                    earliest_date = df['date'].min()
                    timerange = f"{earliest_date.strftime('%Y%m%d')}-{latest_date.strftime('%Y%m%d')}"
                    
                    rank_pairs(tmpdir_path, timeframe, timerange, ranks_output)
                
                # Display top movers
                logger.info("\n📈 Top Movers (24h):")
                for pair in symbols:
                    pair_name = pair.replace('/', '')
                    rank_file = ranks_output / f"{pair_name}-{timeframe}.feather"
                    if rank_file.exists():
                        try:
                            rank_df = pd.read_feather(rank_file)
                            if not rank_df.empty:
                                latest = rank_df.iloc[-1]
                                change = latest.get('changePercentage24h', None)
                                gainer_rank = latest.get('topGainerRank', None)
                                volume_rank = latest.get('topVolumeRank', None)
                                
                                info = f"  {pair_name}"
                                if change is not None:
                                    info += f" | 24h: {change:+.2f}%"
                                if gainer_rank is not None:
                                    info += f" | Gainer Rank: {int(gainer_rank)}"
                                if volume_rank is not None:
                                    info += f" | Volume Rank: {int(volume_rank)}"
                                
                                logger.info(info)
                        except Exception as e:
                            logger.error(f"Error reading rank file for {pair_name}: {e}")
                
            except Exception as e:
                logger.error(f"Error in iteration {iteration}: {e}")
            
            # Wait for next iteration
            elapsed = time.time() - start_time
            sleep_time = max(0, interval - elapsed)
            
            if max_iterations is None or iteration < max_iterations:
                logger.info(f"\n⏱️  Next update in {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        logger.info("\n\n⚠️  Live mode interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in combined live mode: {e}")
        raise
