"""
CLI interface for metrex.
"""


import click
from pathlib import Path
from .processor import process, rank_pairs
from .metrics import all_names

@click.group()
def cli():
    pass

@cli.command()
@click.option('--datafolder', required=False, type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path), help='Path to local .feather data (offline mode)')
@click.option('--timeframe', required=True, type=str, help='Candle timeframe (e.g., 1m, 5m, 1h, 4h, 1d)')
@click.option('--timerange', required=False, type=str, help='Time range for offline mode (e.g., 20230101-20231231)')
@click.option('--metrics', required=False, type=str, help='Comma-separated metric names')
@click.option('--all-metrics', is_flag=True, help='Run all metrics in registry')
@click.option('--output', required=True, type=click.Path(path_type=Path), help='Output file path')
@click.option('--live', is_flag=True, help='Enable live data mode (fetch from exchange)')
@click.option('--symbols', required=False, type=str, help='Comma-separated trading pairs for live mode (e.g., BTC/USDT,ETH/USDT)')
@click.option('--interval', required=False, type=int, default=60, help='Update interval in seconds for live mode (default: 60)')
@click.option('--exchange', required=False, type=str, default='binance', help='Exchange ID for live mode (default: binance)')
def metrics(datafolder, timeframe, timerange, metrics, all_metrics, output, live, symbols, interval, exchange):
    """
    Run selected market metrics and save results.
    
    Offline mode (default):
        metrex metrics --datafolder ./data --timeframe 1h --timerange 20230101-20231231 --all-metrics --output metrics.feather
    
    Live mode:
        metrex metrics --live --symbols BTC/USDT,ETH/USDT --timeframe 1m --interval 60 --all-metrics --output live_metrics.feather
    """
    # Determine metric names
    if all_metrics:
        metric_names = all_names()
    else:
        if not metrics:
            raise click.UsageError('Specify --metrics or --all-metrics')
        metric_names = [m.strip() for m in metrics.split(',')]
    
    # Validate mode-specific requirements
    if live:
        # Live mode
        if not symbols:
            raise click.UsageError('--symbols is required in live mode')
        
        symbol_list = [s.strip() for s in symbols.split(',')]
        
        from .live_runner import run_live_metrics
        click.echo(f"🔴 Starting LIVE metrics mode...")
        click.echo(f"Symbols: {', '.join(symbol_list)}")
        click.echo(f"Timeframe: {timeframe}")
        click.echo(f"Metrics: {', '.join(metric_names)}")
        click.echo(f"Interval: {interval}s")
        click.echo(f"Output: {output}")
        click.echo(f"Press Ctrl+C to stop.\n")
        
        run_live_metrics(
            symbols=symbol_list,
            timeframe=timeframe,
            metric_names=metric_names,
            interval=interval,
            output=Path(output),
            ctx={'exchange': exchange}
        )
    else:
        # Offline mode
        if not datafolder:
            raise click.UsageError('--datafolder is required in offline mode')
        if not timerange:
            raise click.UsageError('--timerange is required in offline mode')
        
        process(datafolder, timeframe, timerange, metric_names, output)
        click.echo(f"✅ Metrics computed: {', '.join(metric_names)}\nSaved to {output}")

if __name__ == '__main__':
    cli()

@cli.command()
@click.option('--datafolder', required=False, type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path), help='Path to local .feather data (offline mode)')
@click.option('--timeframe', required=True, type=str, help='Candle timeframe (e.g., 1m, 5m, 1h, 4h, 1d)')
@click.option('--timerange', required=False, type=str, help='Time range for offline mode (e.g., 20230101-20231231)')
@click.option('--outputfolder', required=True, type=click.Path(path_type=Path), help='Output folder for per-pair rank files')
@click.option('--live', is_flag=True, help='Enable live data mode (fetch from exchange)')
@click.option('--symbols', required=False, type=str, help='Comma-separated trading pairs for live mode (e.g., BTC/USDT,ETH/USDT)')
@click.option('--interval', required=False, type=int, default=120, help='Update interval in seconds for live mode (default: 120)')
@click.option('--exchange', required=False, type=str, default='binance', help='Exchange ID for live mode (default: binance)')
def rank(datafolder, timeframe, timerange, outputfolder, live, symbols, interval, exchange):
        """Generate/append per-pair ranked metrics (no duplicate dates).

        Offline mode (default):
        - If output files exist, new rows are appended; existing dates are not duplicated.
        - Timerange supports special form 'latest-YYYYMMDD' where the start date is
            taken as the last date already present in each pair's output (or the first
            available input date if the output file does not yet exist).
        
        Example offline:
            metrex rank --datafolder ./data --timeframe 1h --timerange 20230101-20231231 --outputfolder ./ranks
        
        Example live:
            metrex rank --live --symbols BTC/USDT,ETH/USDT --timeframe 1m --interval 120 --outputfolder ./live_ranks
        """
        # Validate mode-specific requirements
        if live:
            # Live mode
            if not symbols:
                raise click.UsageError('--symbols is required in live mode')
            
            symbol_list = [s.strip() for s in symbols.split(',')]
            
            from .live_runner import run_live_ranking
            click.echo(f"🔴 Starting LIVE ranking mode...")
            click.echo(f"Symbols: {', '.join(symbol_list)}")
            click.echo(f"Timeframe: {timeframe}")
            click.echo(f"Interval: {interval}s")
            click.echo(f"Output folder: {outputfolder}")
            click.echo(f"Press Ctrl+C to stop.\n")
            
            run_live_ranking(
                symbols=symbol_list,
                timeframe=timeframe,
                interval=interval,
                outputfolder=Path(outputfolder)
            )
        else:
            # Offline mode
            if not datafolder:
                raise click.UsageError('--datafolder is required in offline mode')
            if not timerange:
                raise click.UsageError('--timerange is required in offline mode')
            
            rank_pairs(datafolder, timeframe, timerange, outputfolder)
            click.echo(f"✅ Rank files written to {outputfolder}")

@cli.command(name='live')
@click.option('--symbols', required=True, type=str, help='Comma-separated trading pairs (e.g., BTC/USDT,ETH/USDT,XRP/USDT)')
@click.option('--timeframe', required=True, type=str, help='Candle timeframe (e.g., 1m, 5m, 1h, 4h, 1d)')
@click.option('--interval', required=False, type=int, default=60, help='Update interval in seconds (default: 60)')
@click.option('--metrics', required=False, type=str, help='Comma-separated metric names')
@click.option('--all-metrics', is_flag=True, help='Run all metrics in registry')
@click.option('--metrics-output', required=False, type=click.Path(path_type=Path), default='live_metrics.feather', help='Output file for metrics (default: live_metrics.feather)')
@click.option('--ranks-output', required=False, type=click.Path(path_type=Path), default='live_ranks', help='Output folder for rank files (default: live_ranks)')
@click.option('--exchange', required=False, type=str, default='binance', help='Exchange ID (default: binance)')
def live(symbols, timeframe, interval, metrics, all_metrics, metrics_output, ranks_output, exchange):
    """
    Run both metrics and ranking continuously on live data.
    
    This command fetches live OHLCV data from the exchange, computes market-wide metrics,
    and generates per-pair rankings. Results are saved and displayed each cycle.
    
    Example:
        metrex live --symbols BTC/USDT,ETH/USDT,XRP/USDT --timeframe 1m --interval 60 --all-metrics
    """
    # Determine metric names
    if all_metrics:
        metric_names = all_names()
    else:
        if not metrics:
            raise click.UsageError('Specify --metrics or --all-metrics')
        metric_names = [m.strip() for m in metrics.split(',')]
    
    symbol_list = [s.strip() for s in symbols.split(',')]
    
    from .live_runner import run_live_combined
    
    click.echo(f"🔴 Starting COMBINED LIVE mode (metrics + ranking)...")
    click.echo(f"Symbols: {', '.join(symbol_list)}")
    click.echo(f"Timeframe: {timeframe}")
    click.echo(f"Metrics: {', '.join(metric_names)}")
    click.echo(f"Interval: {interval}s")
    click.echo(f"Metrics output: {metrics_output}")
    click.echo(f"Ranks output: {ranks_output}")
    click.echo(f"Press Ctrl+C to stop.\n")
    
    run_live_combined(
        symbols=symbol_list,
        timeframe=timeframe,
        metric_names=metric_names,
        interval=interval,
        metrics_output=Path(metrics_output),
        ranks_output=Path(ranks_output),
        ctx={'exchange': exchange}
    )

@cli.command(name='list')
def list_metrics():
    """List available metric names in the registry."""
    names = all_names()
    if not names:
        click.echo("No metrics registered.")
    else:
        click.echo("Available metrics:\n- " + "\n- ".join(names))
