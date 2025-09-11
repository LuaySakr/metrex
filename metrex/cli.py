"""
CLI interface for metrex.
"""

import click
import sys
from pathlib import Path
from typing import Optional

from .core import MetrexProcessor


@click.command()
@click.option(
    '--datafolder',
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help='Path to directory containing Freqtrade .feather candle files'
)
@click.option(
    '--timeframe',
    required=True,
    type=str,
    help='Timeframe to filter by (e.g., 1h, 4h, 1d)'
)
@click.option(
    '--timerange',
    required=True,
    type=str,
    help='Time range to filter by (e.g., 20230101-20231231)'
)
@click.option(
    '--output',
    required=True,
    type=click.Path(path_type=Path),
    help='Output path for results .feather file'
)
def main(datafolder: Path, timeframe: str, timerange: str, output: Path) -> None:
    """
    Metrex: Load Freqtrade candles, compute market metrics, and save results.
    
    Computes breadth_above_sma_50, market_vol_regime, and btc_trend_slope metrics
    from Freqtrade .feather candle data.
    """
    try:
        # Initialize processor
        processor = MetrexProcessor(datafolder)
        
        # Load and process data
        click.echo(f"Loading data from {datafolder}")
        click.echo(f"Filtering by timeframe: {timeframe}")
        click.echo(f"Filtering by timerange: {timerange}")
        
        # Process the data
        results = processor.process(timeframe, timerange)
        
        # Save results
        click.echo(f"Saving results to {output}")
        processor.save_results(results, output)
        
        click.echo("✅ Processing completed successfully!")
        
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()