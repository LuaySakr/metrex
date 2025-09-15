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
@click.option('--datafolder', required=True, type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path))
@click.option('--timeframe', required=True, type=str)
@click.option('--timerange', required=True, type=str)
@click.option('--metrics', required=False, type=str, help='Comma-separated metric names')
@click.option('--all-metrics', is_flag=True, help='Run all metrics in registry')
@click.option('--output', required=True, type=click.Path(path_type=Path))
def metrics(datafolder, timeframe, timerange, metrics, all_metrics, output):
    """
    Run selected market metrics and save results.
    """
    if all_metrics:
        metric_names = all_names()
    else:
        if not metrics:
            raise click.UsageError('Specify --metrics or --all-metrics')
        metric_names = [m.strip() for m in metrics.split(',')]
    process(datafolder, timeframe, timerange, metric_names, output)
    click.echo(f"✅ Metrics computed: {', '.join(metric_names)}\nSaved to {output}")

if __name__ == '__main__':
    cli()

@cli.command()
@click.option('--datafolder', required=True, type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path))
@click.option('--timeframe', required=True, type=str)
@click.option('--timerange', required=True, type=str)
@click.option('--outputfolder', required=True, type=click.Path(path_type=Path))
def rank(datafolder, timeframe, timerange, outputfolder):
    """Generate per-pair ranked metrics and write one feather per pair."""
    rank_pairs(datafolder, timeframe, timerange, outputfolder)
    click.echo(f"✅ Rank files written to {outputfolder}")

@cli.command(name='list')
def list_metrics():
    """List available metric names in the registry."""
    names = all_names()
    if not names:
        click.echo("No metrics registered.")
    else:
        click.echo("Available metrics:\n- " + "\n- ".join(names))
