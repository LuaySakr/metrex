"""
CLI interface for metrex.
"""


import click
import pandas as pd
from pathlib import Path
from .processor import process, rank_pairs
from .metrics import all_names
from .io import read_metrics, save
from .conditions import build_context, evaluate_all, compress_boolean_matrix

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

@cli.command()
@click.option('--metrics-file', required=True, type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option('--output', required=True, type=click.Path(path_type=Path), help='Output path (.feather/.csv/.parquet)')
@click.option('--wide', is_flag=True, help='Output wide boolean table instead of compressed id list')
def conditions(metrics_file, output, wide):
    """Evaluate registered market conditions on a metrics file."""
    df = read_metrics(metrics_file)
    print("[DEBUG] Metrics DataFrame shape:", df.shape)
    print("[DEBUG] Metrics DataFrame head:\n", df.head())
    if 'date' not in df.columns:
        raise click.UsageError('metrics file must contain date column')
    df = df.sort_values('date').reset_index(drop=True)
    ctx = build_context(df)
    cond_matrix = evaluate_all(df, ctx)
    print("[DEBUG] Condition matrix shape:", cond_matrix.shape)
    print("[DEBUG] Condition matrix head:\n", cond_matrix.head())
    cond_matrix.index = df['date']
    if wide:
        # Prefix column names with mc for clarity
        renamed = {c: f"mc{c}" for c in cond_matrix.columns}
        cond_matrix = cond_matrix.rename(columns=renamed)
        out_df = cond_matrix.reset_index().rename(columns={'index':'date'})
    else:
        compressed = compress_boolean_matrix(cond_matrix, prefix='mc')
        # Align by index, drop rows where either is missing
        aligned = pd.DataFrame({'date': df['date'], 'conditions': compressed})
        out_df = aligned.dropna(subset=['date', 'conditions']).reset_index(drop=True)
    print("[DEBUG] Output DataFrame shape:", out_df.shape)
    print("[DEBUG] Output DataFrame head:\n", out_df.head())
    save(out_df, output)
    click.echo(f"✅ Evaluated {cond_matrix.shape[1]} conditions -> {output}")

if __name__ == '__main__':
    cli()

@cli.command()
@click.option('--datafolder', required=True, type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path))
@click.option('--timeframe', required=True, type=str)
@click.option('--timerange', required=True, type=str)
@click.option('--outputfolder', required=True, type=click.Path(path_type=Path))
def rank(datafolder, timeframe, timerange, outputfolder):
        """Generate/append per-pair ranked metrics (no duplicate dates).

        Behavior:
        - If output files exist, new rows are appended; existing dates are not duplicated.
        - Timerange supports special form 'latest-YYYYMMDD' where the start date is
            taken as the last date already present in each pair's output (or the first
            available input date if the output file does not yet exist).
        """
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
