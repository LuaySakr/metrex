"""
Processor orchestrates: load -> filter -> run metrics -> merge -> save
"""
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from .io import load_feathers, save
from .timeutils import filter_timerange
from .metrics import get_selected, all_names, REGISTRY

def load_market(datafolder: Path, timeframe: str) -> pd.DataFrame:
    return load_feathers(datafolder, timeframe)

def run_metrics(df: pd.DataFrame, metric_names: List[str], ctx: Dict[str, Any]) -> pd.DataFrame:
    metrics = get_selected(metric_names)
    metric_frames = [m.compute(df, ctx) for m in metrics]
    # Outer join on date, then ffill, dropna(how='all') on metric columns
    result = metric_frames[0].set_index('date')
    for mf in metric_frames[1:]:
        result = result.join(mf.set_index('date'), how='outer')
    result = result.sort_index().ffill().dropna(how='all')
    result = result.reset_index()
    return result

def process(datafolder: Path, timeframe: str, timerange: str, metric_names: List[str], output: Path, ctx: Dict[str, Any] = {}):
    df = load_market(datafolder, timeframe)
    df = filter_timerange(df, timerange)
    result = run_metrics(df, metric_names, ctx)
    save(result, output)

def rank_pairs(datafolder: Path, timeframe: str, timerange: str, outputfolder: Path) -> None:
    """Generate per-pair feather files with cross-sectional ranks and stats.

    Output columns per pair:
    - date, open, high, low, close, volume
    - pairsCount: number of pairs available for that date (after filtering)
    - changePercentage24h: pct change in close vs exactly 24 hours earlier
    - topGainerRank: rank by changePercentage24h desc (1 = biggest gainer)
    - topLooserRank: rank by changePercentage24h asc (1 = biggest loser)
    - volumeInCurrency: volume * close
    - volumeInCurrency24: rolling 24h sum of volumeInCurrency (time-based)
    - topVolumeRank: rank by volumeInCurrency24 desc (1 = largest)
    - bottomVolumeRank: rank by volumeInCurrency24 asc (1 = smallest)
    """
    outputfolder = Path(outputfolder)
    outputfolder.mkdir(parents=True, exist_ok=True)

    # Load and filter market data
    df = load_market(Path(datafolder), timeframe)
    df = filter_timerange(df, timerange)

    if df.empty:
        raise ValueError("No data after filtering by timerange.")

    # Ensure proper dtypes and sorting
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['date', 'pair']).reset_index(drop=True)

    # pairsCount per date
    pairs_count_map = df.groupby('date')['pair'].nunique()
    df['pairsCount'] = df['date'].map(pairs_count_map)

    # 24h change: compute prior close exactly 24h earlier via self-merge
    close_prev = df[['pair', 'date', 'close']].copy()
    close_prev['date'] = close_prev['date'] + pd.Timedelta(hours=24)
    close_prev = close_prev.rename(columns={'close': 'close_prev_24h'})
    df = df.merge(close_prev, on=['pair', 'date'], how='left')
    df['changePercentage24h'] = ((df['close'] - df['close_prev_24h']) / df['close_prev_24h']) * 100.0

    # Volume in currency and its 24h rolling sum per pair (time-based window)
    df['volumeInCurrency'] = df['volume'] * df['close']

    def _calc_vic24(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values('date').set_index('date')
        # Use time-based window to cover any timeframe (1h/4h/1d)
        vic24 = g['volumeInCurrency'].rolling('24H').sum()
        g['volumeInCurrency24'] = vic24
        return g.reset_index()

    df = df.groupby('pair', group_keys=False).apply(_calc_vic24)

    # Cross-sectional ranks per date
    # Rank by changePercentage24h
    df['topGainerRank'] = df.groupby('date')['changePercentage24h'].rank(ascending=False, method='min')
    df['topLooserRank'] = df.groupby('date')['changePercentage24h'].rank(ascending=True, method='min')

    # Rank by volumeInCurrency24
    df['topVolumeRank'] = df.groupby('date')['volumeInCurrency24'].rank(ascending=False, method='min')
    df['bottomVolumeRank'] = df.groupby('date')['volumeInCurrency24'].rank(ascending=True, method='min')

    # Select and order columns
    out_cols = [
        'date', 'pair', 'open', 'high', 'low', 'close', 'volume',
        'pairsCount', 'changePercentage24h', 'topGainerRank', 'topLooserRank',
        'volumeInCurrency', 'volumeInCurrency24', 'topVolumeRank', 'bottomVolumeRank'
    ]
    df = df[out_cols]

    # Write per-pair outputs
    for pair, g in df.groupby('pair'):
        out_path = outputfolder / f"{pair}-{timeframe}.feather"
        save(g.drop(columns=['pair']), out_path)
