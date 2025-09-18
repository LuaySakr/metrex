"""
Processor orchestrates: load -> filter -> run metrics -> merge -> save
"""
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
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

def _parse_timerange_bounds(timerange: str) -> Tuple[str, str]:
    """Return raw start,end strings (may include 'latest')."""
    if '-' not in timerange:
        raise ValueError(f"Invalid timerange format: {timerange}")
    return tuple(timerange.split('-', 1))  # type: ignore

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

    # Determine timerange handling (supports 'latest-YYYYMMDD')
    start_raw, end_raw = _parse_timerange_bounds(timerange)
    df_all = load_market(Path(datafolder), timeframe)

    # If start_raw == 'latest', we will compute per-pair dynamic start dates based on existing outputs.
    use_latest = start_raw.lower() == 'latest'
    end_bound = end_raw

    # Precompute parsed end timestamp (timezone-naive normalized to UTC at midnight)
    end_ts = pd.to_datetime(end_bound, format='%Y%m%d')
    end_ts = end_ts.tz_localize('UTC') if end_ts.tzinfo is None else end_ts

    # Normalize date column
    df_all['date'] = pd.to_datetime(df_all['date'])
    # If any rows are tz-aware, convert all to UTC; else localize to UTC for uniformity
    if df_all['date'].dt.tz is not None:
        df_all['date'] = df_all['date'].dt.tz_convert('UTC')
    else:
        df_all['date'] = df_all['date'].dt.tz_localize('UTC')

    # Build per-pair start date map
    pair_start_map: Dict[str, pd.Timestamp] = {}
    lookback_hours = 24
    lookback_delta = pd.Timedelta(hours=lookback_hours)
    if use_latest:
        # For each pair, inspect existing output file (if any) to find last date
        for pair in df_all['pair'].unique():
            out_file = outputfolder / f"{pair}-{timeframe}.feather"
            if out_file.exists():
                try:
                    existing = pd.read_feather(out_file)
                    if 'date' not in existing.columns or existing.empty:
                        continue
                    existing['date'] = pd.to_datetime(existing['date'])
                    # Normalize existing to UTC
                    if existing['date'].dt.tz is None:
                        existing['date'] = existing['date'].dt.tz_localize('UTC')
                    else:
                        existing['date'] = existing['date'].dt.tz_convert('UTC')
                    last_date = existing['date'].max()
                    # Start from (last_date - lookback) to compute correct rolling metrics; we'll drop <= last_date later
                    pair_start_map[pair] = last_date - lookback_delta
                except Exception:
                    continue
        # If file missing, we set later to first available date per pair
    else:
        start_ts = pd.to_datetime(start_raw, format='%Y%m%d')
        start_ts = start_ts.tz_localize('UTC') if start_ts.tzinfo is None else start_ts
        for pair in df_all['pair'].unique():
            pair_start_map[pair] = start_ts

    # Filter df_all into df respecting per-pair starts
    filtered_frames = []
    for pair, g in df_all.groupby('pair'):
        if use_latest:
            start_ts = pair_start_map.get(pair)
            if start_ts is None:
                start_ts = g['date'].min()
                pair_start_map[pair] = start_ts
        else:
            start_ts = pair_start_map[pair]
        # Ensure same tz
        if start_ts.tzinfo is None:
            start_ts = start_ts.tz_localize('UTC')
        mask = (g['date'] >= start_ts) & (g['date'] <= end_ts)
        sub = g.loc[mask]
        if not sub.empty:
            filtered_frames.append(sub)
    if not filtered_frames:
        return  # Nothing new to process
    df = pd.concat(filtered_frames, ignore_index=True)

    if df.empty:
        raise ValueError("No data after filtering by timerange.")

    # Ensure proper dtypes and sorting
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    if df['date'].dt.tz is None:
        df['date'] = df['date'].dt.tz_localize('UTC')
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

    # Write per-pair outputs (append logic, avoiding duplicates)
    for pair, g in df.groupby('pair'):
        out_path = outputfolder / f"{pair}-{timeframe}.feather"
        g_out = g.drop(columns=['pair']).copy()
        # If latest mode with existing file, drop rows with date <= last existing date
        if use_latest:
            existing_last: Optional[pd.Timestamp] = None
            if (pair in pair_start_map) and (pair_start_map[pair] is not None):
                # pair_start_map[pair] stored (last_existing - lookback) for latest case, so recover last_existing
                existing_last = pair_start_map[pair] + lookback_delta
            if existing_last is not None:
                g_out = g_out[g_out['date'] > existing_last]
                if g_out.empty:
                    continue
        if out_path.exists():
            try:
                existing = pd.read_feather(out_path)
                if 'date' in existing.columns:
                    existing['date'] = pd.to_datetime(existing['date'])
                    if existing['date'].dt.tz is None:
                        existing['date'] = existing['date'].dt.tz_localize('UTC')
                    else:
                        existing['date'] = existing['date'].dt.tz_convert('UTC')
                    g_out['date'] = pd.to_datetime(g_out['date'])
                    if g_out['date'].dt.tz is None:
                        g_out['date'] = g_out['date'].dt.tz_localize('UTC')
                    else:
                        g_out['date'] = g_out['date'].dt.tz_convert('UTC')
                    # Exclude any rows in g_out with dates already present
                    existing_dates = set(existing['date'].astype('int64'))
                    g_out = g_out[~g_out['date'].astype('int64').isin(existing_dates)]
                    if g_out.empty:
                        continue
                    combined = pd.concat([existing, g_out], ignore_index=True)
                    combined = combined.drop_duplicates(subset=['date']).sort_values('date')
                else:
                    combined = g_out
            except Exception:
                combined = g_out
            save(combined, out_path)
        else:
            save(g_out, out_path)
