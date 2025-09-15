"""
IO utilities for metrex: load/save feather/parquet/csv
"""
import pandas as pd
from pathlib import Path

def load_feathers(datafolder: Path, timeframe: str) -> pd.DataFrame:
    pattern = f"*-{timeframe}.feather"
    files = list(Path(datafolder).glob(pattern))
    dfs = []
    for f in files:
        pair = f.stem.split('-')[0]
        df = pd.read_feather(f)
        if 'date' not in df.columns:
            if 'timestamp' in df.columns:
                df['date'] = pd.to_datetime(df['timestamp'])
            else:
                raise ValueError(f"No date/timestamp column in {f}")
        df['date'] = pd.to_datetime(df['date'])
        df['pair'] = pair
        dfs.append(df[['date','pair','open','high','low','close','volume']])
    if not dfs:
        raise ValueError(f"No feather files found for {timeframe} in {datafolder}")
    return pd.concat(dfs, ignore_index=True)

def save(df: pd.DataFrame, output_path: Path):
    ext = str(output_path).split('.')[-1]
    if ext == 'feather':
        df.reset_index(drop=True).to_feather(output_path, compression_level=9, compression="lz4")
    elif ext == 'parquet':
        df.reset_index(drop=True).to_parquet(output_path)
    elif ext == 'csv':
        df.to_csv(output_path, index=False)
    else:
        raise ValueError(f"Unknown output format: {ext}")
