from __future__ import annotations
from typing import Dict, Any
import pandas as pd
from .base import all_conditions


def evaluate_all(df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
    rows = []
    conds = all_conditions()
    for cond in conds:
        series = cond.evaluate(df, ctx)
        if series.index.equals(df.index) is False:
            series = series.reindex(df.index)
        rows.append(series.rename(cond.id))
    mat = pd.DataFrame(rows).T  # shape (n_rows, n_conditions) boolean
    return mat


def compress_boolean_matrix(mat: pd.DataFrame, prefix: str = 'mc') -> pd.Series:
    """Compress boolean matrix into underscore-joined condition identifiers.

    Each True condition in a row is encoded as f"{prefix}{id}" preserving ascending
    numeric order of the original condition IDs.
    """
    cols_sorted = sorted(mat.columns)
    def encode_row(row):
        active = [f"{prefix}{col}" for col in cols_sorted if bool(row[col])]
        return '_'.join(active) if active else ''
    return mat.apply(encode_row, axis=1)
