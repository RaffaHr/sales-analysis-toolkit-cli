from __future__ import annotations

from typing import Iterable

import pandas as pd


def format_percentage_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Retorna uma cópia do DataFrame com colunas percentuais normalizadas como float."""
    formatted = df.copy()
    for column in columns:
        if column not in formatted.columns:
            continue
        series = formatted[column]
        if pd.api.types.is_numeric_dtype(series):
            formatted[column] = pd.to_numeric(series, errors="coerce").fillna(0).astype(float)
        else:
            normalized = (
                series.astype(str)
                .str.replace("%", "", regex=False)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            numeric = pd.to_numeric(normalized, errors="coerce").fillna(0) / 100
            formatted[column] = numeric.astype(float)
        formatted[column] = formatted[column].round(6)
    return formatted
