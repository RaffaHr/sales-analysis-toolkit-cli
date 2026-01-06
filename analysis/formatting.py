from __future__ import annotations

from typing import Iterable

import pandas as pd


def format_percentage_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Retorna cópia do DataFrame com colunas porcentuais formatadas em texto."""
    formatted = df.copy()
    for column in columns:
        if column not in formatted.columns:
            continue
        formatted[column] = formatted[column].apply(_format_percentage_value)
    return formatted


def _format_percentage_value(value: object) -> str:
    if pd.isna(value):
        return "0.00%"
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{numeric_value * 100:.2f}%"
