from __future__ import annotations

from typing import Dict, Iterable, Optional, Sequence, Set

import numpy as np
import pandas as pd

_DEFAULT_UNITS_CANDIDATES: Sequence[str] = (
    "unidades",
    "qtd_sku",
    "qtd",
    "quantidade",
    "unid",
)


def normalize_product_codes(series: object, index: Optional[pd.Index] = None) -> pd.Series:
    """Normaliza códigos de produto removendo sufixos numéricos e valores nulos."""
    if series is None:
        return pd.Series([], dtype=str, index=index)
    if isinstance(series, pd.Series):
        base_series = series
    else:
        base_series = pd.Series(series, index=index)

    def _normalize(value: object) -> str:
        if pd.isna(value):
            return ""
        if isinstance(value, (np.integer, int)):
            return str(int(value))
        if isinstance(value, (np.floating, float)):
            if not np.isfinite(value):
                return ""
            if float(value).is_integer():
                return str(int(value))
            value = f"{value:f}"
        text = str(value).strip()
        lowered = text.lower()
        if lowered in {"", "nan", "none", "null"}:
            return ""
        if "." in text:
            integer, fractional = text.split(".", 1)
            if fractional.strip("0") == "":
                return integer
        return text

    normalized = base_series.apply(_normalize).astype(str)
    return normalized


def detect_units_column(df: pd.DataFrame, candidates: Optional[Iterable[str]] = None) -> str:
    """Detecta a coluna que representa unidades vendidas/devolvidas."""
    search = candidates or _DEFAULT_UNITS_CANDIDATES
    for column in search:
        if column in df.columns:
            return column
    return "qtd_sku"


def ensure_period_series(
    df: pd.DataFrame,
    period_column: str,
    date_column: Optional[str],
) -> pd.Series:
    """Garante uma Series Period[M] usando coluna de período ou uma coluna de data de fallback."""
    if period_column in df.columns:
        series = df[period_column]
        if isinstance(series.dtype, pd.PeriodDtype):
            return series
        if pd.api.types.is_datetime64_any_dtype(series):
            return series.dt.to_period("M")
        converted = pd.to_datetime(series, dayfirst=True, errors="coerce")
        return converted.dt.to_period("M")

    if date_column and date_column in df.columns:
        converted = pd.to_datetime(df[date_column], dayfirst=True, errors="coerce")
        return converted.dt.to_period("M")

    return pd.Series(pd.PeriodIndex([pd.NaT] * len(df), freq="M"), index=df.index)


def build_period_product_totals(
    df: pd.DataFrame,
    *,
    period_column: str,
    product_column: str = "cd_produto",
    date_column: Optional[str] = None,
    allowed_periods: Optional[Set[str]] = None,
    units_column: Optional[str] = None,
    unit_result_name: str = "total_unidades",
    include_order_count: bool = False,
    order_result_name: str = "total_pedidos",
    extra_aggs: Optional[Dict[str, tuple[str, str]]] = None,
) -> pd.DataFrame:
    """Agrupa a base por período e produto, somando unidades e (opcionalmente) pedidos.

    O filtro de período é aplicado usando valores string no formato YYYY-MM.
    """
    if df is None or df.empty:
        columns = ["periodo", product_column, unit_result_name]
        if include_order_count:
            columns.append(order_result_name)
        if extra_aggs:
            columns.extend(dest for dest, _ in extra_aggs.values())
        return pd.DataFrame(columns=columns)

    working = df.copy()

    period_series = ensure_period_series(working, period_column, date_column)
    working["periodo"] = period_series.astype(str)

    if allowed_periods:
        allowed_str = {str(p) for p in allowed_periods if p and str(p).lower() != "nat"}
        working = working[working["periodo"].isin(allowed_str)].copy()
        if working.empty:
            columns = ["periodo", product_column, unit_result_name]
            if include_order_count:
                columns.append(order_result_name)
            if extra_aggs:
                columns.extend(dest for dest, _ in extra_aggs.values())
            return pd.DataFrame(columns=columns)

    if product_column in working.columns:
        working[product_column] = normalize_product_codes(working[product_column])
    else:
        working[product_column] = normalize_product_codes("", index=working.index)

    detected_units = units_column or detect_units_column(working)
    if detected_units not in working.columns:
        working[detected_units] = 0.0

    working["__units__"] = pd.to_numeric(working.get(detected_units, 0), errors="coerce").fillna(0.0)
    working["__rows__"] = 1

    aggregations: Dict[str, tuple[str, str]] = {
        unit_result_name: ("__units__", "sum"),
    }
    if include_order_count:
        aggregations[order_result_name] = ("__rows__", "sum")

    if extra_aggs:
        for source, (dest, func) in extra_aggs.items():
            aggregations[dest] = (source, func)

    grouped = (
        working.groupby(["periodo", product_column], as_index=False)
        .agg(**{dest: (source, func) for dest, (source, func) in aggregations.items()})
    )

    grouped[unit_result_name] = grouped[unit_result_name].fillna(0.0)
    if include_order_count:
        grouped[order_result_name] = grouped[order_result_name].fillna(0).astype(int)

    if extra_aggs:
        for dest, _ in extra_aggs.values():
            if dest in grouped.columns:
                grouped[dest] = grouped[dest].fillna(0.0)

    return grouped
