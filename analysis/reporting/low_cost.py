from __future__ import annotations

from typing import Dict, Optional, Set

import numpy as np
import pandas as pd

from ..formatting import format_percentage_columns
from . import returns as returns_report
from .common_returns import (
    build_period_product_totals,
    ensure_period_series,
    normalize_product_codes,
)

COST_PERCENTILE = 0.25
MIN_QUANTITY = 50
MAX_RETURN_RATE = 0.05


def build_low_cost_reputation_analysis(
    df: pd.DataFrame,
    category: Optional[str],
    cost_percentile: float = COST_PERCENTILE,
    min_quantity: int = MIN_QUANTITY,
    max_return_rate: float = MAX_RETURN_RATE,
    historical_prices: Optional[Dict[str, float]] = None,
) -> Dict[str, pd.DataFrame]:
    """Sugere itens baratos, com boa saída e baixa devolução para fortalecer reputação."""
    data = _filter_by_category(df, category)
    data["cd_produto"] = normalize_product_codes(data.get("cd_produto", ""))
    period_series = ensure_period_series(data, "periodo", "data")
    data["periodo"] = period_series.astype(str)
    allowed_periods: Set[str] = {
        p for p in data["periodo"].dropna().astype(str) if p and p.lower() != "nat"
    }
    returns_totals = _compute_returns_totals(data, category, allowed_periods)

    interval_prices = (
        data.groupby("cd_anuncio")["preco_vendido"].min()
        .replace([np.inf, -np.inf], np.nan)
    )

    aggregated = (
        data.groupby(["cd_anuncio", "ds_anuncio"], as_index=False)
        .agg(
            quantidade_total=("qtd_sku", "sum"),
            pedidos_total=("nr_nota_fiscal", "nunique"),
            receita_total=("rbld", "sum"),
            custo_medio_unitario=("custo_produto", "mean"),
            custo_total=("custo_total", "sum"),
            devolucao_total=("qtd_devolvido", "sum"),
            receita_devolucao_total=("devolucao_receita_bruta", "sum"),
            margem_media=("perc_margem_bruta", "mean"),
            cd_produto=("cd_produto", "first"),
        )
    )

    if aggregated.empty:
        return {"produtos_indicados": aggregated}

    aggregated.drop(
        columns=["devolucao_total", "receita_devolucao_total"],
        inplace=True,
        errors="ignore",
    )
    aggregated = aggregated.merge(returns_totals, on="cd_produto", how="left")
    aggregated["devolucao_total"] = aggregated.get("devolucao_total", 0.0).fillna(0.0)
    aggregated["receita_devolucao_total"] = aggregated.get("receita_devolucao_total", 0.0).fillna(0.0)
    aggregated["pedidos_devolvidos_total"] = aggregated.get("pedidos_devolvidos_total", 0).fillna(0).astype(int)

    aggregated["taxa_devolucao"] = np.where(
        aggregated["quantidade_total"] > 0,
        aggregated["devolucao_total"] / aggregated["quantidade_total"],
        0,
    )
    aggregated["ticket_medio_estimado"] = np.where(
        aggregated["pedidos_total"] > 0,
        aggregated["receita_total"] / aggregated["pedidos_total"],
        0,
    )

    custo_threshold = aggregated["custo_medio_unitario"].quantile(cost_percentile)

    selecionados = aggregated[
        (aggregated["custo_medio_unitario"] <= custo_threshold)
        & (aggregated["quantidade_total"] >= min_quantity)
        & (aggregated["taxa_devolucao"] <= max_return_rate)
    ].copy()

    selecionados.sort_values(
        ["custo_medio_unitario", "quantidade_total"],
        ascending=[True, False],
        inplace=True,
    )

    selecionados["potencial_reputacao_score"] = (
        (1 - selecionados["taxa_devolucao"]) * selecionados["quantidade_total"]
    ) / np.where(
        selecionados["custo_medio_unitario"] > 0,
        selecionados["custo_medio_unitario"],
        1,
    )
    selecionados.sort_values(
        "potencial_reputacao_score",
        ascending=False,
        inplace=True,
    )

    selecionados["preco_min_intervalo"] = pd.to_numeric(
        selecionados["cd_anuncio"].map(interval_prices), errors="coerce"
    ).round(2)
    if historical_prices:
        selecionados["preco_min_historico_total"] = pd.to_numeric(
            selecionados["cd_anuncio"].map(historical_prices), errors="coerce"
        ).round(2)
    else:
        selecionados["preco_min_historico_total"] = np.nan

    selecionados_fmt = format_percentage_columns(
        selecionados,
        ["taxa_devolucao", "margem_media"],
    )

    return {"produtos_indicados": selecionados_fmt}


def _filter_by_category(df: pd.DataFrame, category: Optional[str]) -> pd.DataFrame:
    if not category:
        filtered = df.copy()
    else:
        filtered = df[df["categoria"] == category].copy()
    filtered.attrs = dict(df.attrs)
    return filtered


def _compute_returns_totals(
    data: pd.DataFrame,
    category: Optional[str],
    allowed_periods: Set[str],
) -> pd.DataFrame:
    returns_raw = data.attrs.get("returns_data", pd.DataFrame())
    returns_filtered = returns_report._filter_returns_dataset(returns_raw, category)
    prepared = returns_report._prepare_returns_dataset(returns_filtered)
    if prepared is None or prepared.empty:
        return pd.DataFrame(
            columns=[
                "cd_produto",
                "devolucao_total",
                "pedidos_devolvidos_total",
                "receita_devolucao_total",
            ]
        )

    prepared = prepared.copy()
    prepared["cd_produto"] = normalize_product_codes(prepared.get("cd_produto", ""), index=prepared.index)

    product_scope = set(data["cd_produto"].dropna().astype(str))
    if product_scope:
        prepared = prepared[prepared["cd_produto"].isin(product_scope)].copy()
        if prepared.empty:
            return pd.DataFrame(
                columns=[
                    "cd_produto",
                    "devolucao_total",
                    "pedidos_devolvidos_total",
                    "receita_devolucao_total",
                ]
            )

    data_dates = pd.to_datetime(data.get("data"), dayfirst=True, errors="coerce").dt.normalize()
    min_date = data_dates.min() if not data_dates.empty else None
    max_date = data_dates.max() if not data_dates.empty else None

    prepared["data"] = pd.to_datetime(prepared.get("data_venda"), errors="coerce").dt.normalize()
    if min_date is not None and max_date is not None:
        prepared = prepared[
            (prepared["data"] >= min_date)
            & (prepared["data"] <= max_date)
        ].copy()
        if prepared.empty:
            return pd.DataFrame(
                columns=[
                    "cd_produto",
                    "devolucao_total",
                    "pedidos_devolvidos_total",
                    "receita_devolucao_total",
                ]
            )

    prepared["periodo"] = ensure_period_series(prepared, "periodo_venda", "data_venda").astype(str)
    if allowed_periods:
        prepared = prepared[prepared["periodo"].isin(allowed_periods)].copy()
        if prepared.empty:
            return pd.DataFrame(
                columns=[
                    "cd_produto",
                    "devolucao_total",
                    "pedidos_devolvidos_total",
                    "receita_devolucao_total",
                ]
            )

    totals = build_period_product_totals(
        prepared,
        period_column="periodo",
        product_column="cd_produto",
        date_column=None,
        allowed_periods=None,
        units_column="qtd_sku",
        unit_result_name="devolucao_total",
        include_order_count=True,
        order_result_name="pedidos_devolvidos_total",
        extra_aggs={
            "devolucao_receita_bruta": ("receita_devolucao_total", "sum"),
        },
    )

    if totals.empty:
        return pd.DataFrame(
            columns=[
                "cd_produto",
                "devolucao_total",
                "pedidos_devolvidos_total",
                "receita_devolucao_total",
            ]
        )

    aggregated = (
        totals.groupby("cd_produto", as_index=False)
        .agg(
            devolucao_total=("devolucao_total", "sum"),
            pedidos_devolvidos_total=("pedidos_devolvidos_total", "sum"),
            receita_devolucao_total=("receita_devolucao_total", "sum"),
        )
    )
    aggregated["pedidos_devolvidos_total"] = aggregated["pedidos_devolvidos_total"].fillna(0).astype(int)
    aggregated["devolucao_total"] = aggregated["devolucao_total"].fillna(0.0)
    aggregated["receita_devolucao_total"] = aggregated["receita_devolucao_total"].fillna(0.0)
    return aggregated
