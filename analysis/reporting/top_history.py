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

MIN_MONTHS_RECURRENCE = 3


def build_top_history_analysis(
    df: pd.DataFrame,
    category: Optional[str],
    rank_size: int = 20,
    historical_prices: Optional[Dict[str, float]] = None,
) -> Dict[str, pd.DataFrame]:
    """Gera ranking dos SKUs com melhor consistência histórica."""
    data = _filter_by_category(df, category)
    data["cd_produto"] = normalize_product_codes(data.get("cd_produto", ""))
    period_series = ensure_period_series(data, "periodo", "data")
    data["periodo"] = period_series.astype(str)
    allowed_periods: Set[str] = {
        p for p in data["periodo"].dropna().astype(str) if p and p.lower() != "nat"
    }
    returns_overall, returns_monthly = _compute_returns_totals(
        data,
        category=category,
        allowed_periods=allowed_periods,
    )

    interval_prices = (
        data.groupby("cd_anuncio")["preco_vendido"].min()
        .replace([np.inf, -np.inf], np.nan)
    )

    monthly = (
        data.groupby(["periodo", "cd_anuncio", "ds_anuncio"], as_index=False)
        .agg(
            qtd_vendida=("qtd_sku", "sum"),
            pedidos=("nr_nota_fiscal", "nunique"),
            receita=("rbld", "sum"),
            devolucao=("qtd_devolvido", "sum"),
            margem=("perc_margem_bruta", "mean"),
            preco_min_periodo=("preco_vendido", "min"),
            cd_produto=("cd_produto", "first"),
        )
    )
    monthly["periodo"] = monthly["periodo"].astype(str)
    monthly.drop(columns=["devolucao"], inplace=True, errors="ignore")
    monthly = monthly.merge(returns_monthly, on=["periodo", "cd_produto"], how="left")
    monthly["devolucao"] = monthly.get("devolucao", 0.0).fillna(0.0).astype(float)
    monthly["receita_devolucao"] = monthly.get("receita_devolucao", 0.0).fillna(0.0).astype(float)
    monthly["pedidos_devolvidos"] = monthly.get("pedidos_devolvidos", 0).fillna(0).astype(int)
    monthly["devolucao"] = monthly["devolucao"].round(2)
    monthly["receita_devolucao"] = monthly["receita_devolucao"].round(2)
    monthly["preco_medio_vendido"] = np.where(
        monthly["qtd_vendida"] > 0,
        monthly["receita"] / monthly["qtd_vendida"],
        0,
    ).round(2)
    monthly["preco_min_periodo"] = monthly["preco_min_periodo"].round(2)

    summary = (
        data.groupby(["cd_anuncio", "ds_anuncio"], as_index=False)
        .agg(
            meses_com_venda=("periodo", "nunique"),
            quantidade_total=("qtd_sku", "sum"),
            pedidos_total=("nr_nota_fiscal", "nunique"),
            receita_total=("rbld", "sum"),
            devolucao_total=("qtd_devolvido", "sum"),
            perc_margem_media_rbld=("perc_margem_bruta", "mean"),
            cd_produto=("cd_produto", "first"),
        )
    )
    summary.drop(columns=["devolucao_total"], inplace=True, errors="ignore")
    summary = summary.merge(returns_overall, on="cd_produto", how="left")
    summary["devolucao_total"] = summary.get("devolucao_total", 0.0).fillna(0.0).astype(float)
    summary["receita_devolucao_total"] = summary.get("receita_devolucao_total", 0.0).fillna(0.0).astype(float)
    summary["pedidos_devolvidos_total"] = summary.get("pedidos_devolvidos_total", 0).fillna(0).astype(int)
    summary["taxa_devolucao_total"] = np.where(
        summary["quantidade_total"] > 0,
        summary["devolucao_total"] / summary["quantidade_total"],
        0,
    )
    summary = summary[summary["meses_com_venda"] >= MIN_MONTHS_RECURRENCE]
    summary.sort_values(
        ["meses_com_venda", "quantidade_total", "receita_total"],
        ascending=[False, False, False],
        inplace=True,
    )

    ranking = summary.head(rank_size).copy()
    ranking["ticket_medio_estimado"] = np.where(
        ranking["pedidos_total"] > 0,
        ranking["receita_total"] / ranking["pedidos_total"],
        0,
    )
    ranking["ticket_medio_estimado"] = ranking["ticket_medio_estimado"].round(2)
    ranking["preco_medio_intervalo"] = np.where(
        ranking["quantidade_total"] > 0,
        ranking["receita_total"] / ranking["quantidade_total"],
        0,
    ).round(2)
    ranking["preco_min_intervalo"] = pd.to_numeric(
        ranking["cd_anuncio"].map(interval_prices), errors="coerce"
    ).round(2)
    if historical_prices:
        ranking["preco_min_historico_total"] = pd.to_numeric(
            ranking["cd_anuncio"].map(historical_prices), errors="coerce"
        ).round(2)
    else:
        ranking["preco_min_historico_total"] = np.nan

    detalhes = monthly[monthly["cd_anuncio"].isin(ranking["cd_anuncio"])].copy()
    detalhes["preco_min_historico_total"] = pd.to_numeric(
        detalhes["cd_anuncio"].map(historical_prices), errors="coerce"
    ).round(2) if historical_prices else np.nan
    detalhes["preco_min_intervalo"] = pd.to_numeric(
        detalhes["cd_anuncio"].map(interval_prices), errors="coerce"
    ).round(2)

    ranking_fmt = format_percentage_columns(
        ranking,
        ["taxa_devolucao_total", "perc_margem_media_rbld"],
    )
    detalhes_fmt = format_percentage_columns(detalhes, ["margem"])

    return {
        "ranking": ranking_fmt,
        "detalhe_mensal": detalhes_fmt,
    }


def _compute_returns_totals(
    data: pd.DataFrame,
    *,
    category: Optional[str],
    allowed_periods: Set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    returns_raw = data.attrs.get("returns_data", pd.DataFrame())
    returns_filtered = returns_report._filter_returns_dataset(returns_raw, category)
    prepared = returns_report._prepare_returns_dataset(returns_filtered)
    if prepared is None or prepared.empty:
        empty_overall = pd.DataFrame(
            columns=["cd_produto", "devolucao_total", "pedidos_devolvidos_total", "receita_devolucao_total"]
        )
        empty_monthly = pd.DataFrame(
            columns=["periodo", "cd_produto", "devolucao", "pedidos_devolvidos", "receita_devolucao"]
        )
        return empty_overall, empty_monthly

    prepared = prepared.copy()
    prepared["cd_produto"] = normalize_product_codes(prepared.get("cd_produto", ""), index=prepared.index)

    product_scope = set(data["cd_produto"].dropna().astype(str))
    if product_scope:
        prepared = prepared[prepared["cd_produto"].isin(product_scope)].copy()
        if prepared.empty:
            empty_overall = pd.DataFrame(
                columns=["cd_produto", "devolucao_total", "pedidos_devolvidos_total", "receita_devolucao_total"]
            )
            empty_monthly = pd.DataFrame(
                columns=["periodo", "cd_produto", "devolucao", "pedidos_devolvidos", "receita_devolucao"]
            )
            return empty_overall, empty_monthly

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
            empty_overall = pd.DataFrame(
                columns=["cd_produto", "devolucao_total", "pedidos_devolvidos_total", "receita_devolucao_total"]
            )
            empty_monthly = pd.DataFrame(
                columns=["periodo", "cd_produto", "devolucao", "pedidos_devolvidos", "receita_devolucao"]
            )
            return empty_overall, empty_monthly

    prepared["periodo"] = ensure_period_series(prepared, "periodo_venda", "data_venda").astype(str)
    if allowed_periods:
        prepared = prepared[prepared["periodo"].isin(allowed_periods)].copy()
        if prepared.empty:
            empty_overall = pd.DataFrame(
                columns=["cd_produto", "devolucao_total", "pedidos_devolvidos_total", "receita_devolucao_total"]
            )
            empty_monthly = pd.DataFrame(
                columns=["periodo", "cd_produto", "devolucao", "pedidos_devolvidos", "receita_devolucao"]
            )
            return empty_overall, empty_monthly

    monthly_totals = build_period_product_totals(
        prepared,
        period_column="periodo",
        product_column="cd_produto",
        date_column=None,
        allowed_periods=None,
        units_column="qtd_sku",
        unit_result_name="devolucao",
        include_order_count=True,
        order_result_name="pedidos_devolvidos",
        extra_aggs={
            "devolucao_receita_bruta": ("receita_devolucao", "sum"),
        },
    )

    if monthly_totals.empty:
        empty_overall = pd.DataFrame(
            columns=["cd_produto", "devolucao_total", "pedidos_devolvidos_total", "receita_devolucao_total"]
        )
        empty_monthly = pd.DataFrame(
            columns=["periodo", "cd_produto", "devolucao", "pedidos_devolvidos", "receita_devolucao"]
        )
        return empty_overall, empty_monthly

    monthly_totals["devolucao"] = monthly_totals["devolucao"].fillna(0.0)
    monthly_totals["receita_devolucao"] = monthly_totals["receita_devolucao"].fillna(0.0)
    monthly_totals["pedidos_devolvidos"] = monthly_totals["pedidos_devolvidos"].fillna(0).astype(int)

    overall_totals = (
        monthly_totals.groupby("cd_produto", as_index=False)
        .agg(
            devolucao_total=("devolucao", "sum"),
            pedidos_devolvidos_total=("pedidos_devolvidos", "sum"),
            receita_devolucao_total=("receita_devolucao", "sum"),
        )
    )
    overall_totals["devolucao_total"] = overall_totals["devolucao_total"].fillna(0.0)
    overall_totals["receita_devolucao_total"] = overall_totals["receita_devolucao_total"].fillna(0.0)
    overall_totals["pedidos_devolvidos_total"] = overall_totals["pedidos_devolvidos_total"].fillna(0).astype(int)

    return overall_totals, monthly_totals


def _filter_by_category(df: pd.DataFrame, category: Optional[str]) -> pd.DataFrame:
    if not category:
        filtered = df.copy()
    else:
        filtered = df[df["categoria"] == category].copy()
    filtered.attrs = dict(df.attrs)
    return filtered
