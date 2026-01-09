from __future__ import annotations

from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd

from ..formatting import format_percentage_columns
from . import returns as returns_report
from .common_returns import (
    build_period_product_totals,
    ensure_period_series,
    normalize_product_codes,
)


def build_product_focus_analysis(
    df: pd.DataFrame,
    category: Optional[str],
    product_codes: Optional[List[str]] = None,
) -> Dict[str, pd.DataFrame]:
    """Avalia o desempenho comercial filtrando por categoria ou lista específica de anúncios."""

    data = _filter_by_category(df, category)
    normalized_codes = (
        [str(code).strip() for code in product_codes if str(code).strip()]
        if product_codes
        else []
    )
    focus = (
        data[data["cd_anuncio"].isin(normalized_codes)].copy()
        if normalized_codes
        else data.copy()
    )
    focus["data"] = pd.to_datetime(
        focus.get("data"), dayfirst=True, errors="coerce"
    )
    focus = focus.dropna(subset=["data"])

    focus["data"] = focus["data"].dt.normalize()
    focus["cd_produto"] = normalize_product_codes(focus.get("cd_produto", ""))
    period_series = ensure_period_series(focus, "periodo", "data")
    focus["periodo"] = period_series.astype(str)
    allowed_periods: Set[str] = {
        p for p in focus["periodo"].dropna().astype(str) if p and p.lower() != "nat"
    }

    returns_overall, returns_monthly, returns_daily = _compute_returns_metrics(
        focus,
        category=category,
        allowed_periods=allowed_periods,
    )

    resumo = _aggregate_metrics(
        focus,
        group_cols=["cd_anuncio", "ds_anuncio", "cd_fabricante", "tp_anuncio"],
    )
    resumo.sort_values(["receita", "qtd_vendida"], ascending=[False, False], inplace=True)

    analise_diaria = _aggregate_metrics(
        focus,
        group_cols=["data", "cd_anuncio", "ds_anuncio", "cd_fabricante", "tp_anuncio"],
    )
    analise_diaria.sort_values(["data", "cd_anuncio"], inplace=True)

    analise_mensal = _aggregate_metrics(
        focus,
        group_cols=["periodo", "cd_anuncio", "ds_anuncio", "cd_fabricante", "tp_anuncio"],
    )
    analise_mensal.sort_values(["periodo", "cd_anuncio"], inplace=True)
    analise_mensal["periodo"] = analise_mensal["periodo"].astype(str)

    resumo = _merge_return_totals(resumo, returns_overall, key_cols=["cd_produto"])
    analise_diaria = _merge_return_totals(
        analise_diaria,
        returns_daily,
        key_cols=["data", "cd_produto"],
    )
    analise_mensal = _merge_return_totals(
        analise_mensal,
        returns_monthly,
        key_cols=["periodo", "cd_produto"],
    )

    resumo_fmt = format_percentage_columns(resumo, ["margem_media", "taxa_devolucao"])
    diaria_fmt = format_percentage_columns(analise_diaria, ["margem_media", "taxa_devolucao"])
    mensal_fmt = format_percentage_columns(analise_mensal, ["margem_media", "taxa_devolucao"])
    return {
        "resumo_produtos": resumo_fmt,
        "analise_diaria": diaria_fmt,
        "analise_mensal": mensal_fmt,
    }


def _aggregate_metrics(
    df: pd.DataFrame,
    group_cols: List[str],
) -> pd.DataFrame:
    working = df.copy()

    aggregations = {
        "pedidos": ("nr_nota_fiscal", "nunique"),
        "qtd_vendida": ("qtd_sku", "sum"),
        "receita": ("rbld", "sum"),
        "custo_total": ("custo_total", "sum"),
        "margem_media": ("perc_margem_bruta", "mean"),
        "qtd_devolvida": ("qtd_devolvido", "sum"),
        "receita_devolucao": ("devolucao_receita_bruta", "sum"),
        "lucro_bruto_estimado": ("lucro_bruto_estimado", "sum"),
        "preco_medio_praticado": ("preco_vendido", "mean"),
        "preco_min_periodo": ("preco_vendido", "min"),
    }

    for info_col in ("cd_produto", "ds_produto"):
        if info_col in working.columns and info_col not in group_cols:
            aggregations[info_col] = (info_col, "first")

    aggregated = working.groupby(group_cols, as_index=False).agg(aggregations)

    aggregated["preco_medio_praticado"] = aggregated["preco_medio_praticado"].round(2)
    aggregated["preco_min_periodo"] = aggregated["preco_min_periodo"].round(2)
    aggregated["receita"] = aggregated["receita"].round(2)
    aggregated["custo_total"] = aggregated["custo_total"].round(2)
    aggregated["receita_devolucao"] = aggregated["receita_devolucao"].round(2)
    aggregated["lucro_bruto_estimado"] = aggregated["lucro_bruto_estimado"].round(2)

    aggregated["ticket_medio"] = np.where(
        aggregated["pedidos"] > 0,
        aggregated["receita"] / aggregated["pedidos"],
        0,
    ).round(2)
    aggregated["preco_medio_vendido"] = np.where(
        aggregated["qtd_vendida"] > 0,
        aggregated["receita"] / aggregated["qtd_vendida"],
        0,
    ).round(2)
    aggregated["taxa_devolucao"] = np.where(
        aggregated["qtd_vendida"] > 0,
        aggregated["qtd_devolvida"] / aggregated["qtd_vendida"],
        0,
    )

    ordered_columns = [
        *(col for col in group_cols if col in aggregated.columns),
        *(col for col in ("cd_produto", "ds_produto") if col in aggregated.columns),
        "pedidos",
        "qtd_vendida",
        "receita",
        "ticket_medio",
        "preco_medio_vendido",
        "preco_medio_praticado",
        "preco_min_periodo",
        "margem_media",
        "lucro_bruto_estimado",
        "custo_total",
        "pedidos_devolvidos",
        "qtd_devolvida",
        "taxa_devolucao",
        "receita_devolucao",
    ]

    # Garantia de ordenacao consistente quando colunas adicionais forem adicionadas ao agrupamento
    existing_columns = [col for col in ordered_columns if col in aggregated.columns]
    remaining_columns = [col for col in aggregated.columns if col not in existing_columns]
    aggregated = aggregated[existing_columns + remaining_columns]
    return aggregated


def _filter_by_category(df: pd.DataFrame, category: Optional[str]) -> pd.DataFrame:
    if not category:
        filtered = df.copy()
    else:
        filtered = df[df["categoria"] == category].copy()
    filtered.attrs = dict(df.attrs)
    return filtered


def _compute_returns_metrics(
    focus_df: pd.DataFrame,
    *,
    category: Optional[str],
    allowed_periods: Set[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    returns_raw = focus_df.attrs.get("returns_data", pd.DataFrame())
    returns_filtered = returns_report._filter_returns_dataset(returns_raw, category)
    prepared = returns_report._prepare_returns_dataset(returns_filtered)
    if prepared is None or prepared.empty:
        return (
            pd.DataFrame(columns=["cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
            pd.DataFrame(columns=["periodo", "cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
            pd.DataFrame(columns=["data", "cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
        )

    prepared = prepared.copy()
    prepared["cd_produto"] = normalize_product_codes(prepared.get("cd_produto", ""), index=prepared.index)

    product_scope = set(focus_df.get("cd_produto", pd.Series(dtype=str)).dropna().astype(str))
    if product_scope:
        prepared = prepared[prepared["cd_produto"].isin(product_scope)].copy()
        if prepared.empty:
            return (
                pd.DataFrame(columns=["cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
                pd.DataFrame(columns=["periodo", "cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
                pd.DataFrame(columns=["data", "cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
            )

    focus_dates = pd.to_datetime(focus_df.get("data"), errors="coerce")
    min_date = focus_dates.min() if not focus_df.empty else None
    max_date = focus_dates.max() if not focus_df.empty else None

    prepared["data"] = pd.to_datetime(prepared.get("data_venda"), errors="coerce").dt.normalize()
    if min_date is not None and max_date is not None:
        prepared = prepared[
            (prepared["data"] >= min_date)
            & (prepared["data"] <= max_date)
        ].copy()
        if prepared.empty:
            return (
                pd.DataFrame(columns=["cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
                pd.DataFrame(columns=["periodo", "cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
                pd.DataFrame(columns=["data", "cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
            )

    prepared["periodo"] = ensure_period_series(prepared, "periodo_venda", "data_venda").astype(str)
    if allowed_periods:
        prepared = prepared[prepared["periodo"].isin(allowed_periods)].copy()
        if prepared.empty:
            return (
                pd.DataFrame(columns=["cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
                pd.DataFrame(columns=["periodo", "cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
                pd.DataFrame(columns=["data", "cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]),
            )

    monthly_totals = build_period_product_totals(
        prepared,
        period_column="periodo",
        product_column="cd_produto",
        date_column=None,
        allowed_periods=None,
        units_column="qtd_sku",
        unit_result_name="qtd_devolvida",
        include_order_count=True,
        order_result_name="pedidos_devolvidos",
        extra_aggs={
            "devolucao_receita_bruta": ("receita_devolucao", "sum"),
        },
    )

    if monthly_totals.empty:
        overall_totals = pd.DataFrame(
            columns=["cd_produto", "qtd_devolvida", "pedidos_devolvidos", "receita_devolucao"]
        )
    else:
        overall_totals = (
            monthly_totals.groupby("cd_produto", as_index=False)
            .agg(
                qtd_devolvida=("qtd_devolvida", "sum"),
                pedidos_devolvidos=("pedidos_devolvidos", "sum"),
                receita_devolucao=("receita_devolucao", "sum"),
            )
        )
        overall_totals["pedidos_devolvidos"] = overall_totals["pedidos_devolvidos"].fillna(0).astype(int)

    daily_totals = (
        prepared.groupby(["data", "cd_produto"], as_index=False)
        .agg(
            qtd_devolvida=("qtd_sku", "sum"),
            pedidos_devolvidos=("pedido_devolucao_id", "nunique"),
            receita_devolucao=("devolucao_receita_bruta", "sum"),
        )
    )
    if not daily_totals.empty:
        daily_totals["pedidos_devolvidos"] = daily_totals["pedidos_devolvidos"].fillna(0).astype(int)

    for frame in (monthly_totals, overall_totals, daily_totals):
        if frame.empty:
            continue
        frame["qtd_devolvida"] = frame["qtd_devolvida"].fillna(0.0)
        frame["receita_devolucao"] = frame["receita_devolucao"].fillna(0.0)
        if "pedidos_devolvidos" in frame.columns:
            frame["pedidos_devolvidos"] = frame["pedidos_devolvidos"].fillna(0).astype(int)

    return overall_totals, monthly_totals, daily_totals


def _merge_return_totals(
    df: pd.DataFrame,
    totals: pd.DataFrame,
    *,
    key_cols: List[str],
) -> pd.DataFrame:
    if df is None or df.empty:
        if "qtd_devolvida" not in df.columns:
            df["qtd_devolvida"] = 0.0
        if "pedidos_devolvidos" not in df.columns:
            df["pedidos_devolvidos"] = 0
        if "receita_devolucao" not in df.columns:
            df["receita_devolucao"] = 0.0
        df["taxa_devolucao"] = np.where(
            df.get("qtd_vendida", 0) > 0,
            df.get("qtd_devolvida", 0) / df.get("qtd_vendida", 1),
            0,
        )
        return df

    working = df.drop(
        columns=[
            c
            for c in ("qtd_devolvida", "pedidos_devolvidos", "receita_devolucao", "taxa_devolucao")
            if c in df.columns
        ],
        errors="ignore",
    )

    if totals is None or totals.empty:
        working["qtd_devolvida"] = 0.0
        working["pedidos_devolvidos"] = 0
        working["receita_devolucao"] = 0.0
    else:
        working = working.merge(totals, on=key_cols, how="left")
        for col, default, dtype in (
            ("qtd_devolvida", 0.0, float),
            ("receita_devolucao", 0.0, float),
            ("pedidos_devolvidos", 0, int),
        ):
            if col not in working.columns:
                working[col] = default
            else:
                working[col] = working[col].fillna(default).astype(dtype)

    working["qtd_devolvida"] = working["qtd_devolvida"].astype(float).round(2)
    working["receita_devolucao"] = working["receita_devolucao"].astype(float).round(2)
    working["taxa_devolucao"] = np.where(
        working.get("qtd_vendida", 0) > 0,
        working["qtd_devolvida"] / working["qtd_vendida"],
        0,
    )

    return working
