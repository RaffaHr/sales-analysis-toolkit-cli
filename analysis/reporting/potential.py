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

RECENT_WINDOW = 3
MIN_HIST_MONTHS = 3
MIN_DROP_RATIO = 0.3


def build_potential_sku_analysis(
    df: pd.DataFrame,
    category: Optional[str],
    rank_size: int = 20,
    historical_prices: Optional[Dict[str, float]] = None,
    recent_periods: Optional[list[str]] = None,
    recent_window: int = RECENT_WINDOW,
) -> Dict[str, pd.DataFrame]:
    """Identifica SKUs com queda recente, mas histórico forte de vendas.
    
    Exemplo:
    Imagine o SKU “PRODUTO A” com vendas de janeiro/2024 a janeiro/2026:

    2024/01: 120 unidades
    2024/02: 100
    2024/03: 110
    … segue firme até 2025/06, sempre entre 90 e 130 unidades → esse conjunto de meses forma o histórico forte do produto.
    Agora considere os últimos três meses (janela recente que você escolheu na CLI):

    2025/11: 55 unidades
    2025/12: 45
    2026/01: 35
    Quando rodamos a análise:

    Histórico = todo o período EXCETO os meses selecionados na janela recente (jan/24 a out/25). Calculamos aí a média de vendas, pedidos, margem etc. Suponha que a média de unidades no histórico seja 105.
    Janela recente = períodos que você escolheu (nov/25, dez/25, jan/26). A média ficou em 45 unidades.
    Comparando:

    Queda absoluta = 105 – 45 = 60 unidades.
    Queda percentual = 60 / 105 ≈ 57%.

    Como o produto tinha desempenho constante por quase dois anos e despencou nos meses recentes, ele entra como candidato: o histórico mostra potencial, a janela recente sinaliza queda e, se a taxa de devolução/margem estiver aceitável, o relatório classifica esse SKU como “em potencial".
    """
    data = _filter_by_category(df, category)

    if "cd_produto" in data.columns:
        data["cd_produto"] = normalize_product_codes(data["cd_produto"])
    else:
        data["cd_produto"] = normalize_product_codes("", index=data.index)

    period_series = ensure_period_series(data, "periodo", "data")
    data["periodo"] = period_series.astype(str)
    allowed_periods: Set[str] = {
        str(p) for p in period_series.dropna().tolist() if str(p) and str(p).lower() != "nat"
    }

    produto_map = pd.Series(dtype="object")
    if "cd_anuncio" in data.columns:
        produto_map = (
            data.loc[data["cd_anuncio"].notna(), ["cd_anuncio", "cd_produto"]]
            .drop_duplicates(subset=["cd_anuncio"])
            .set_index("cd_anuncio")["cd_produto"]
        )

    categoria_map = None
    categoria_default = category if category is not None else ""
    if "categoria" in data.columns:
        categoria_map = (
            data.loc[data["cd_anuncio"].notna(), ["cd_anuncio", "categoria"]]
            .drop_duplicates(subset=["cd_anuncio"])
            .set_index("cd_anuncio")["categoria"]
        )

    returns_raw = data.attrs.get("returns_data", pd.DataFrame())
    returns_filtered = returns_report._filter_returns_dataset(returns_raw, category)
    return_totals = _build_returns_totals_by_sale_period(returns_filtered, allowed_periods)

    interval_prices = (
        data.groupby("cd_anuncio")["preco_vendido"].min()
        .replace([np.inf, -np.inf], np.nan)
    )
    grouped = (
        data.groupby(["periodo", "cd_produto", "cd_anuncio", "ds_anuncio"], as_index=False)
        .agg(
            qtd_vendida=("qtd_sku", "sum"),
            pedidos=("nr_nota_fiscal", "nunique"),
            receita=("rbld", "sum"),
            custo=("custo_total", "sum"),
            margem_media=("perc_margem_bruta", "mean"),
            preco_min_periodo=("preco_vendido", "min"),
        )
    )

    if not return_totals.empty:
        grouped = grouped.merge(
            return_totals,
            on=["periodo", "cd_produto"],
            how="left",
        )

    if "qtd_devolvida_ret" in grouped.columns:
        grouped["qtd_devolvida"] = grouped["qtd_devolvida_ret"].fillna(0.0)
        grouped["pedidos_devolvidos"] = (
            grouped["pedidos_devolvidos_ret"].fillna(0).astype(int)
        )
        grouped["receita_devolucao"] = grouped["receita_devolucao_ret"].fillna(0.0)
        grouped.drop(
            columns=[col for col in [
                "qtd_devolvida_ret",
                "pedidos_devolvidos_ret",
                "receita_devolucao_ret",
            ] if col in grouped.columns],
            inplace=True,
        )
    else:
        grouped["qtd_devolvida"] = 0.0
        grouped["pedidos_devolvidos"] = 0
        grouped["receita_devolucao"] = 0.0

    if grouped.empty:
        return {"potenciais": grouped, "skus_potenciais_mensal": grouped}

    grouped["periodo"] = grouped["periodo"].astype(str)
    grouped.sort_values("periodo", inplace=True)
    grouped["preco_min_periodo"] = grouped["preco_min_periodo"].round(2)
    available_periods = grouped["periodo"].unique()

    if recent_periods:
        selected = sorted({str(p) for p in recent_periods})
        valid_selected = [p for p in selected if p in available_periods]
        if not valid_selected:
            return {"potenciais": grouped.head(0), "skus_potenciais_mensal": grouped.head(0)}
        historical_periods = [p for p in available_periods if p not in valid_selected]
        if not historical_periods:
            return {"potenciais": grouped.head(0), "skus_potenciais_mensal": grouped.head(0)}
        recent_periods = np.array(valid_selected)
        historical_periods = np.array(historical_periods)
    else:
        if len(available_periods) <= recent_window:
            recent_window = max(1, len(available_periods) // 2 or 1)
        recent_periods = available_periods[-recent_window:]
        historical_periods = available_periods[:-recent_window]

    recent = _aggregate_window(grouped, recent_periods, suffix="recente")
    historical = _aggregate_window(grouped, historical_periods, suffix="historico")

    stats = historical.merge(
        recent,
        on=["cd_anuncio", "ds_anuncio"],
        how="left",
    ).fillna(0)

    stats["queda_abs_qtd"] = stats["qtd_vendida_media_historico"] - stats[
        "qtd_vendida_media_recente"
    ]
    stats["queda_pct_qtd"] = np.where(
        stats["qtd_vendida_media_historico"] > 0,
        stats["queda_abs_qtd"] / stats["qtd_vendida_media_historico"],
        0,
    )
    stats["potencial_score"] = (
        stats["queda_abs_qtd"].clip(lower=0)
        * stats["historico_meses_validos"]
        * (1 - stats["taxa_devolucao_media_historico"])
    )

    eligible = stats.loc[
        stats["historico_meses_validos"] >= MIN_HIST_MONTHS,
        "qtd_vendida_media_historico",
    ]
    median_reference = eligible.median() if not eligible.empty else stats["qtd_vendida_media_historico"].median()
    if np.isnan(median_reference):
        median_reference = 0

    candidatos = stats[
        (stats["historico_meses_validos"] >= MIN_HIST_MONTHS)
        & (stats["qtd_vendida_media_historico"] >= median_reference)
        & (stats["queda_pct_qtd"] >= MIN_DROP_RATIO)
        & (stats["taxa_devolucao_media_recente"] <= 0.2)
    ].copy()

    candidatos.sort_values(
        ["potencial_score", "queda_pct_qtd", "qtd_vendida_media_historico"],
        ascending=[False, False, False],
        inplace=True,
    )

    selecionados = candidatos.head(rank_size).copy()

    if "cd_anuncio" in selecionados.columns:
        cd_produto_values = selecionados["cd_anuncio"].map(produto_map).fillna("")
        selecionados.insert(0, "cd_produto", cd_produto_values)
        if categoria_map is not None:
            categoria_values = selecionados["cd_anuncio"].map(categoria_map).fillna(categoria_default)
        else:
            categoria_values = pd.Series(categoria_default, index=selecionados.index)
        selecionados.insert(1, "categoria", categoria_values)

    if selecionados.empty:
        historico_focado = grouped.head(0)
    else:
        foco = selecionados["cd_anuncio"].unique()
        historico_focado = grouped[grouped["cd_anuncio"].isin(foco)].copy()

    if "cd_anuncio" in historico_focado.columns:
        if categoria_map is not None:
            historico_categoria = historico_focado["cd_anuncio"].map(categoria_map).fillna(categoria_default)
        else:
            historico_categoria = pd.Series(categoria_default, index=historico_focado.index)
        insert_pos = (
            historico_focado.columns.get_loc("cd_produto") + 1
            if "cd_produto" in historico_focado.columns
            else 0
        )
        historico_focado.insert(insert_pos, "categoria", historico_categoria)

    historico_focado["preco_medio_vendido"] = np.where(
        historico_focado["qtd_vendida"] > 0,
        historico_focado["receita"] / historico_focado["qtd_vendida"],
        0,
    ).round(2)

    selecionados["preco_min_intervalo"] = pd.to_numeric(
        selecionados["cd_anuncio"].map(interval_prices), errors="coerce"
    ).round(2)
    historico_focado["preco_min_intervalo"] = pd.to_numeric(
        historico_focado["cd_anuncio"].map(interval_prices), errors="coerce"
    ).round(2)
    if historical_prices:
        selecionados["preco_min_historico_total"] = pd.to_numeric(
            selecionados["cd_anuncio"].map(historical_prices), errors="coerce"
        ).round(2)
        historico_focado["preco_min_historico_total"] = pd.to_numeric(
            historico_focado["cd_anuncio"].map(historical_prices), errors="coerce"
        ).round(2)
    else:
        selecionados["preco_min_historico_total"] = np.nan
        historico_focado["preco_min_historico_total"] = np.nan

    selecionados_fmt = format_percentage_columns(
        selecionados,
        [
            "queda_pct_qtd",
            "taxa_devolucao_media_historico",
            "taxa_devolucao_media_recente",
            "margem_media_historico",
            "margem_media_recente",
        ],
    )
    historico_fmt = format_percentage_columns(historico_focado, ["margem_media"])

    return {
        "potenciais": selecionados_fmt,
        "skus_potenciais_mensal": historico_fmt,
    }


def _build_returns_totals_by_sale_period(
    returns_df: pd.DataFrame,
    allowed_periods: Set[str],
) -> pd.DataFrame:
    if returns_df is None or returns_df.empty or not allowed_periods:
        return pd.DataFrame(
            columns=[
                "periodo",
                "cd_produto",
                "qtd_devolvida_ret",
                "pedidos_devolvidos_ret",
                "receita_devolucao_ret",
            ]
        )

    prepared = returns_report._prepare_returns_dataset(returns_df)
    if prepared.empty:
        return pd.DataFrame(
            columns=[
                "periodo",
                "cd_produto",
                "qtd_devolvida_ret",
                "pedidos_devolvidos_ret",
                "receita_devolucao_ret",
            ]
        )

    period_series = ensure_period_series(
        prepared,
        "periodo_venda",
        "data_venda",
    )
    prepared = prepared.copy()
    prepared["periodo"] = period_series.astype(str)
    prepared = prepared[prepared["periodo"].isin(allowed_periods)].copy()
    if prepared.empty:
        return pd.DataFrame(
            columns=[
                "periodo",
                "cd_produto",
                "qtd_devolvida_ret",
                "pedidos_devolvidos_ret",
                "receita_devolucao_ret",
            ]
        )

    prepared["cd_produto"] = normalize_product_codes(
        prepared.get("cd_produto", ""),
        index=prepared.index,
    )

    totals = build_period_product_totals(
        prepared,
        period_column="periodo",
        product_column="cd_produto",
        allowed_periods=allowed_periods,
        units_column="qtd_sku",
        unit_result_name="qtd_devolvida_ret",
        include_order_count=True,
        order_result_name="pedidos_devolvidos_ret",
        extra_aggs={
            "devolucao_receita_bruta": ("receita_devolucao_ret", "sum"),
        },
    )

    return totals


def _aggregate_window(df: pd.DataFrame, periods: np.ndarray, suffix: str) -> pd.DataFrame:
    if len(periods) == 0:
        return pd.DataFrame(
            columns=[
                "cd_anuncio",
                "ds_anuncio",
                f"qtd_vendida_media_{suffix}",
                f"receita_media_{suffix}",
                f"pedidos_medios_{suffix}",
                f"taxa_devolucao_media_{suffix}",
                f"margem_media_{suffix}",
                f"preco_min_{suffix}",
                f"{suffix}_meses_validos",
            ]
        )

    filtered = df[df["periodo"].isin(periods)].copy()
    filtered["taxa_devolucao_mensal"] = np.where(
        filtered["qtd_vendida"] > 0,
        filtered["qtd_devolvida"] / filtered["qtd_vendida"],
        0,
    )
    aggregated = (
        filtered.groupby(["cd_anuncio", "ds_anuncio"], as_index=False)
        .agg(
            qtd_vendida_media=("qtd_vendida", "mean"),
            receita_media=("receita", "mean"),
            pedidos_medios=("pedidos", "mean"),
            taxa_devolucao_media=("taxa_devolucao_mensal", "mean"),
            margem_media=("margem_media", "mean"),
            preco_min=("preco_min_periodo", "min"),
            meses_validos=("periodo", "nunique"),
        )
    )

    aggregated = aggregated.rename(
        columns={
            "qtd_vendida_media": f"qtd_vendida_media_{suffix}",
            "receita_media": f"receita_media_{suffix}",
            "pedidos_medios": f"pedidos_medios_{suffix}",
            "taxa_devolucao_media": f"taxa_devolucao_media_{suffix}",
            "margem_media": f"margem_media_{suffix}",
            "preco_min": f"preco_min_{suffix}_janela",
            "meses_validos": f"{suffix}_meses_validos",
        }
    )
    aggregated[f"preco_min_{suffix}_janela"] = aggregated[
        f"preco_min_{suffix}_janela"
    ].round(2)
    return aggregated


def _filter_by_category(df: pd.DataFrame, category: Optional[str]) -> pd.DataFrame:
    if not category:
        filtered = df.copy()
    else:
        filtered = df[df["categoria"] == category].copy()
    filtered.attrs = dict(df.attrs)
    return filtered
