from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from ..formatting import format_percentage_columns

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
    """Identifica SKUs com queda recente, mas histórico forte de vendas."""
    data = _filter_by_category(df, category)
    interval_prices = (
        data.groupby("cd_produto")["preco_vendido"].min()
        .replace([np.inf, -np.inf], np.nan)
    )
    grouped = (
        data.groupby(["periodo", "cd_produto", "ds_produto"], as_index=False)
        .agg(
            qtd_vendida=("qtd_sku", "sum"),
            pedidos=("nr_nota_fiscal", "nunique"),
            receita=("rob", "sum"),
            custo=("custo_total", "sum"),
            margem_media=("perc_margem_bruta", "mean"),
            qtd_devolvida=("qtd_devolvido", "sum"),
            preco_min_periodo=("preco_vendido", "min"),
        )
    )

    if grouped.empty:
        return {"potenciais": grouped, "hist_mensal": grouped}

    grouped["periodo"] = grouped["periodo"].astype(str)
    grouped.sort_values("periodo", inplace=True)
    grouped["preco_min_periodo"] = grouped["preco_min_periodo"].round(2)
    available_periods = grouped["periodo"].unique()

    if recent_periods:
        selected = sorted({str(p) for p in recent_periods})
        valid_selected = [p for p in selected if p in available_periods]
        if not valid_selected:
            return {"potenciais": grouped.head(0), "hist_mensal": grouped.head(0)}
        historical_periods = [p for p in available_periods if p not in valid_selected]
        if not historical_periods:
            return {"potenciais": grouped.head(0), "hist_mensal": grouped.head(0)}
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
        on=["cd_produto", "ds_produto"],
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

    if selecionados.empty:
        historico_focado = grouped.head(0)
    else:
        foco = selecionados["cd_produto"].unique()
        historico_focado = grouped[grouped["cd_produto"].isin(foco)].copy()

    historico_focado["preco_medio_vendido"] = np.where(
        historico_focado["qtd_vendida"] > 0,
        historico_focado["receita"] / historico_focado["qtd_vendida"],
        0,
    ).round(2)

    selecionados["preco_min_intervalo"] = pd.to_numeric(
        selecionados["cd_produto"].map(interval_prices), errors="coerce"
    ).round(2)
    historico_focado["preco_min_intervalo"] = pd.to_numeric(
        historico_focado["cd_produto"].map(interval_prices), errors="coerce"
    ).round(2)
    if historical_prices:
        selecionados["preco_min_historico_total"] = pd.to_numeric(
            selecionados["cd_produto"].map(historical_prices), errors="coerce"
        ).round(2)
        historico_focado["preco_min_historico_total"] = pd.to_numeric(
            historico_focado["cd_produto"].map(historical_prices), errors="coerce"
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
        "hist_mensal": historico_fmt,
    }


def _aggregate_window(df: pd.DataFrame, periods: np.ndarray, suffix: str) -> pd.DataFrame:
    if len(periods) == 0:
        return pd.DataFrame(
            columns=[
                "cd_produto",
                "ds_produto",
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
        filtered.groupby(["cd_produto", "ds_produto"], as_index=False)
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
        return df.copy()
    return df[df["categoria"] == category].copy()
