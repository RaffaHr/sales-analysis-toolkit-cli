from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from ..formatting import format_percentage_columns

MIN_MONTHS_RECURRENCE = 3


def build_top_history_analysis(
    df: pd.DataFrame,
    category: Optional[str],
    rank_size: int = 20,
    historical_prices: Optional[Dict[str, float]] = None,
) -> Dict[str, pd.DataFrame]:
    """Gera ranking dos SKUs com melhor consistência histórica."""
    data = _filter_by_category(df, category)

    interval_prices = (
        data.groupby("cd_produto")["preco_vendido"].min()
        .replace([np.inf, -np.inf], np.nan)
    )

    monthly = (
        data.groupby(["periodo", "cd_produto", "ds_produto"], as_index=False)
        .agg(
            qtd_vendida=("qtd_sku", "sum"),
            pedidos=("nr_nota_fiscal", "nunique"),
            receita=("rob", "sum"),
            devolucao=("qtd_devolvido", "sum"),
            margem=("perc_margem_bruta", "mean"),
            preco_min_periodo=("preco_vendido", "min"),
        )
    )
    monthly["periodo"] = monthly["periodo"].astype(str)
    monthly["preco_medio_vendido"] = np.where(
        monthly["qtd_vendida"] > 0,
        monthly["receita"] / monthly["qtd_vendida"],
        0,
    ).round(2)
    monthly["preco_min_periodo"] = monthly["preco_min_periodo"].round(2)

    summary = (
        data.groupby(["cd_produto", "ds_produto"], as_index=False)
        .agg(
            meses_com_venda=("periodo", "nunique"),
            quantidade_total=("qtd_sku", "sum"),
            pedidos_total=("nr_nota_fiscal", "nunique"),
            receita_total=("rob", "sum"),
            devolucao_total=("qtd_devolvido", "sum"),
            perc_margem_media_rbld=("perc_margem_bruta", "mean"),
        )
    )
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
        ranking["cd_produto"].map(interval_prices), errors="coerce"
    ).round(2)
    if historical_prices:
        ranking["preco_min_historico_total"] = pd.to_numeric(
            ranking["cd_produto"].map(historical_prices), errors="coerce"
        ).round(2)
    else:
        ranking["preco_min_historico_total"] = np.nan

    detalhes = monthly[monthly["cd_produto"].isin(ranking["cd_produto"])].copy()
    detalhes["preco_min_historico_total"] = pd.to_numeric(
        detalhes["cd_produto"].map(historical_prices), errors="coerce"
    ).round(2) if historical_prices else np.nan
    detalhes["preco_min_intervalo"] = pd.to_numeric(
        detalhes["cd_produto"].map(interval_prices), errors="coerce"
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


def _filter_by_category(df: pd.DataFrame, category: Optional[str]) -> pd.DataFrame:
    if not category:
        return df.copy()
    return df[df["categoria"] == category].copy()
