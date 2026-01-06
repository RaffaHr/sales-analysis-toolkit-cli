from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from ..formatting import format_percentage_columns

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

    interval_prices = (
        data.groupby("cd_produto")["preco_vendido"].min()
        .replace([np.inf, -np.inf], np.nan)
    )

    aggregated = (
        data.groupby(["cd_produto", "ds_produto"], as_index=False)
        .agg(
            quantidade_total=("qtd_sku", "sum"),
            pedidos_total=("nr_nota_fiscal", "nunique"),
            receita_total=("rob", "sum"),
            custo_medio_unitario=("custo_produto", "mean"),
            custo_total=("custo_total", "sum"),
            devolucao_total=("qtd_devolvido", "sum"),
            receita_devolucao_total=("devolucao_receita_bruta", "sum"),
            margem_media=("perc_margem_bruta", "mean"),
        )
    )

    if aggregated.empty:
        return {"produtos_indicados": aggregated}

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
        selecionados["cd_produto"].map(interval_prices), errors="coerce"
    ).round(2)
    if historical_prices:
        selecionados["preco_min_historico_total"] = pd.to_numeric(
            selecionados["cd_produto"].map(historical_prices), errors="coerce"
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
        return df.copy()
    return df[df["categoria"] == category].copy()
