from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from ..formatting import format_percentage_columns


def build_product_focus_analysis(
    df: pd.DataFrame,
    category: Optional[str],
    product_codes: Optional[List[str]] = None,
) -> Dict[str, pd.DataFrame]:
    """Avalia o desempenho comercial filtrando por categoria ou lista especifica de SKUs."""

    data = _filter_by_category(df, category)
    normalized_codes = (
        [str(code).strip() for code in product_codes if str(code).strip()]
        if product_codes
        else []
    )
    focus = (
        data[data["cd_produto"].isin(normalized_codes)].copy()
        if normalized_codes
        else data.copy()
    )
    focus["data"] = pd.to_datetime(focus.get("data"), errors="coerce")
    focus = focus.dropna(subset=["data"])

    focus["data"] = focus["data"].dt.normalize()

    resumo = _aggregate_metrics(
        focus,
        group_cols=["cd_produto", "ds_produto", "cd_fabricante", "tp_anuncio"],
    )
    resumo.sort_values(["receita", "qtd_vendida"], ascending=[False, False], inplace=True)

    analise_diaria = _aggregate_metrics(
        focus,
        group_cols=["data", "cd_produto", "ds_produto", "cd_fabricante", "tp_anuncio"],
    )
    analise_diaria.sort_values(["data", "cd_produto"], inplace=True)
    if not analise_diaria.empty:
        analise_diaria["data"] = pd.to_datetime(analise_diaria["data"], errors="coerce").dt.strftime("%d/%m/%Y")

    analise_mensal = _aggregate_metrics(
        focus,
        group_cols=["periodo", "cd_produto", "ds_produto", "cd_fabricante", "tp_anuncio"],
    )
    analise_mensal.sort_values(["periodo", "cd_produto"], inplace=True)
    analise_mensal["periodo"] = analise_mensal["periodo"].astype(str)

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

    aggregated = (
        working.groupby(group_cols, as_index=False)
        .agg(
            pedidos=("nr_nota_fiscal", "nunique"),
            qtd_vendida=("qtd_sku", "sum"),
            receita=("rob", "sum"),
            custo_total=("custo_total", "sum"),
            margem_media=("perc_margem_bruta", "mean"),
            qtd_devolvida=("qtd_devolvido", "sum"),
            receita_devolucao=("devolucao_receita_bruta", "sum"),
            lucro_bruto_estimado=("lucro_bruto_estimado", "sum"),
            preco_medio_praticado=("preco_vendido", "mean"),
            preco_min_periodo=("preco_vendido", "min"),
        )
    )

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
        return df.copy()
    return df[df["categoria"] == category].copy()
