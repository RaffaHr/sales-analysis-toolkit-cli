from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from ..formatting import format_percentage_columns

MIN_MONTHLY_QUANTITY = 0
MIN_RETURN_RATE = 0.2

MONTH_NAMES_PT = {
    1: ("Janeiro", "Jan"),
    2: ("Fevereiro", "Fev"),
    3: ("Marco", "Mar"),
    4: ("Abril", "Abr"),
    5: ("Maio", "Mai"),
    6: ("Junho", "Jun"),
    7: ("Julho", "Jul"),
    8: ("Agosto", "Ago"),
    9: ("Setembro", "Set"),
    10: ("Outubro", "Out"),
    11: ("Novembro", "Nov"),
    12: ("Dezembro", "Dez"),
}


def build_return_analysis(
    df: pd.DataFrame,
    category: Optional[str],
    min_return_rate: float = MIN_RETURN_RATE,
    min_monthly_qty: int = MIN_MONTHLY_QUANTITY,
) -> Dict[str, pd.DataFrame]:
    """Avalia produtos com devolução acima do limite definido."""
    data = _filter_by_category(df, category)

    grouped = (
        data.groupby(["periodo", "cd_produto", "ds_produto"], as_index=False)
        .agg(
            qtd_vendida=("qtd_sku", "sum"),
            pedidos=("nr_nota_fiscal", "nunique"),
            qtd_devolvida=("qtd_devolvido", "sum"),
            receita=("rob", "sum"),
            receita_devolucao=("devolucao_receita_bruta", "sum"),
        )
    )

    grouped["taxa_devolucao"] = np.where(
        grouped["qtd_vendida"] > 0,
        grouped["qtd_devolvida"] / grouped["qtd_vendida"],
        0,
    )
    critical = grouped[
        (grouped["taxa_devolucao"] >= min_return_rate)
        & (grouped["qtd_vendida"] >= min_monthly_qty)
    ].copy()
    if not critical.empty and "periodo" in critical.columns:
        critical["periodo"] = critical["periodo"].astype(str)
    critical.sort_values([
        "taxa_devolucao",
        "qtd_vendida",
    ], ascending=[False, False], inplace=True)

    summary = (
        data.groupby(["cd_produto", "ds_produto"], as_index=False)
        .agg(
            meses_com_venda=("periodo", "nunique"),
            qtd_vendida_total=("qtd_sku", "sum"),
            qtd_devolvida_total=("qtd_devolvido", "sum"),
            receita_total=("rob", "sum"),
            receita_devolucao_total=("devolucao_receita_bruta", "sum"),
            pedidos_total=("nr_nota_fiscal", "nunique"),
        )
    )
    summary["taxa_devolucao_total"] = np.where(
        summary["qtd_vendida_total"] > 0,
        summary["qtd_devolvida_total"] / summary["qtd_vendida_total"],
        0,
    )
    summary.sort_values(
        ["taxa_devolucao_total", "qtd_vendida_total"],
        ascending=[False, False],
        inplace=True,
    )

    overview = (
        critical.groupby(["periodo", "cd_produto", "ds_produto"], as_index=False)
        .agg(
            produtos_afetados=("cd_produto", "size"),
            total_devolvido=("qtd_devolvida", "sum"),
            total_vendido=("qtd_vendida", "sum"),
            pedidos_totais=("pedidos", "sum"),
        )
    )
    if not overview.empty and "periodo" in overview.columns:
        period_dt = pd.to_datetime(overview["periodo"], errors="coerce")
        overview["ano"] = period_dt.dt.year.astype("Int64")
        overview["mes_extenso"] = period_dt.dt.month.map(lambda m: MONTH_NAMES_PT.get(m, ("", ""))[0])
        overview["mes_abreviado"] = period_dt.dt.month.map(lambda m: MONTH_NAMES_PT.get(m, ("", ""))[1])
        missing_mask = period_dt.isna()
        if missing_mask.any():
            overview.loc[missing_mask, ["ano", "mes_extenso", "mes_abreviado"]] = pd.NA
        overview["periodo"] = overview["periodo"].astype(str)
    overview["taxa_devolucao_media"] = np.where(
        overview["total_vendido"] > 0,
        overview["total_devolvido"] / overview["total_vendido"],
        0,
    )
    if not overview.empty:
        ordered_columns = [
            "periodo",
            "ano",
            "mes_extenso",
            "mes_abreviado",
            "cd_produto",
            "ds_produto",
            "produtos_afetados",
            "total_devolvido",
            "total_vendido",
            "pedidos_totais",
            "taxa_devolucao_media",
        ]
        existing = [column for column in ordered_columns if column in overview.columns]
        remainder = [column for column in overview.columns if column not in existing]
        overview = overview[existing + remainder]
    resumo_produto = format_percentage_columns(summary, ["taxa_devolucao_total"])
    picos_por_mes = format_percentage_columns(critical, ["taxa_devolucao"])
    visao_mensal = format_percentage_columns(overview, ["taxa_devolucao_media"])

    return {
        "resumo_produto": resumo_produto,
        "picos_por_mes": picos_por_mes,
        "visao_mensal": visao_mensal,
    }


def _filter_by_category(df: pd.DataFrame, category: Optional[str]) -> pd.DataFrame:
    if not category:
        return df.copy()
    return df[df["categoria"] == category].copy()
