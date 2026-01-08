from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from ..formatting import format_percentage_columns

MIN_MONTHLY_QUANTITY = 0
MIN_RETURN_RATE = 0.2

MONTH_NAMES = {
    1: ("Janeiro", "Jan"),
    2: ("Fevereiro", "Fev"),
    3: ("Março", "Mar"),
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
    returns_detail = df.attrs.get("returns_data", pd.DataFrame())
    returns_filtered = _filter_returns_dataset(returns_detail, category)
    data = _filter_by_category(df, category)

    grouped = (
        data.groupby(["periodo", "cd_produto", "ds_produto"], as_index=False)
        .agg(
            qtd_vendida=("qtd_sku", "sum"),
            pedidos=("nr_nota_fiscal", "nunique"),
            qtd_devolvida=("qtd_devolvido", "sum"),
            receita=("rbld", "sum"),
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
            receita_total=("rbld", "sum"),
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
            total_devolvido=("qtd_devolvida", "sum"),
            total_vendido=("qtd_vendida", "sum"),
            pedidos_totais=("pedidos", "sum"),
        )
    )
    overview["taxa_devolucao_media"] = np.where(
        overview["total_vendido"] > 0,
        overview["total_devolvido"] / overview["total_vendido"],
        0,
    )
    if not overview.empty and "periodo" in overview.columns:
        meses = overview["periodo"].dt.month
        overview["ano"] = overview["periodo"].dt.year
        overview["mes_extenso"] = meses.map(lambda m: MONTH_NAMES.get(m, ("", ""))[0])
        overview["mes_abreviado"] = meses.map(lambda m: MONTH_NAMES.get(m, ("", ""))[1])
        overview["periodo"] = overview["periodo"].astype(str)
        cols_order = [
            "ano",
            "mes_extenso",
            "mes_abreviado",
            "periodo",
            "cd_produto",
            "ds_produto",
            "total_devolvido",
            "total_vendido",
            "pedidos_totais",
            "taxa_devolucao_media",
        ]
        overview = overview[cols_order]
    resumo_produto = format_percentage_columns(summary, ["taxa_devolucao_total"])
    picos_por_mes = format_percentage_columns(critical, ["taxa_devolucao"])
    if "periodo" in picos_por_mes.columns:
        picos_por_mes["periodo"] = picos_por_mes["periodo"].astype(str)
    visao_mensal = format_percentage_columns(overview, ["taxa_devolucao_media"])

    devolucoes_por_devolucao = _build_returns_by_return_month(returns_filtered)

    result: Dict[str, pd.DataFrame] = {
        "resumo_produto": resumo_produto,
        "picos_por_mes": picos_por_mes,
        "visao_mensal": visao_mensal,
    }
    if not devolucoes_por_devolucao.empty:
        result["devolucoes_por_mes_devolucao"] = devolucoes_por_devolucao
    return result


def _filter_by_category(df: pd.DataFrame, category: Optional[str]) -> pd.DataFrame:
    if not category:
        filtered = df.copy()
    else:
        filtered = df[df["categoria"] == category].copy()
    filtered.attrs = dict(df.attrs)
    return filtered


def _filter_returns_dataset(returns_df: pd.DataFrame, category: Optional[str]) -> pd.DataFrame:
    if returns_df is None or returns_df.empty:
        return pd.DataFrame()
    if not category:
        return returns_df.copy()
    return returns_df[returns_df["categoria"] == category].copy()


def _build_returns_by_return_month(returns_df: pd.DataFrame) -> pd.DataFrame:
    if returns_df is None or returns_df.empty:
        return pd.DataFrame(columns=[
            "ano_devolucao",
            "mes_extenso",
            "mes_abreviado",
            "periodo_devolucao",
            "cd_produto",
            "ds_produto",
            "unidades_devolvidas",
            "receita_devolvida",
            "ticket_medio_devolucao",
            "pedidos_impactados",
            "dias_medio_para_devolucao",
        ])

    working = returns_df.copy()
    working.dropna(subset=["data_devolucao"], inplace=True)
    if working.empty:
        return working.head(0)

    working["data_venda"] = pd.to_datetime(
        working["data_venda"], dayfirst=True, errors="coerce"
    ).dt.normalize()
    working["data_devolucao"] = pd.to_datetime(
        working["data_devolucao"], dayfirst=True, errors="coerce"
    ).dt.normalize()
    working.dropna(subset=["data_devolucao"], inplace=True)
    if working.empty:
        return working.head(0)

    working["periodo_devolucao"] = working["periodo_devolucao"].astype(str)
    working["ano_devolucao"] = working["data_devolucao"].dt.year
    working["mes_num"] = working["data_devolucao"].dt.month
    working["mes_extenso"] = working["mes_num"].map(lambda m: MONTH_NAMES.get(m, ("", ""))[0])
    working["mes_abreviado"] = working["mes_num"].map(lambda m: MONTH_NAMES.get(m, ("", ""))[1])
    working.drop(columns=["mes_num"], inplace=True)
    working["dias_para_devolucao"] = (working["data_devolucao"] - working["data_venda"]).dt.days

    grouped = (
        working.groupby(
            [
                "ano_devolucao",
                "mes_extenso",
                "mes_abreviado",
                "periodo_devolucao",
                "cd_produto",
                "ds_produto",
            ],
            as_index=False,
        )
        .agg(
            unidades_devolvidas=("qtd_sku", "sum"),
            receita_devolvida=("devolucao_receita_bruta", "sum"),
            pedidos_impactados=("nr_nota_fiscal", "nunique"),
            dias_medio_para_devolucao=("dias_para_devolucao", "mean"),
        )
    )

    if grouped.empty:
        return grouped

    grouped["ticket_medio_devolucao"] = np.where(
        grouped["unidades_devolvidas"] > 0,
        grouped["receita_devolvida"] / grouped["unidades_devolvidas"],
        0,
    )
    grouped["unidades_devolvidas"] = grouped["unidades_devolvidas"].fillna(0).round(2)
    grouped["receita_devolvida"] = grouped["receita_devolvida"].fillna(0).round(2)
    grouped["ticket_medio_devolucao"] = grouped["ticket_medio_devolucao"].round(2)
    grouped["dias_medio_para_devolucao"] = grouped["dias_medio_para_devolucao"].round(1)
    grouped["pedidos_impactados"] = grouped["pedidos_impactados"].fillna(0).astype(int)
    grouped.sort_values(
        ["ano_devolucao", "periodo_devolucao", "cd_produto"],
        inplace=True,
    )

    return grouped[[
        "ano_devolucao",
        "mes_extenso",
        "mes_abreviado",
        "periodo_devolucao",
        "cd_produto",
        "ds_produto",
        "unidades_devolvidas",
        "receita_devolvida",
        "ticket_medio_devolucao",
        "pedidos_impactados",
        "dias_medio_para_devolucao",
    ]]
