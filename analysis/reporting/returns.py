from __future__ import annotations

from typing import Dict, Optional, Set

import numpy as np
import pandas as pd

from ..formatting import format_percentage_columns

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

RESULT_COLUMNS = [
    "ano",
    "mes_extenso",
    "mes_abreviado",
    "periodo",
    "cd_produto",
    "ds_produto",
    "itens_vendidos",
    "itens_devolvidos",
    "pedidos_devolvidos",
    "receita_devolucao",
    "taxa_devolucao",
]


def _normalize_product_codes(series: object, index: Optional[pd.Index] = None) -> pd.Series:
    """Normaliza códigos numéricos para evitar mapeamento por anúncio."""
    if series is None:
        return pd.Series([], dtype=str, index=index)
    if isinstance(series, pd.Series):
        base_series = series
    else:
        base_series = pd.Series(series, index=index)

    def _normalize(value: object) -> str:
        if pd.isna(value):
            return ""
        if isinstance(value, (np.integer, int)):
            return str(int(value))
        if isinstance(value, (np.floating, float)):
            if not np.isfinite(value):
                return ""
            if float(value).is_integer():
                return str(int(value))
            value = f"{value:f}"
        text = str(value).strip()
        lowered = text.lower()
        if lowered in {"", "nan", "none", "null"}:
            return ""
        if "." in text:
            integer, fractional = text.split(".", 1)
            if fractional.strip("0") == "":
                return integer
        return text

    normalized = base_series.apply(_normalize).astype(str)
    return normalized


def build_return_analysis(
    df: pd.DataFrame,
    category: Optional[str],
) -> Dict[str, pd.DataFrame]:
    """Entrega visões mensais de devolução usando venda e data do retorno."""
    # df aqui é a aba de VENDAS (base principal)
    sales_base = _filter_by_category(df, category)
    returns_raw = df.attrs.get("returns_data", pd.DataFrame())
    returns_filtered = _filter_returns_dataset(returns_raw, category)

    # pré-aggregate sales totals (por periodo e cd_produto) usando a coluna de unidades detectada
    sales_totals = _build_sales_totals(sales_base)
    available_periods: Set[str] = set(sales_totals["periodo"].unique()) if not sales_totals.empty else set()
    prepared_returns = _prepare_returns_dataset(returns_filtered)

    tied_to_sale = _build_return_view(
        prepared_returns,
        period_column="periodo_venda",
        sales_totals=sales_totals,
        sales_base=sales_base,
        period_filter=available_periods,
    )
    tied_to_return = _build_return_view(
        prepared_returns,
        period_column="periodo_devolucao",
        sales_totals=sales_totals,
        sales_base=sales_base,
        period_filter=available_periods or None,
    )

    return {
        "Dev. atrelada ao mês da venda": format_percentage_columns(
            tied_to_sale, ["taxa_devolucao"]
        ),
        "Analise de Dev. mensal": format_percentage_columns(
            tied_to_return, ["taxa_devolucao"]
        ),
    }


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


def _detect_units_column(df: pd.DataFrame) -> str:
    # procura pela coluna de unidades mais provável
    candidates = ["unidades", "qtd_sku", "qtd", "quantidade", "unid"]
    for c in candidates:
        if c in df.columns:
            return c
    # fallback: qualquer coluna numérica que pareça quantidade — mas preferimos exigir uma coluna
    # se não houver coluna explícita, voltamos pra 'qtd_sku' e deixamos zeros
    return "qtd_sku"


def _build_sales_totals(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["periodo", "cd_produto", "itens_vendidos"])

    working = df.copy()
    # garantir periodo no formato YYYY-MM (string)
    period_series = working.get("periodo")
    if period_series is None:
        date_series = pd.to_datetime(working.get("data"), dayfirst=True, errors="coerce")
        period_series = date_series.dt.to_period("M")
    if isinstance(period_series.dtype, pd.PeriodDtype):
        working["periodo"] = period_series.astype(str)
    else:
        working["periodo"] = pd.to_datetime(period_series, errors="coerce").dt.to_period("M").astype(str)

    if "cd_produto" in working.columns:
        working["cd_produto"] = _normalize_product_codes(working["cd_produto"])
    else:
        working["cd_produto"] = _normalize_product_codes("", index=working.index)

    units_col = _detect_units_column(working)
    if units_col not in working.columns:
        # garante coluna com zeros
        working["qtd_sku"] = 0.0
        units_col = "qtd_sku"

    working["__units__"] = pd.to_numeric(working.get(units_col, 0), errors="coerce").fillna(0.0)

    grouped = (
        working.groupby(["periodo", "cd_produto"], as_index=False)
        .agg(itens_vendidos=("__units__", "sum"))
    )
    grouped["itens_vendidos"] = grouped["itens_vendidos"].fillna(0.0)
    return grouped


def _prepare_returns_dataset(
    returns_df: pd.DataFrame,
) -> pd.DataFrame:
    if returns_df is None or returns_df.empty:
        return pd.DataFrame()

    working = returns_df.copy()
    # normaliza colunas esperadas
    working["qtd_sku"] = working.get("qtd_sku", 0).fillna(0.0)
    working["devolucao_receita_bruta"] = working.get("devolucao_receita_bruta", 0).fillna(0.0)
    if "cd_produto" in working.columns:
        working["cd_produto"] = _normalize_product_codes(working["cd_produto"])
    else:
        working["cd_produto"] = _normalize_product_codes("", index=working.index)
    working["ds_produto"] = working.get("ds_produto", "").astype(str).str.strip()
    working["nr_nota_fiscal"] = working.get("nr_nota_fiscal", "").astype(str).str.strip()

    working["periodo_venda"] = _ensure_period_series(working, "periodo_venda", "data_venda")
    working["periodo_devolucao"] = _ensure_period_series(working, "periodo_devolucao", "data_devolucao")

    nota_devolucao = working.get("nr_nota_devolucao")
    nota_venda = working.get("nr_nota_fiscal")
    devolucao_id = (
        nota_devolucao.astype(str).str.strip() if nota_devolucao is not None else pd.Series("", index=working.index)
    )
    devolucao_id = devolucao_id.replace({"nan": "", "None": ""})
    fallback = nota_venda.astype(str).str.strip() if nota_venda is not None else ""
    working["pedido_devolucao_id"] = np.where(
        devolucao_id != "",
        devolucao_id,
        fallback,
    )
    working["pedido_devolucao_id"] = working["pedido_devolucao_id"].fillna("").astype(str).str.strip()

    return working


def _ensure_period_series(df: pd.DataFrame, period_column: str, date_column: str) -> pd.Series:
    if period_column in df.columns:
        series = df[period_column]
        if isinstance(series.dtype, pd.PeriodDtype):
            return series
        if pd.api.types.is_datetime64_any_dtype(series):
            return series.dt.to_period("M")
        converted = pd.to_datetime(series, dayfirst=True, errors="coerce")
        return converted.dt.to_period("M")

    if date_column in df.columns:
        converted = pd.to_datetime(df[date_column], dayfirst=True, errors="coerce")
        return converted.dt.to_period("M")

    empty = pd.Series(pd.PeriodIndex([pd.NaT] * len(df), freq="M"), index=df.index)
    return empty


def _build_return_view(
    returns_df: pd.DataFrame,
    *,
    period_column: str,
    sales_totals: pd.DataFrame,
    sales_base: pd.DataFrame,
    period_filter: Optional[Set[str]] = None,
) -> pd.DataFrame:
    """
    Monta visão de devoluções.
    itens_vendidos será obtido a partir do 'sales_base' (aba VENDAS) agrupada por periodo e cd_produto,
    garantindo que o vínculo é por CD_PRODUTO e PERÍODO — NUNCA por nota fiscal.
    """
    if returns_df is None or returns_df.empty:
        return _empty_result()
    if period_column not in returns_df.columns:
        return _empty_result()

    working = returns_df.dropna(subset=[period_column]).copy()
    working = working[working[period_column].notna()]
    if working.empty:
        return _empty_result()

    # usar o periodo solicitado (venda ou devolução)
    working["periodo"] = working[period_column].astype(str)
    if period_filter:
        working = working[working["periodo"].isin(period_filter)].copy()
        if working.empty:
            return _empty_result()

    # agregar devoluções por periodo + cd_produto (+ ds_produto quando disponível)
    group_keys = ["periodo", "cd_produto", "ds_produto"]

    devolucoes = (
        working.groupby(group_keys, as_index=False)
        .agg(
            itens_devolvidos=("qtd_sku", "sum"),
            receita_devolucao=("devolucao_receita_bruta", "sum"),
            pedidos_devolvidos=("pedido_devolucao_id", "nunique"),
        )
    )

    # garantir sales_totals pronto para merge; se estiver vazio, recalcular a partir do sales_base
    sales_lookup = sales_totals.copy() if sales_totals is not None else pd.DataFrame()
    if sales_lookup.empty and sales_base is not None and not sales_base.empty:
        sales_lookup = _build_sales_totals(sales_base)
    if not sales_lookup.empty:
        sales_lookup["periodo"] = sales_lookup["periodo"].astype(str)
        if "cd_produto" in sales_lookup.columns:
            sales_lookup["cd_produto"] = _normalize_product_codes(sales_lookup["cd_produto"])

    # merge inicial para trazer 'itens_vendidos'
    result = devolucoes.merge(
        sales_lookup,
        on=["periodo", "cd_produto"],
        how="left",
    )

    # Se houver linhas sem itens_vendidos (merge nao encontrou vendas), recalcular diretamente filtrando sales_base.
    if ("itens_vendidos" not in result.columns) or result["itens_vendidos"].isna().any():
        if sales_base is None or sales_base.empty:
            # sem base de vendas: preencher zeros
            result["itens_vendidos"] = result.get("itens_vendidos", pd.Series(0.0, index=result.index)).fillna(0.0)
        else:
            # cria mapa (periodo, cd_produto) -> soma unidades
            direct_totals = _build_sales_totals(sales_base)
            direct_totals["periodo"] = direct_totals["periodo"].astype(str)
            if "cd_produto" in direct_totals.columns:
                direct_totals["cd_produto"] = _normalize_product_codes(direct_totals["cd_produto"])
            # merge direto com direct_totals (garante cover)
            result = result.merge(
                direct_totals,
                on=["periodo", "cd_produto"],
                how="left",
                suffixes=("", "_from_sales"),
            )
            # preferir coluna 'itens_vendidos' original, se existir; senão, usar a vinda de sales
            if "itens_vendidos" in result.columns and "itens_vendidos_from_sales" in result.columns:
                # combinar: se itens_vendidos estiver nulo, pegar from_sales
                result["itens_vendidos"] = np.where(
                    result["itens_vendidos"].notna(),
                    result["itens_vendidos"],
                    result["itens_vendidos_from_sales"],
                )
                result.drop(columns=["itens_vendidos_from_sales"], inplace=True, errors="ignore")
            elif "itens_vendidos_from_sales" in result.columns:
                result["itens_vendidos"] = result["itens_vendidos_from_sales"]
                result.drop(columns=["itens_vendidos_from_sales"], inplace=True, errors="ignore")
            else:
                result["itens_vendidos"] = 0.0

    # preencher NAs e tipos
    result["itens_vendidos"] = pd.to_numeric(result["itens_vendidos"], errors="coerce").fillna(0.0)
    result["itens_devolvidos"] = result["itens_devolvidos"].fillna(0.0)
    result["receita_devolucao"] = result["receita_devolucao"].fillna(0.0)
    result["pedidos_devolvidos"] = result["pedidos_devolvidos"].fillna(0).astype(int)

    result["taxa_devolucao"] = np.where(
        result["itens_vendidos"] > 0,
        result["itens_devolvidos"] / result["itens_vendidos"],
        0.0,
    )

    result["itens_devolvidos"] = result["itens_devolvidos"].round(2)
    result["itens_vendidos"] = result["itens_vendidos"].round(2)
    result["receita_devolucao"] = result["receita_devolucao"].round(2)

    result = _append_month_labels(result)
    result.sort_values(["periodo", "cd_produto"], inplace=True)
    result.reset_index(drop=True, inplace=True)

    # garantir colunas na ordem esperada
    for c in RESULT_COLUMNS:
        if c not in result.columns:
            result[c] = 0 if c in ("itens_vendidos", "itens_devolvidos", "pedidos_devolvidos", "receita_devolucao", "taxa_devolucao") else ""

    return result[RESULT_COLUMNS]


def _append_month_labels(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    try:
        period_index = pd.PeriodIndex(df["periodo"], freq="M")
    except Exception:
        period_index = pd.PeriodIndex(pd.to_datetime(df["periodo"], errors="coerce").to_period("M"))

    df["ano"] = period_index.year
    month_numbers = period_index.month
    df["mes_extenso"] = [MONTH_NAMES.get(m, ("", ""))[0] for m in month_numbers]
    df["mes_abreviado"] = [MONTH_NAMES.get(m, ("", ""))[1] for m in month_numbers]
    return df


def _empty_result() -> pd.DataFrame:
    return pd.DataFrame(columns=RESULT_COLUMNS)
