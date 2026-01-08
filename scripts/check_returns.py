from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.data_loader import load_sales_dataset
from analysis.reporting import returns as returns_module



def _log_dataframe(label: str, df: pd.DataFrame, *, limit: int = 5, columns: list[str] | None = None) -> None:
    print(f"\n--- {label} (linhas={len(df)}) ---")
    if df.empty:
        print("(vazio)")
        return
    try:
        data = df if columns is None else df.loc[:, [c for c in columns if c in df.columns]]
    except KeyError:
        data = df
    print(data.head(limit).to_string(index=False))


def _parse_date(value: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Data inválida: {value}")
    return parsed.normalize()


def _filter_period(
    df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    debug: bool = False,
) -> pd.DataFrame:
    data_series = pd.to_datetime(df.get("data"), dayfirst=True, errors="coerce").dt.normalize()
    mask = data_series.between(start, end)
    filtered = df.loc[mask].copy()
    filtered.attrs = dict(df.attrs)
    if debug:
        _log_dataframe(
            "Sales após filtro por período",
            filtered,
            columns=["data", "periodo", "cd_produto", "cd_anuncio", "qtd_sku", "categoria"],
        )
    return filtered


def _summarize_sales(
    df: pd.DataFrame,
    product_code: str,
    *,
    debug: bool = False,
) -> tuple[int, float]:
    if df.empty:
        return 0, 0.0
    sales = df.copy()
    sales["cd_produto_norm"] = returns_module._normalize_product_codes(
        sales.get("cd_produto", ""), index=sales.index
    )
    if debug:
        _log_dataframe(
            "Vendas com colunas relevantes",
            sales,
            columns=["data", "periodo", "cd_produto", "cd_produto_norm", "cd_anuncio", "qtd_sku", "nr_pedido"],
        )

    product_sales = sales[sales["cd_produto_norm"] == product_code]
    if debug:
        _log_dataframe(
            f"Vendas filtradas para produto={product_code}",
            product_sales,
            columns=["data", "periodo", "cd_produto", "cd_anuncio", "qtd_sku", "nr_pedido"],
        )
    if product_sales.empty:
        return 0, 0.0
    units_column = returns_module._detect_units_column(product_sales)
    units = pd.to_numeric(product_sales.get(units_column, 0), errors="coerce").fillna(0.0)
    pedidos = product_sales.get("nr_pedido")
    pedidos_count = int(pd.Series(pedidos).nunique()) if pedidos is not None else len(product_sales)
    return pedidos_count, float(units.sum())


def _summarize_returns(
    returns_df: pd.DataFrame,
    product_code: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    debug: bool = False,
) -> tuple[int, float]:
    prepared = returns_module._prepare_returns_dataset(returns_df)
    if prepared.empty:
        return 0, 0.0
    prepared["data_devolucao"] = pd.to_datetime(prepared.get("data_devolucao"), errors="coerce").dt.normalize()
    if debug:
        _log_dataframe(
            "Base de devoluções preparada",
            prepared,
            columns=[
                "data_venda",
                "data_devolucao",
                "periodo_venda",
                "periodo_devolucao",
                "cd_produto",
                "cd_anuncio",
                "qtd_sku",
            ],
        )
    filtered = prepared[
        (prepared["cd_produto"] == product_code)
        & prepared["data_devolucao"].between(start, end)
    ].copy()
    if debug:
        _log_dataframe(
            f"Devoluções filtradas para produto={product_code}",
            filtered,
            columns=[
                "data_venda",
                "data_devolucao",
                "periodo_venda",
                "periodo_devolucao",
                "cd_produto",
                "cd_anuncio",
                "qtd_sku",
                "pedido_devolucao_id",
            ],
        )
    if filtered.empty:
        return 0, 0.0
    pedidos = int(filtered["pedido_devolucao_id"].nunique())
    unidades = float(filtered["qtd_sku"].sum())
    return pedidos, unidades


def main() -> None:
    parser = argparse.ArgumentParser(description="Checa totais de venda e devolução por produto/periodo.")
    parser.add_argument("dataset", type=Path, help="Caminho para o arquivo BASE.xlsx")
    parser.add_argument("--produto", required=True, help="Código CD_PRODUTO a inspecionar")
    parser.add_argument("--inicio", required=True, help="Data inicial (dd/mm/aaaa ou aaaa-mm-dd)")
    parser.add_argument("--fim", required=True, help="Data final (dd/mm/aaaa ou aaaa-mm-dd)")
    parser.add_argument("--debug", action="store_true", help="Exibe amostras de cada etapa de filtragem")
    args = parser.parse_args()

    start = _parse_date(args.inicio)
    end = _parse_date(args.fim)
    if start > end:
        raise ValueError("Data inicial maior que a final")

    df_full = load_sales_dataset(args.dataset)
    if args.debug:
        _log_dataframe(
            "Base completa carregada",
            df_full,
            columns=["data", "periodo", "cd_produto", "cd_anuncio", "qtd_sku", "categoria"],
        )
    df_period = _filter_period(df_full, start, end, debug=args.debug)
    df_period.attrs["returns_data"] = df_full.attrs.get("returns_data", pd.DataFrame())

    normalized_code = returns_module._normalize_product_codes([args.produto]).iloc[0]

    pedidos_venda, unidades_vendidas = _summarize_sales(
        df_period,
        normalized_code,
        debug=args.debug,
    )
    pedidos_devolucao, unidades_devolvidas = _summarize_returns(
        df_period.attrs.get("returns_data", pd.DataFrame()),
        normalized_code,
        start,
        end,
        debug=args.debug,
    )

    analysis = returns_module.build_return_analysis(df_period, category=None)

    print("=== Totais diretos ===")
    print(f"Produto: {normalized_code}")
    print(f"Pedidos vendidos: {pedidos_venda}")
    print(f"Unidades vendidas: {unidades_vendidas}")
    print(f"Pedidos devolucao: {pedidos_devolucao}")
    print(f"Unidades devolvidas: {unidades_devolvidas}")

    print("\n=== Visões da analise ===")
    for view_name, table in analysis.items():
        produto_view = table[table["cd_produto"] == normalized_code]
        if produto_view.empty:
            print(f"{view_name}: sem registros")
            continue
        row = produto_view.iloc[0]
        print(
            f"{view_name}: periodo={row['periodo']}, vendidos={row['itens_vendidos']}, devolvidos={row['itens_devolvidos']}, taxa={row['taxa_devolucao']}"
        )


if __name__ == "__main__":
    main()
