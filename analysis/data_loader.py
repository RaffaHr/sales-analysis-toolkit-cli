from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

COLUMN_MAP = {
    "ANO_MES": "ano_mes",
    "NR_NOTA_FISCAL": "nr_nota_fiscal",
    "CATEGORIA": "categoria",
    "CD_PRODUTO": "cd_produto",
    "DS_PRODUTO": "ds_produto",
    "Qtd de pedido": "qtd_pedidos",
    "Qtd de sku no pedido": "qtd_sku",
    "ROB": "rob",
    "Preco vendido": "preco_vendido",
    "Perc Margem Bruta% RBLD": "perc_margem_bruta",
    "Custo do produto": "custo_produto",
    "Qtd Produto Devolvido": "qtd_devolvido",
    "Devolução Receita Bruta Tot$": "devolucao_receita_bruta",
}

NUMERIC_COLUMNS = [
    "qtd_pedidos",
    "qtd_sku",
    "rob",
    "preco_vendido",
    "perc_margem_bruta",
    "custo_produto",
    "qtd_devolvido",
    "devolucao_receita_bruta",
]

PERCENT_COLUMNS = ["perc_margem_bruta"]


class SalesDataLoader:
    """Centraliza a leitura e tratamento inicial da planilha de vendas."""

    def __init__(self, excel_path: Path | str, sheet_name: str = "VENDA") -> None:
        self.excel_path = Path(excel_path)
        self.sheet_name = sheet_name

    def load(self) -> pd.DataFrame:
        """Carrega a aba de vendas e devolve um DataFrame padronizado."""
        if not self.excel_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {self.excel_path}")

        df = pd.read_excel(
            self.excel_path,
            sheet_name=self.sheet_name,
            engine="openpyxl",
            dtype={
                "CD_PRODUTO": str,
                "DS_PRODUTO": str,
                "ANO_MES": str,
                "NR_NOTA_FISCAL": str,
            },
        )

        df = self._normalize_columns(df)
        df = self._coerce_numeric(df)
        df = self._enrich(df)
        return df

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        available_map = {original: COLUMN_MAP[original] for original in df.columns if original in COLUMN_MAP}
        df = df.rename(columns=available_map)
        df.columns = [col.strip().lower() for col in df.columns]
        return df

    def _coerce_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        for column in NUMERIC_COLUMNS:
            if column not in df.columns:
                continue
            series = df[column]
            if series.dtype == object:
                series = (
                    series.astype(str)
                    .str.replace("%", "", regex=False)
                    .str.replace(",", ".", regex=False)
                )
            df[column] = pd.to_numeric(series, errors="coerce")
        for column in PERCENT_COLUMNS:
            if column not in df.columns:
                continue
            mask = df[column].notna()
            df.loc[mask, column] = np.where(
                df.loc[mask, column] > 1,
                df.loc[mask, column] / 100,
                df.loc[mask, column],
            )
        return df

    def _enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        if "ano_mes" in df.columns:
            cleaned = df["ano_mes"].astype(str).str.replace(" ", "", regex=False)
            cleaned = cleaned.str.replace(r"[^0-9]", "", regex=True)
            cleaned = cleaned.where(cleaned.str.len() == 6)
            df["ano_mes"] = cleaned
            df["periodo"] = pd.to_datetime(df["ano_mes"], format="%Y%m", errors="coerce").dt.to_period("M")
        else:
            df["periodo"] = pd.NaT

        if "categoria" not in df.columns:
            df["categoria"] = "Sem Categoria"
        df["categoria"] = df["categoria"].fillna("Sem Categoria").astype(str).str.strip()

        if "cd_produto" not in df.columns:
            df["cd_produto"] = ""
        df["cd_produto"] = df["cd_produto"].fillna("").astype(str).str.strip()

        if "ds_produto" not in df.columns:
            df["ds_produto"] = ""
        df["ds_produto"] = df["ds_produto"].fillna("").astype(str).str.strip()

        if "nr_nota_fiscal" not in df.columns:
            df["nr_nota_fiscal"] = ""
        df["nr_nota_fiscal"] = df["nr_nota_fiscal"].fillna("").astype(str).str.strip()

        qtd_sku = df["qtd_sku"] if "qtd_sku" in df.columns else pd.Series(0, index=df.index, dtype=float)
        preco_vendido = df["preco_vendido"] if "preco_vendido" in df.columns else pd.Series(0, index=df.index, dtype=float)
        custo_unitario = df["custo_produto"] if "custo_produto" in df.columns else pd.Series(0, index=df.index, dtype=float)
        devolvido = df["qtd_devolvido"] if "qtd_devolvido" in df.columns else pd.Series(0, index=df.index, dtype=float)
        margem = df["perc_margem_bruta"] if "perc_margem_bruta" in df.columns else pd.Series(0, index=df.index, dtype=float)
        receita = df["rob"] if "rob" in df.columns else pd.Series(0, index=df.index, dtype=float)

        qtd_sku = qtd_sku.fillna(0)
        preco_vendido = preco_vendido.fillna(0)
        custo_unitario = custo_unitario.fillna(0)
        devolvido = devolvido.fillna(0)
        margem = margem.fillna(0)
        receita = receita.fillna(0)

        df["receita_bruta_calc"] = preco_vendido * qtd_sku
        df["rob"] = receita.where(receita > 0, df["receita_bruta_calc"])
        df["custo_total"] = custo_unitario * qtd_sku
        df["lucro_bruto_estimado"] = df["receita_bruta_calc"] * margem

        base = qtd_sku.to_numpy(dtype=float)
        devol = devolvido.to_numpy(dtype=float)
        taxas = np.divide(
            devol,
            base,
            out=np.zeros_like(devol, dtype=float),
            where=base > 0,
        )
        df["taxa_devolucao"] = np.nan_to_num(taxas, nan=0.0)
        return df


def load_sales_dataset(path: Path | str, sheet_name: str = "VENDA") -> pd.DataFrame:
    """Atalho simples para carregar o conjunto de vendas padronizado."""
    loader = SalesDataLoader(path, sheet_name)
    return loader.load()
