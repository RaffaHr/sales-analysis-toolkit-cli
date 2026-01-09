from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence

import hashlib
import re

import numpy as np
import pandas as pd

SALES_COLUMN_MAP = {
    "DATA_VENDA": "data",
    "NOTA_FISCAL_VENDA": "nr_nota_fiscal",
    "CATEGORIA": "categoria",
    "CD_ANUNCIO": "cd_anuncio",
    "DS_ANUNCIO": "ds_anuncio",
    "CD_PRODUTO": "cd_produto",
    "CD_FABRICANTE": "cd_fabricante",
    "DS_PRODUTO": "ds_produto",
    "TP_ANUNCIO": "tp_anuncio",
    "Custo Medio$": "custo_produto",
    "Custo Médio$": "custo_produto",
    "Preco Medio Unit$": "preco_unitario",
    "Preço Medio Unit$": "preco_unitario",
    "Unidades": "qtd_sku",
    "Perc Margem Bruta% RBLD": "perc_margem_bruta",
    "Receita Bruta (-) Devoluções Tot$": "rbld",
    "TP_REGISTRO": "tp_registro",
}

RETURN_COLUMN_MAP = {
    "DATA_VENDA": "data_venda",
    "DATA_DEVOLUCAO": "data_devolucao",
    "NOTA_FISCAL_VENDA": "nr_nota_fiscal",
    "NOTA_FISCAL_DEVOLUCAO": "nr_nota_devolucao",
    "CATEGORIA": "categoria",
    "CD_ANUNCIO": "cd_anuncio",
    "DS_ANUNCIO": "ds_anuncio",
    "CD_PRODUTO": "cd_produto",
    "CD_FABRICANTE": "cd_fabricante",
    "DS_PRODUTO": "ds_produto",
    "TP_ANUNCIO": "tp_anuncio",
    "Custo Medio$": "custo_produto",
    "Custo Médio$": "custo_produto",
    "Preco Medio Unit$": "preco_unitario",
    "Preço Medio Unit$": "preco_unitario",
    "Unidades": "qtd_sku",
    "Devolução Receita Bruta Tot$": "devolucao_receita_bruta",
    "TP_REGISTRO": "tp_registro",
}

SALES_DTYPES = {
    "NOTA_FISCAL_VENDA": str,
    "CATEGORIA": str,
    "CD_ANUNCIO": str,
    "DS_ANUNCIO": str,
    "CD_PRODUTO": str,
    "CD_FABRICANTE": str,
    "DS_PRODUTO": str,
    "TP_ANUNCIO": str,
    "Custo Medio$": str,
    "Custo Médio$": str,
    "Preco Medio Unit$": str,
    "Preço Medio Unit$": str,
    "Unidades": str,
    "Perc Margem Bruta% RBLD": str,
    "Receita Bruta (-) Devoluções Tot$": str,
    "TP_REGISTRO": str,
}

RETURN_DTYPES = {
    "NOTA_FISCAL_VENDA": str,
    "NOTA_FISCAL_DEVOLUCAO": str,
    "CATEGORIA": str,
    "CD_ANUNCIO": str,
    "DS_ANUNCIO": str,
    "CD_PRODUTO": str,
    "CD_FABRICANTE": str,
    "DS_PRODUTO": str,
    "TP_ANUNCIO": str,
    "Custo Medio$": str,
    "Custo Médio$": str,
    "Preco Medio Unit$": str,
    "Preço Medio Unit$": str,
    "Unidades": str,
    "Devolução Receita Bruta Tot$": str,
    "TP_REGISTRO": str,
}

SALES_NUMERIC_COLUMNS = [
    "custo_produto",
    "preco_unitario",
    "qtd_sku",
    "perc_margem_bruta",
    "rbld",
]

RETURN_NUMERIC_COLUMNS = [
    "custo_produto",
    "preco_unitario",
    "qtd_sku",
    "devolucao_receita_bruta",
]

PERCENT_COLUMNS = ["perc_margem_bruta"]


ProgressCallback = Callable[[int, int], None]


class SalesDataLoader:
    """Centraliza a leitura, combinação e tratamento inicial das abas de vendas."""

    def __init__(
        self,
        excel_path: Path | str,
        sheet_name: str = "VENDA",
        cache_dir: Path | str | None = Path(".cache"),
        enable_cache: bool = True,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        self.excel_path = Path(excel_path)
        self.sheet_name = sheet_name
        self.return_prefix = "DEVOLUCAO"
        self.cache_dir = Path(cache_dir) if cache_dir is not None else None
        self.enable_cache = enable_cache
        self.progress_callback = progress_callback

    def load(self) -> pd.DataFrame:
        """Carrega vendas e devoluções, aplicando os tratamentos necessários."""
        if not self.excel_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {self.excel_path}")

        sales_sheets = self._resolve_sheet_names(self.sheet_name, required=True)
        return_sheets = self._resolve_sheet_names(self.return_prefix, required=False)
        signature = self._build_signature(sales_sheets, return_sheets)
        total_steps = max(1, len(sales_sheets) + len(return_sheets) + 1)

        cache = self._try_load_cache(signature)
        if cache is not None:
            self._notify_progress(total_steps, total_steps)
            return cache

        self._notify_progress(0, total_steps)
        completed = 0

        def _advance() -> None:
            nonlocal completed
            completed += 1
            self._notify_progress(completed, total_steps)

        sales_df = self._read_group(sales_sheets, SALES_COLUMN_MAP, SALES_DTYPES, on_sheet_read=_advance)
        returns_df = (
            self._read_group(return_sheets, RETURN_COLUMN_MAP, RETURN_DTYPES, on_sheet_read=_advance)
            if return_sheets
            else pd.DataFrame()
        )

        sales_df = self._coerce_numeric(sales_df, SALES_NUMERIC_COLUMNS, PERCENT_COLUMNS)
        if not returns_df.empty:
            returns_df = self._coerce_numeric(returns_df, RETURN_NUMERIC_COLUMNS, None)

        enriched = self._enrich(sales_df, returns_df)
        _advance()
        self._store_cache(enriched, signature)
        return enriched

    def _resolve_sheet_names(self, prefix: str, *, required: bool) -> list[str]:
        with pd.ExcelFile(self.excel_path, engine="openpyxl") as workbook:
            available = workbook.sheet_names

        direct_match = prefix if prefix in available else None
        matches = [name for name in available if name.startswith(prefix)]
        if direct_match and direct_match not in matches:
            matches.insert(0, direct_match)
        if matches:
            return sorted(matches, key=_natural_sort_key)
        if required:
            raise ValueError(
                "Nenhuma aba corresponde ao padrão solicitado. "
                f"Informe uma aba existente ou use prefixos como '{prefix}01'."
            )
        return []

    def _build_signature(self, sales_sheets: Sequence[str], return_sheets: Sequence[str]) -> list[str]:
        signature = [f"{self.sheet_name}:{name}" for name in sales_sheets]
        signature.extend(f"{self.return_prefix}:{name}" for name in return_sheets)
        return signature or ["empty"]

    def _normalize_columns(self, df: pd.DataFrame, column_map: dict[str, str]) -> pd.DataFrame:
        rename_map = {original: column_map[original] for original in df.columns if original in column_map}
        df = df.rename(columns=rename_map)
        df.columns = [col.strip().lower() for col in df.columns]
        return df

    def _read_group(
        self,
        sheet_names: Sequence[str],
        column_map: dict[str, str],
        dtype_map: dict[str, object],
        *,
        on_sheet_read: Callable[[], None] | None = None,
    ) -> pd.DataFrame:
        if not sheet_names:
            return pd.DataFrame()
        frames: list[pd.DataFrame] = []
        for name in sheet_names:
            raw = pd.read_excel(
                self.excel_path,
                sheet_name=name,
                engine="openpyxl",
                dtype=dtype_map,
            )
            frames.append(self._normalize_columns(raw, column_map))
            if on_sheet_read is not None:
                on_sheet_read()
        return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    def _notify_progress(self, processed: int, total: int) -> None:
        if self.progress_callback is None:
            return
        try:
            self.progress_callback(processed, total)
        except Exception:
            pass

    def _try_load_cache(self, signature: Iterable[str]) -> Optional[pd.DataFrame]:
        if not self.enable_cache or self.cache_dir is None:
            return None
        cache_path = self._cache_path(signature)
        if not cache_path.exists():
            return None
        if cache_path.stat().st_mtime < self.excel_path.stat().st_mtime:
            return None
        try:
            return pd.read_pickle(cache_path)
        except Exception:
            return None

    def _store_cache(self, df: pd.DataFrame, signature: Iterable[str]) -> None:
        if not self.enable_cache or self.cache_dir is None:
            return
        cache_path = self._cache_path(signature)
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_pickle(cache_path)
        except Exception:
            pass

    def _cache_path(self, signature: Iterable[str]) -> Path:
        fingerprint = ",".join(sorted(signature))
        digest = hashlib.md5(fingerprint.encode("utf-8"), usedforsecurity=False).hexdigest()[:10]
        name = f"{self.excel_path.stem}_{digest}.pkl"
        base_dir = self.cache_dir if self.cache_dir is not None else self.excel_path.parent
        return base_dir / name

    def _coerce_numeric(
        self,
        df: pd.DataFrame,
        numeric_columns: Sequence[str],
        percent_columns: Optional[Sequence[str]],
    ) -> pd.DataFrame:
        if df.empty:
            return df
        for column in numeric_columns:
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
        if percent_columns:
            for column in percent_columns:
                if column not in df.columns:
                    continue
                mask = df[column].notna()
                df.loc[mask, column] = np.where(
                    df.loc[mask, column] > 1,
                    df.loc[mask, column] / 100,
                    df.loc[mask, column],
                )
        return df

    def _enrich(self, sales_df: pd.DataFrame, returns_df: pd.DataFrame) -> pd.DataFrame:
        if sales_df.empty:
            result = sales_df.copy()
            result.attrs["returns_data"] = pd.DataFrame()
            return result

        df = sales_df.copy()

        if "tp_registro" in df.columns:
            df = df[df["tp_registro"].str.contains("venda", case=False, na=True)].copy()

        df["data"] = pd.to_datetime(df.get("data"), dayfirst=True, errors="coerce")
        df["data"] = df["data"].dt.normalize()
        df["ano_mes"] = df["data"].dt.strftime("%Y%m")
        df["periodo"] = df["data"].dt.to_period("M")

        text_defaults = {
            "categoria": "Sem Categoria",
            "cd_anuncio": "",
            "ds_anuncio": "",
            "cd_produto": "",
            "ds_produto": "",
            "cd_fabricante": "",
            "tp_anuncio": "Nao informado",
            "nr_nota_fiscal": "",
        }
        for column, default in text_defaults.items():
            if column not in df.columns:
                df[column] = default
            df[column] = df[column].fillna(default)
            df[column] = df[column].astype(str).str.strip()
            if default == "":
                df[column] = df[column].replace("nan", "")

        if "cd_anuncio" not in df.columns:
            df["cd_anuncio"] = df.get("cd_produto", "")
        if "ds_anuncio" not in df.columns:
            df["ds_anuncio"] = df.get("ds_produto", "")
        df["cd_anuncio"] = df["cd_anuncio"].astype(str).str.strip()
        df["ds_anuncio"] = df["ds_anuncio"].astype(str).str.strip()

        returns_data = pd.DataFrame()
        if not returns_df.empty:
            returns = returns_df.copy()
            if "tp_registro" in returns.columns:
                returns = returns[returns["tp_registro"].str.contains("devol", case=False, na=True)].copy()
            returns["data_venda"] = pd.to_datetime(returns.get("data_venda"), dayfirst=True, errors="coerce").dt.normalize()
            returns["data_devolucao"] = pd.to_datetime(returns.get("data_devolucao"), dayfirst=True, errors="coerce").dt.normalize()
            returns["periodo_venda"] = returns["data_venda"].dt.to_period("M")
            returns["periodo_devolucao"] = returns["data_devolucao"].dt.to_period("M")
            returns["qtd_sku"] = returns.get("qtd_sku", 0).fillna(0.0)
            returns["devolucao_receita_bruta"] = returns.get("devolucao_receita_bruta", 0).fillna(0.0)
            returns["cd_anuncio"] = returns.get("cd_anuncio", returns.get("cd_produto", "")).fillna("")
            returns["ds_anuncio"] = returns.get("ds_anuncio", returns.get("ds_produto", "")).fillna("")
            returns["cd_anuncio"] = returns["cd_anuncio"].astype(str).str.strip()
            returns["ds_anuncio"] = returns["ds_anuncio"].astype(str).str.strip()

            summary = (
                returns.groupby(["nr_nota_fiscal", "cd_produto"], as_index=False)
                .agg(
                    qtd_devolvido=("qtd_sku", "sum"),
                    devolucao_receita_bruta=("devolucao_receita_bruta", "sum"),
                )
            )
            df = df.merge(summary, on=["nr_nota_fiscal", "cd_produto"], how="left")

            returns_data = returns[
                [
                    "data_venda",
                    "data_devolucao",
                    "periodo_venda",
                    "periodo_devolucao",
                    "nr_nota_fiscal",
                    "nr_nota_devolucao",
                    "categoria",
                    "cd_anuncio",
                    "ds_anuncio",
                    "cd_produto",
                    "cd_fabricante",
                    "ds_produto",
                    "tp_anuncio",
                    "qtd_sku",
                    "devolucao_receita_bruta",
                ]
            ].copy()
        if "qtd_devolvido" not in df.columns:
            df["qtd_devolvido"] = 0.0
        df["qtd_devolvido"] = df["qtd_devolvido"].fillna(0.0)

        if "devolucao_receita_bruta" not in df.columns:
            df["devolucao_receita_bruta"] = 0.0
        df["devolucao_receita_bruta"] = df["devolucao_receita_bruta"].fillna(0.0)

        df["preco_unitario"] = df.get("preco_unitario", 0).fillna(0.0)
        df["custo_produto"] = df.get("custo_produto", 0).fillna(0.0)
        df["qtd_sku"] = df.get("qtd_sku", 0).fillna(0.0)
        df["perc_margem_bruta"] = df.get("perc_margem_bruta", 0).fillna(0.0)
        df["rbld"] = df.get("rbld", 0).fillna(0.0)

        df["receita_bruta_calc"] = df["preco_unitario"] * df["qtd_sku"]
        fallback_mask = df["rbld"] <= 0
        df.loc[fallback_mask, "rbld"] = df.loc[fallback_mask, "receita_bruta_calc"]
        df["lucro_bruto_estimado"] = df["receita_bruta_calc"] * df["perc_margem_bruta"]

        base = df["qtd_sku"].to_numpy(dtype=float)
        devol = df["qtd_devolvido"].to_numpy(dtype=float)
        taxas = np.divide(devol, base, out=np.zeros_like(devol, dtype=float), where=base > 0)
        df["taxa_devolucao"] = np.nan_to_num(taxas, nan=0.0)

        df.attrs["returns_data"] = returns_data
        return df


def load_sales_dataset(path: Path | str, sheet_name: str = "VENDA", **kwargs) -> pd.DataFrame:
    """Atalho simples para carregar o conjunto de vendas padronizado."""
    loader = SalesDataLoader(path, sheet_name, **kwargs)
    return loader.load()


def _natural_sort_key(value: str) -> list[object]:
    parts = re.split(r"(\d+)", value)
    return [int(part) if part.isdigit() else part.lower() for part in parts]
