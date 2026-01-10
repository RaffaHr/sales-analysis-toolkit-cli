from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
import io

import pandas as pd

from analysis.data_loader import load_sales_dataset


@dataclass
class PeriodFilter:
    start: Optional[pd.Timestamp] = None
    end: Optional[pd.Timestamp] = None

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.start is None and self.end is None:
            return df.copy()
        data_series = pd.to_datetime(df.get("data"), dayfirst=True, errors="coerce").dt.normalize()
        mask = pd.Series(True, index=df.index)
        if self.start is not None:
            mask &= data_series >= self.start
        if self.end is not None:
            mask &= data_series <= self.end
        return df.loc[mask].copy()


class DatasetManager:
    """Centraliza o carregamento e otimização da base de vendas."""

    def __init__(self, source_path: Path, cache_dir: Path) -> None:
        self._source_path = Path(source_path)
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._dataset: pd.DataFrame | None = None
        self._historical_prices: dict[str, float] | None = None

    def load(self, *, force: bool = False) -> pd.DataFrame:
        if self._dataset is None or force:
            self._dataset = load_sales_dataset(self._source_path)
        return self._dataset

    def load_filtered(self, period_filter: PeriodFilter | None = None) -> pd.DataFrame:
        df = self.load()
        if period_filter is None:
            result = df.copy()
        else:
            result = period_filter.apply(df)
        result.attrs = dict(df.attrs)
        return result

    def historical_prices(self) -> dict[str, float]:
        if self._historical_prices is None:
            df = self.load()
            price_col = "preco_unitario" if "preco_unitario" in df.columns else "preco_vendido"
            if price_col not in df.columns:
                self._historical_prices = {}
            else:
                valid = df[df[price_col].notna()]
                if valid.empty:
                    self._historical_prices = {}
                else:
                    series = (
                        valid.groupby("cd_anuncio")[price_col].min().round(2)
                    )
                    self._historical_prices = series.to_dict()
        return dict(self._historical_prices)

    def to_parquet(self, destination: Path, *, force: bool = False) -> Path:
        destination = Path(destination)
        if destination.exists() and not force:
            return destination
        df = self.load()
        destination.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(destination, index=False)
        return destination

    def export_analysis_tables(self, tables: dict[str, pd.DataFrame]) -> bytes:
        """Gera bytes de um Excel com as tabelas informadas."""
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            for sheet_name, table in tables.items():
                table.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        buffer.seek(0)
        return buffer.read()

    def iter_batches(self, batch_size: int = 100_000) -> Iterable[pd.DataFrame]:
        df = self.load()
        total_rows = len(df)
        for start in range(0, total_rows, batch_size):
            yield df.iloc[start : start + batch_size].copy()
