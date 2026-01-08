from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd

DEFAULT_OUTPUT_DIR = Path("output")


def export_to_excel(
    dataframes: Dict[str, pd.DataFrame],
    base_name: str,
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
) -> Path:
    """Gera um arquivo Excel contendo apenas as tabelas fornecidas."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    payload = dict(dataframes)
    used_table_names: set[str] = set()

    safe_base = base_name.replace(" ", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_path / f"{safe_base}_{timestamp}.xlsx"

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        for sheet_name, df in payload.items():
            sanitized_sheet = sheet_name[:31]
            df.to_excel(writer, sheet_name=sanitized_sheet, index=False)
            worksheet = writer.sheets[sanitized_sheet]
            table_name = _unique_table_name(sanitized_sheet, used_table_names)
            _add_table_layout(worksheet, df, table_name)

    return file_path


def _add_table_layout(worksheet, df: pd.DataFrame, table_name: str) -> None:
    rows = len(df.index)
    cols = len(df.columns)
    if rows == 0 or cols == 0:
        return
    table_options = {
        "columns": [{"header": str(col)} for col in df.columns],
        "style": "Table Style Medium 9",
        "autofilter": True,
        "name": table_name,
    }
    worksheet.add_table(0, 0, rows, cols - 1, table_options)
    for col_idx in range(cols):
        worksheet.set_column(col_idx, col_idx, 15)


def _unique_table_name(base: str, used: set[str]) -> str:
    sanitized = "".join(ch if ch.isalnum() else "_" for ch in base)
    sanitized = sanitized[:31] or "tabela"
    candidate = sanitized
    counter = 1
    while candidate in used:
        suffix = f"_{counter}"
        candidate = f"{sanitized[:31 - len(suffix)]}{suffix}"
        counter += 1
    used.add(candidate)
    return candidate
