from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd

DEFAULT_OUTPUT_DIR = Path("output")


def export_to_excel(dataframes: Dict[str, pd.DataFrame], base_name: str, output_dir: Path | str = DEFAULT_OUTPUT_DIR) -> Path:
    """Gera um arquivo Excel novo reunindo os DataFrames fornecidos."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    safe_base = base_name.replace(" ", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_path / f"{safe_base}_{timestamp}.xlsx"

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        for sheet_name, df in dataframes.items():
            sanitized_sheet = sheet_name[:31]
            df.to_excel(writer, sheet_name=sanitized_sheet, index=False)
    return file_path
