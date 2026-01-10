from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class AppConfig:
    base_dataset_path: Path
    cache_dir: Path
    database_path: Path
    vectorstore_dir: Path
    default_rank_size: int = 20
    default_recent_window: int = 3

    @property
    def optimized_dataset_path(self) -> Path:
        return self.cache_dir / "datasets" / f"{self.base_dataset_path.stem}.parquet"


def load_config() -> AppConfig:
    project_root = Path(os.getenv("SALES_TOOLKIT_ROOT", Path.cwd()))
    dataset_path = Path(os.getenv("SALES_TOOLKIT_DATASET", project_root / "BASE.xlsx"))
    cache_dir = Path(os.getenv("SALES_TOOLKIT_CACHE", project_root / ".cache" / "streamlit"))
    database_path = Path(os.getenv("SALES_TOOLKIT_DB", cache_dir / "chatbot.db"))
    vectorstore_dir = Path(os.getenv("SALES_TOOLKIT_VECTORSTORE", cache_dir / "vectorstore"))
    return AppConfig(
        base_dataset_path=dataset_path,
        cache_dir=cache_dir,
        database_path=database_path,
        vectorstore_dir=vectorstore_dir,
    )
