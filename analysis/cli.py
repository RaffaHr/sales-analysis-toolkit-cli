from __future__ import annotations

import re
import sys
import threading
import time
from itertools import cycle
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd

from .data_loader import load_sales_dataset
from .exporters import export_to_excel
from .reporting.low_cost import build_low_cost_reputation_analysis
from .reporting.potential import build_potential_sku_analysis
from .reporting.product_focus import build_product_focus_analysis
from .reporting.returns import build_return_analysis
from .reporting.top_history import build_top_history_analysis


def _copy_with_attrs(source: pd.DataFrame, target: pd.DataFrame) -> pd.DataFrame:
    """Garante que o DataFrame copiado mantenha os metadados do original."""
    target.attrs = dict(source.attrs)
    return target


class _ProgressPrinter:
    """Exibe o progresso percentual real das etapas de carregamento."""

    def __init__(self, label: str) -> None:
        self._label = label.strip()
        self._display_percent = 0
        self._target_percent = 0
        self._finished = False
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._animate, daemon=True)
        self._started = False

    def update(self, processed: int, total: int) -> None:
        percent = 100 if total <= 0 else int(round((processed / total) * 100))
        percent = max(0, min(100, percent))
        with self._lock:
            self._target_percent = max(self._target_percent, percent)
        if not self._started:
            self._thread.start()
            self._started = True

    def finish(self) -> None:
        with self._lock:
            self._target_percent = max(self._target_percent, 100)
            self._finished = True
        if not self._started:
            self._thread.start()
            self._started = True

        while True:
            with self._lock:
                if self._display_percent >= self._target_percent:
                    break
            time.sleep(0.05)

        self._stop_event.set()
        self._thread.join()
        sys.stdout.write(f"\r{self._label}: {self._target_percent:3d}%\n")
        sys.stdout.flush()

    def _animate(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                target = self._target_percent
                display = self._display_percent
                if display < target:
                    step = max(1, (target - display) // 5 or 1)
                    display = min(target, display + step)
                    self._display_percent = display
                elif self._finished and display >= target:
                    break
            sys.stdout.write(f"\r{self._label}: {display:3d}%")
            sys.stdout.flush()
            time.sleep(0.05)


class _ConsoleSpinner:
    """Mostra uma animação simples enquanto operações demoradas rodam."""

    def __init__(self, message: str, interval: float = 0.1) -> None:
        self._message = message.strip()
        self._interval = interval
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._animate, daemon=True)
        self._line_template = f"{self._message} " if self._message else ""

    def __enter__(self) -> "_ConsoleSpinner":
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._stop_event.set()
        self._thread.join()
        clear_len = len(self._line_template) + 2
        sys.stdout.write("\r" + " " * clear_len + "\r")
        sys.stdout.flush()

    def _animate(self) -> None:
        spinner = cycle("|/-\\")
        while not self._stop_event.is_set():
            frame = next(spinner)
            sys.stdout.write(f"\r{self._line_template}{frame}")
            sys.stdout.flush()
            time.sleep(self._interval)


class AnalysisOption:
    def __init__(
        self,
        key: str,
        label: str,
        builder: Callable[..., Dict[str, pd.DataFrame]],
        needs_rank: bool = False,
        needs_product_codes: bool = False,
    ) -> None:
        self.key = key
        self.label = label
        self.builder = builder
        self.needs_rank = needs_rank
        self.needs_product_codes = needs_product_codes


ANALYSIS_OPTIONS = [
    AnalysisOption(
        key="RETURN",
        label="Análise de Devolução",
        builder=build_return_analysis,
    ),
    AnalysisOption(
        key="POTENTIAL",
        label="Análise de SKU em Potencial de Venda",
        builder=build_potential_sku_analysis,
        needs_rank=True,
    ),
    AnalysisOption(
        key="TOP_SELLERS",
        label="Análise de Top SKUs Mais Vendidos Historicamente",
        builder=build_top_history_analysis,
        needs_rank=True,
    ),
    AnalysisOption(
        key="REPUTATION",
        label="Análise de Produto de Custo Baixo para Reputação",
        builder=build_low_cost_reputation_analysis,
    ),
    AnalysisOption(
        key="PRODUCT_FOCUS",
        label="Análise de performance de venda",
        builder=build_product_focus_analysis,
        needs_product_codes=True,
    ),
]


def run_cli(dataset_path: Path | str = Path("BASE.xlsx")) -> None:
    """Executa o fluxo interativo de seleção e geração das análises."""
    progress = _ProgressPrinter("Carregando dados")
    df_full = load_sales_dataset(dataset_path, progress_callback=progress.update)
    progress.finish()

    with _ConsoleSpinner("Calculando métricas históricas"):
        historical_prices = _compute_historical_lowest_prices(df_full)

    while True:
        option = _prompt_analysis_option()
        if option is None:
            print("Encerrando aplicação.")
            break

        period_range = _prompt_period_range(df_full)
        df_period = _apply_period_range(df_full, period_range)
        if df_period.empty:
            print("Sem dados para o período selecionado. Tente novamente.")
            continue

        categories = sorted(df_period["categoria"].dropna().unique())
        category_filter: Optional[str] = None
        df_for_category = _copy_with_attrs(df_period, df_period.copy())
        product_codes: Optional[list[str]] = None

        if option.needs_product_codes:
            filter_mode = _prompt_focus_filter_mode()
            if filter_mode == "category":
                category = _prompt_category(categories)
                if category != "Todas":
                    category_filter = category
                    df_for_category = _copy_with_attrs(
                        df_period,
                        df_period[df_period["categoria"] == category_filter].copy(),
                    )
            else:
                product_codes = _prompt_product_codes(df_period)
                df_for_category = _copy_with_attrs(
                    df_period,
                    df_period[df_period["cd_anuncio"].isin(product_codes)].copy(),
                )
        else:
            category = _prompt_category(categories)
            if category != "Todas":
                category_filter = category
                df_for_category = _copy_with_attrs(
                    df_period,
                    df_period[df_period["categoria"] == category_filter].copy(),
                )

        if df_for_category.empty:
            if product_codes is not None:
                print("Nenhum registro encontrado para os códigos informados. Tente novamente.")
            else:
                print("Nenhum registro encontrado com essa combinação de filtros.")
            continue

        extra_args: Dict[str, object] = {}
        if option.needs_rank:
            extra_args["rank_size"] = _prompt_rank_size()

        if option.key in {"POTENTIAL", "TOP_SELLERS", "REPUTATION"}:
            extra_args["historical_prices"] = historical_prices

        if option.key == "POTENTIAL":
            extra_args.update(_prompt_potential_window(df_for_category))

        if product_codes is not None:
            extra_args["product_codes"] = product_codes

        dataframes = option.builder(df_period, category_filter, **extra_args)

        export_payload = dict(dataframes)

        base_name = option.key
        if period_range:
            base_name = f"{base_name}_{_format_period_suffix(period_range)}"
        output_file = export_to_excel(export_payload, base_name)
        print(f"Arquivo gerado: {output_file}")

        if not _prompt_continue():
            print("Encerrando aplicação.")
            break


def _prompt_analysis_option() -> Optional[AnalysisOption]:
    print("\nSelecione a análise desejada:")
    for index, option in enumerate(ANALYSIS_OPTIONS, start=1):
        print(f" {index}. {option.label}")
    print(" 0. Sair")

    while True:
        try:
            choice = int(input("Opção: ").strip())
        except ValueError:
            print("Informe um número válido.")
            continue
        if choice == 0:
            return None
        if 1 <= choice <= len(ANALYSIS_OPTIONS):
            return ANALYSIS_OPTIONS[choice - 1]
        print("Escolha uma opção existente.")


def _prompt_category(categories: list[str]) -> str:
    print("\nCategorias disponíveis:")
    print(" 0. Todas")
    for index, category in enumerate(categories, start=1):
        print(f" {index}. {category}")
    while True:
        try:
            choice = int(input("Categoria: ").strip())
        except ValueError:
            print("Informe um número válido.")
            continue
        if choice == 0:
            return "Todas"
        if 1 <= choice <= len(categories):
            return categories[choice - 1]
        print("Escolha uma categoria existente.")


def _prompt_rank_size() -> int:
    print("\nSelecione o tamanho do ranking (10, 20, 50, 100) ou informe um valor customizado:")
    while True:
        value = input("Ranking: ").strip()
        if value in {"10", "20", "50", "100"}:
            return int(value)
        if value.isdigit() and int(value) > 0:
            return int(value)
        print("Informe um número positivo.")


def _prompt_continue() -> bool:
    answer = input("\nDeseja realizar outra análise? (s/n): ").strip().lower()
    return answer == "s"


def _prompt_product_codes(df: pd.DataFrame) -> list[str]:
    available_series = df.get("cd_anuncio", pd.Series(dtype=str))
    available = sorted({str(code).strip() for code in available_series.dropna() if str(code).strip()})
    if available:
        preview = ", ".join(available[:10])
        suffix = "..." if len(available) > 10 else ""
        print("\nInforme os códigos de anúncio (CD_ANUNCIO) separados por vírgula ou ponto e vírgula.")
        print(f"Exemplos disponíveis: {preview}{suffix}")
    else:
        print("\nInforme os códigos de anúncio (CD_ANUNCIO) separados por vírgula ou ponto e vírgula.")
        print("Nenhum código disponível no filtro atual, mas você ainda pode informar manualmente.")

    while True:
        raw = input("CD_ANUNCIO(s): ").strip()
        parts = [part.strip() for part in re.split(r"[;,]", raw) if part.strip()]
        if not parts:
            print("Informe ao menos um código de anúncio válido.")
            continue
        unique_codes = list(dict.fromkeys(parts))
        missing = [code for code in unique_codes if code not in available]
        if missing and available:
            print(
                "Aviso: alguns códigos não foram encontrados no filtro atual e serão considerados mesmo assim: "
                + ", ".join(missing)
            )
        return unique_codes


def _prompt_focus_filter_mode() -> str:
    print("\nComo deseja filtrar esta análise?")
    print(" 1. Filtrar por categoria")
    print(" 2. Informar lista de CD_ANUNCIO")
    while True:
        choice = input("Opção: ").strip()
        if choice == "1":
            return "category"
        if choice == "2":
            return "products"
        print("Informe 1 ou 2 para selecionar o modo de filtro.")




def _compute_historical_lowest_prices(df: pd.DataFrame) -> Dict[str, float]:
    price_col = "preco_unitario" if "preco_unitario" in df.columns else "preco_vendido"
    if price_col not in df.columns:
        return {}
    valid = df.loc[df[price_col].notna()]
    if valid.empty:
        return {}
    return (
        valid.groupby("cd_anuncio")[price_col].min()
        .round(2)
        .to_dict()
    )


def _prompt_period_range(df: pd.DataFrame) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    base_series = df.get("data", pd.Series(dtype="datetime64[ns]"))
    available_dates = pd.to_datetime(base_series, dayfirst=True, errors="coerce")
    available_dates = available_dates.dropna().sort_values()
    if available_dates.empty:
        return None

    min_date = available_dates.iloc[0]
    max_date = available_dates.iloc[-1]

    periods = sorted(
        p for p in df["periodo"].dropna().unique() if isinstance(p, pd.Period)
    )
    if periods:
        _display_available_periods(periods)

    print(
        f"Intervalo disponível: {min_date.strftime('%d/%m/%Y')} até {max_date.strftime('%d/%m/%Y')}"
    )
    print(
        "Informe o intervalo desejado no formato DD/MM/AAAA. Pressione Enter para analisar todo o histórico."
    )

    while True:
        start_input = input("Data inicial (DD/MM/AAAA) ou Enter: ").strip()
        if not start_input:
            return None
        end_input = input("Data final (DD/MM/AAAA): ").strip()

        start_date = _parse_date_input(start_input)
        end_date = _parse_date_input(end_input)

        if start_date is None or end_date is None:
            print("Formato inválido. Use DD/MM/AAAA, por exemplo 25/01/2025.")
            continue

        start_date = start_date.normalize()
        end_date = end_date.normalize()

        if start_date < min_date.normalize() or end_date > max_date.normalize():
            print("Datas fora do intervalo disponível. Escolha novamente.")
            continue

        if start_date > end_date:
            print("Data inicial deve ser menor ou igual à final.")
            continue

        return start_date, end_date


def _apply_period_range(
    df: pd.DataFrame, period_range: Optional[Tuple[pd.Timestamp, pd.Timestamp]]
) -> pd.DataFrame:
    if not period_range:
        return _copy_with_attrs(df, df.copy())
    start, end = period_range
    if "data" in df.columns:
        data_series = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
        mask = data_series.between(start, end)
        return _copy_with_attrs(df, df.loc[mask].copy())
    mask = df["periodo"].between(start.to_period("M"), end.to_period("M"))
    return _copy_with_attrs(df, df.loc[mask].copy())


def _format_period_suffix(
    period_range: Optional[
        Tuple[Union[pd.Timestamp, pd.Period], Union[pd.Timestamp, pd.Period]]
    ]
) -> str:
    if not period_range:
        return "historico_completo"
    start, end = period_range
    if isinstance(start, pd.Timestamp):
        return f"{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
    return f"{start.strftime('%Y%m')}_{end.strftime('%Y%m')}"


def _display_available_periods(periods: List[pd.Period]) -> None:
    months_map = {
        1: "jan",
        2: "fev",
        3: "mar",
        4: "abr",
        5: "mai",
        6: "jun",
        7: "jul",
        8: "ago",
        9: "set",
        10: "out",
        11: "nov",
        12: "dez",
    }
    year_months: Dict[int, set[int]] = {}
    for period in periods:
        year_months.setdefault(period.year, set()).add(period.month)

    print("\nPeríodos disponíveis:")
    for year in sorted(year_months):
        months = sorted(year_months[year])
        if len(months) == 12:
            print(f" {year} (período completo disponível)")
        else:
            months_names = ", ".join(months_map[m] for m in months)
            print(f" {year}: {months_names}")


def _parse_period_input(value: str) -> Optional[pd.Period]:
    normalized = value.strip().replace("/", "-")
    try:
        return pd.Period(normalized, freq="M")
    except (ValueError, TypeError):
        return None


def _parse_date_input(value: str) -> Optional[pd.Timestamp]:
    normalized = value.strip()
    if not normalized:
        return None
    normalized = normalized.replace("-", "/")
    parsed = pd.to_datetime(normalized, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed


def _prompt_potential_window(df: pd.DataFrame) -> Dict[str, object]:
    periods = sorted(
        p for p in df["periodo"].dropna().unique() if isinstance(p, pd.Period)
    )
    if len(periods) <= 1:
        return {}

    answer = input(
        "Deseja definir manualmente a janela recente para análise de potencial? (s/n): "
    ).strip().lower()
    if answer != "s":
        return {}

    _display_available_periods(periods)

    available_strings = {p.strftime("%Y-%m") for p in periods}
    while True:
        try:
            count = int(input("Quantidade de meses na janela recente: ").strip())
        except ValueError:
            print("Informe um número válido de meses.")
            continue
        if count <= 0:
            print("Informe um número positivo.")
            continue
        if count >= len(available_strings):
            print("Selecione menos meses que o total disponível para manter um histórico comparativo.")
            continue

        selected: List[str] = []
        while len(selected) < count:
            idx = len(selected) + 1
            period_input = input(f"Período {idx} (AAAA-MM): ").strip()
            period = _parse_period_input(period_input)
            if not period:
                print("Formato inválido. Use AAAA-MM.")
                continue
            period_str = period.strftime("%Y-%m")
            if period_str not in available_strings:
                print("Período fora da faixa disponível. Informe outro.")
                continue
            if period_str in selected:
                print("Período já selecionado. Informe outro.")
                continue
            selected.append(period_str)

        remaining = available_strings.difference(selected)
        if not remaining:
            print("A seleção cobre todo o intervalo. Selecione meses que permitam comparação com histórico.")
            continue

        selected.sort()
        return {"recent_periods": selected}
