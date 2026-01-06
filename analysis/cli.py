from __future__ import annotations

import sys
import threading
import time
from itertools import cycle
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple, List

import pandas as pd

from .data_loader import load_sales_dataset
from .exporters import export_to_excel
from .reporting.low_cost import build_low_cost_reputation_analysis
from .reporting.potential import build_potential_sku_analysis
from .reporting.returns import build_return_analysis
from .reporting.top_history import build_top_history_analysis


class _ProgressPrinter:
    """Exibe o progresso percentual real das etapas de carregamento."""

    def __init__(self, label: str) -> None:
        self._label = label.strip()
        self._last_percent = -1
        self._finished = False

    def update(self, processed: int, total: int) -> None:
        percent = 100 if total <= 0 else int(round((processed / total) * 100))
        percent = max(0, min(100, percent))
        if percent != self._last_percent or processed >= total:
            sys.stdout.write(f"\r{self._label}: {percent:3d}%")
            sys.stdout.flush()
            self._last_percent = percent
        if processed >= total and not self._finished:
            sys.stdout.write("\n")
            sys.stdout.flush()
            self._finished = True

    def finish(self) -> None:
        if not self._finished:
            sys.stdout.write("\n")
            sys.stdout.flush()
            self._finished = True


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
    ) -> None:
        self.key = key
        self.label = label
        self.builder = builder
        self.needs_rank = needs_rank


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
        category = _prompt_category(categories)
        category_filter = None if category == "Todas" else category

        df_for_category = (
            df_period
            if category_filter is None
            else df_period[df_period["categoria"] == category_filter]
        )
        if df_for_category.empty:
            print("Nenhum registro encontrado com essa combinação de filtros.")
            continue

        extra_args: Dict[str, object] = {}
        if option.needs_rank:
            extra_args["rank_size"] = _prompt_rank_size()

        if option.key in {"POTENTIAL", "TOP_SELLERS", "REPUTATION"}:
            extra_args["historical_prices"] = historical_prices

        if option.key == "POTENTIAL":
            extra_args.update(_prompt_potential_window(df_for_category))

        dataframes = option.builder(df_period, category_filter, **extra_args)

        base_name = option.key
        if period_range:
            base_name = f"{base_name}_{_format_period_suffix(period_range)}"
        output_file = export_to_excel(dataframes, base_name)
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


def _compute_historical_lowest_prices(df: pd.DataFrame) -> Dict[str, float]:
    valid = df.loc[df["preco_vendido"].notna()]
    return (
        valid.groupby("cd_produto")["preco_vendido"].min()
        .round(2)
        .to_dict()
    )


def _prompt_period_range(df: pd.DataFrame) -> Optional[Tuple[pd.Period, pd.Period]]:
    periods = sorted(
        p for p in df["periodo"].dropna().unique() if isinstance(p, pd.Period)
    )
    if not periods:
        return None

    _display_available_periods(periods)
    print("Informe o intervalo desejado no formato AAAA-MM. Pressione Enter para analisar todo o histórico.")

    while True:
        start_input = input("Período inicial (AAAA-MM) ou Enter: ").strip()
        if not start_input:
            return None
        end_input = input("Período final (AAAA-MM): ").strip()

        start_period = _parse_period_input(start_input)
        end_period = _parse_period_input(end_input)

        if not start_period or not end_period:
            print("Formato inválido. Use AAAA-MM, por exemplo 2025-01.")
            continue

        if start_period not in periods or end_period not in periods:
            print("Período fora do intervalo disponível. Escolha novamente.")
            continue

        if start_period > end_period:
            print("Período inicial deve ser menor ou igual ao período final.")
            continue

        return start_period, end_period


def _apply_period_range(
    df: pd.DataFrame, period_range: Optional[Tuple[pd.Period, pd.Period]]
) -> pd.DataFrame:
    if not period_range:
        return df.copy()
    start, end = period_range
    mask = df["periodo"].between(start, end)
    return df.loc[mask].copy()


def _format_period_suffix(period_range: Optional[Tuple[pd.Period, pd.Period]]) -> str:
    if not period_range:
        return "historico_completo"
    start, end = period_range
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
