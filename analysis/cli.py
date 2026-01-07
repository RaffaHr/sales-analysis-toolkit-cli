from __future__ import annotations

import re
import sys
import threading
import time
from itertools import cycle
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, List, Union, Set

import pandas as pd

from .data_loader import load_sales_dataset
from .exporters import export_to_excel
from .reporting.low_cost import build_low_cost_reputation_analysis
from .reporting.potential import build_potential_sku_analysis
from .reporting.product_focus import build_product_focus_analysis
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
        df_for_category = df_period.copy()
        product_codes: Optional[list[str]] = None

        if option.needs_product_codes:
            filter_mode = _prompt_focus_filter_mode()
            if filter_mode == "category":
                category = _prompt_category(categories)
                if category != "Todas":
                    category_filter = category
                    df_for_category = df_period[df_period["categoria"] == category_filter].copy()
            else:
                product_codes = _prompt_product_codes(df_period)
                df_for_category = df_period[df_period["cd_produto"].isin(product_codes)].copy()
        else:
            category = _prompt_category(categories)
            if category != "Todas":
                category_filter = category
                df_for_category = df_period[df_period["categoria"] == category_filter].copy()

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
        chart_configs = _prompt_chart_configs(option, dataframes)
        if chart_configs:
            export_payload["__charts__"] = chart_configs

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
    available_series = df.get("cd_produto", pd.Series(dtype=str))
    available = sorted({str(code).strip() for code in available_series.dropna() if str(code).strip()})
    if available:
        preview = ", ".join(available[:10])
        suffix = "..." if len(available) > 10 else ""
        print("\nInforme os códigos de produto (CD_PRODUTO) separados por vírgula ou ponto e vírgula.")
        print(f"Exemplos disponíveis: {preview}{suffix}")
    else:
        print("\nInforme os códigos de produto (CD_PRODUTO) separados por vírgula ou ponto e vírgula.")
        print("Nenhum código disponível no filtro atual, mas você ainda pode informar manualmente.")

    while True:
        raw = input("CD_PRODUTO(s): ").strip()
        parts = [part.strip() for part in re.split(r"[;,]", raw) if part.strip()]
        if not parts:
            print("Informe ao menos um código de produto válido.")
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
    print(" 2. Informar lista de CD_PRODUTO")
    while True:
        choice = input("Opção: ").strip()
        if choice == "1":
            return "category"
        if choice == "2":
            return "products"
        print("Informe 1 ou 2 para selecionar o modo de filtro.")


def _prompt_yes_no(message: str) -> bool:
    while True:
        answer = input(message).strip().lower()
        if answer in {"s", "sim"}:
            return True
        if answer in {"n", "nao", "não"}:
            return False
        print("Responda com 's' ou 'n'.")


def _prompt_chart_configs(
    option: AnalysisOption,
    dataframes: Dict[str, pd.DataFrame],
) -> List[Dict[str, object]]:
    if not dataframes:
        return []
    if not _prompt_yes_no("\nDeseja gerar gráficos neste relatório? (s/n): "):
        return []

    configs: List[Dict[str, object]] = []
    for sheet_name, df in dataframes.items():
        if sheet_name.startswith("__") or df.empty:
            continue
        if "cd_produto" not in df.columns:
            print(f"Aba '{sheet_name}' não possui coluna 'cd_produto'; gráfico não será criado.")
            continue
        if not _prompt_yes_no(f"\nGerar gráfico para a aba '{sheet_name}'? (s/n): "):
            continue

        columns_info = _infer_columns_info(df)
        if not columns_info:
            print("Não foi possível identificar colunas válidas para gráficos.")
            continue

        if _prompt_yes_no("Deseja personalizar o gráfico desta aba? (s/n): "):
            config_core = _build_custom_chart_config(sheet_name, df, columns_info)
        else:
            config_core = _build_default_chart_config(option.key, sheet_name, df, columns_info)

        if not config_core:
            print("Configuração de gráfico ignorada para esta aba.")
            continue

        config_core.update(
            {
                "sheet": sheet_name,
                "filter_column": "cd_produto",
                "dropdown_label": "Selecione o produto:",
                "dataframe": df,
                "columns_info": columns_info,
            }
        )
        configs.append(config_core)

    return configs


def _infer_columns_info(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    info: Dict[str, Dict[str, Any]] = {}
    percent_pattern = re.compile(r"^-?\d+(?:[.,]\d+)?%$")
    numeric_pattern = re.compile(r"^-?\d+(?:[.,]\d+)?$")
    date_patterns = [
        re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$"),
        re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    ]

    for column in df.columns:
        series = df[column]
        kind = "text"
        convert_to_date = False
        if pd.api.types.is_datetime64_any_dtype(series):
            kind = "date"
        elif pd.api.types.is_numeric_dtype(series):
            if _looks_like_percentage(column, series):
                kind = "percentage"
            else:
                kind = "numeric"
        else:
            sample = series.dropna().astype(str)
            if sample.empty:
                kind = "text"
            elif sample.str.match(percent_pattern).all():
                kind = "percentage_text"
            elif sample.str.match(numeric_pattern).all():
                kind = "numeric_text"
            elif any(sample.str.match(pattern).all() for pattern in date_patterns):
                kind = "date"
                convert_to_date = True
            else:
                kind = "text"
        entry: Dict[str, Any] = {"kind": kind}
        if convert_to_date:
            entry["convert_to_date"] = True
        info[column] = entry
    return info


def _looks_like_percentage(column_name: str, series: pd.Series) -> bool:
    name = column_name.lower()
    keywords = ("percent", "perc", "pct", "margem", "taxa", "rate")
    if any(keyword in name for keyword in keywords):
        return True
    non_na = pd.to_numeric(series.dropna(), errors="coerce")
    if non_na.empty:
        return False
    return non_na.between(-2, 2).all()


def _build_default_chart_config(
    option_key: str,
    sheet_name: str,
    df: pd.DataFrame,
    columns_info: Dict[str, Dict[str, str]],
) -> Optional[Dict[str, object]]:
    if "cd_produto" not in df.columns:
        return None

    x_candidates = ["data", "periodo", "mes", "ano_mes"]
    x_column = next((col for col in x_candidates if col in df.columns), None)
    if x_column is None:
        x_column = df.columns[0]

    numeric_kinds = {"numeric", "numeric_text", "percentage", "percentage_text"}
    pedido_columns = [col for col in df.columns if "pedid" in col.lower()]
    y_column = next((col for col in pedido_columns if columns_info.get(col, {}).get("kind") in numeric_kinds), None)
    if y_column is None and option_key == "RETURN":
        devol_columns = [col for col in df.columns if "devol" in col.lower()]
        y_column = next((col for col in devol_columns if columns_info.get(col, {}).get("kind") in numeric_kinds), None)
    if y_column is None:
        y_column = next(
            (col for col, meta in columns_info.items() if meta.get("kind") in numeric_kinds and col != x_column),
            None,
        )
    if y_column is None:
        print(f"Aba '{sheet_name}': não há coluna numérica disponível para o eixo Y.")
        return None

    y_label = "Pedidos" if "pedid" in y_column.lower() else y_column
    if option_key == "RETURN" and "devol" in y_column.lower():
        y_label = "Pedidos devolvidos"

    return {
        "chart_type": "column",
        "x_column": x_column,
        "primary_series": [{"column": y_column, "label": y_label}],
        "secondary_series": [],
        "title": f"{sheet_name} - {y_label}",
        "x_axis_label": x_column.upper(),
        "y_axis_label": y_label,
        "y2_axis_label": "",
    }


def _build_custom_chart_config(
    sheet_name: str,
    df: pd.DataFrame,
    columns_info: Dict[str, Dict[str, str]],
) -> Optional[Dict[str, object]]:
    chart_option = _prompt_chart_type()
    if chart_option is None:
        return None

    columns = list(df.columns)
    _display_columns(columns, columns_info)

    x_selection = _prompt_column_selection(
        columns,
        columns_info,
        "Selecione a coluna para o eixo X (número ou nome): ",
        allow_multiple=False,
    )
    if not x_selection:
        return None
    x_column = x_selection[0]

    numeric_kinds = {"numeric", "numeric_text", "percentage", "percentage_text"}
    primary_selection = _prompt_column_selection(
        columns,
        columns_info,
        "Informe as colunas para o eixo Y principal (separe por vírgula): ",
        allow_multiple=chart_option.get("allow_multiple_primary", True),
        allowed_kinds=numeric_kinds,
    )
    if not primary_selection:
        print("É necessário informar ao menos uma coluna numérica para o eixo Y.")
        return None

    secondary_selection: List[str] = []
    if chart_option.get("supports_secondary"):
        if _prompt_yes_no("Deseja informar colunas para o eixo Y secundário? (s/n): "):
            secondary_selection = _prompt_column_selection(
                columns,
                columns_info,
                "Informe as colunas para o eixo secundário (separe por vírgula): ",
                allow_multiple=True,
                allowed_kinds=numeric_kinds,
            )
            if not secondary_selection:
                print("Nenhuma coluna válida selecionada para o eixo secundário.")

    primary_series = [{"column": col, "label": col} for col in primary_selection]
    secondary_series = [{"column": col, "label": col} for col in secondary_selection]
    if chart_option.get("secondary_type") and secondary_series:
        for series in secondary_series:
            series["chart_type"] = chart_option["secondary_type"]

    default_title = f"{sheet_name} - {chart_option['label']}"
    default_y_label = primary_selection[0]
    default_y2_label = secondary_selection[0] if secondary_selection else ""

    if _prompt_yes_no("Deseja definir título e rótulos personalizados? (s/n): "):
        title = input("Título (Enter para padrão): ").strip() or default_title
        x_axis_label = input("Rótulo do eixo X (Enter para usar o nome da coluna): ").strip() or x_column.upper()
        y_axis_label = input("Rótulo do eixo Y (Enter para usar o nome da coluna): ").strip() or default_y_label
        y2_axis_label = ""
        if secondary_series:
            y2_axis_label = input("Rótulo do eixo Y secundário (Enter para padrão): ").strip() or default_y2_label
    else:
        title = default_title
        x_axis_label = x_column.upper()
        y_axis_label = default_y_label
        y2_axis_label = default_y2_label

    return {
        "chart_type": chart_option["chart_type"],
        "secondary_chart_type": chart_option.get("secondary_type"),
        "x_column": x_column,
        "primary_series": primary_series,
        "secondary_series": secondary_series,
        "title": title,
        "x_axis_label": x_axis_label,
        "y_axis_label": y_axis_label,
        "y2_axis_label": y2_axis_label,
    }


def _prompt_chart_type() -> Optional[Dict[str, object]]:
    options = {
        "1": {"label": "Colunas", "chart_type": "column", "allow_multiple_primary": True, "supports_secondary": False},
        "2": {"label": "Linhas", "chart_type": "line", "allow_multiple_primary": True, "supports_secondary": False},
        "3": {"label": "Pizza", "chart_type": "pie", "allow_multiple_primary": False, "supports_secondary": False},
        "4": {"label": "Barras", "chart_type": "bar", "allow_multiple_primary": True, "supports_secondary": False},
        "5": {"label": "Área", "chart_type": "area", "allow_multiple_primary": True, "supports_secondary": False},
        "6": {"label": "Combinação (coluna + linha)", "chart_type": "column", "allow_multiple_primary": True, "supports_secondary": True, "secondary_type": "line"},
    }
    print("\nSelecione o tipo de gráfico (alguns tipos avançados como histograma ou funil não são suportados pelo mecanismo atual):")
    for key, cfg in options.items():
        print(f" {key}. {cfg['label']}")
    while True:
        choice = input("Tipo de gráfico: ").strip()
        if choice in options:
            return options[choice]
        print("Escolha uma opção válida.")


def _display_columns(columns: List[str], columns_info: Dict[str, Dict[str, str]]) -> None:
    print("\nColunas disponíveis:")
    for idx, column in enumerate(columns, start=1):
        kind = columns_info.get(column, {}).get("kind", "desconhecido")
        print(f" {idx}. {column} ({_describe_column_kind(kind)})")


def _describe_column_kind(kind: str) -> str:
    mapping = {
        "numeric": "numérico",
        "numeric_text": "texto numérico",
        "percentage": "percentual",
        "percentage_text": "percentual (texto)",
        "date": "data",
        "text": "texto",
    }
    return mapping.get(kind, kind)


def _prompt_column_selection(
    columns: List[str],
    columns_info: Dict[str, Dict[str, str]],
    message: str,
    *,
    allow_multiple: bool,
    allowed_kinds: Optional[Set[str]] = None,
) -> List[str]:
    while True:
        raw = input(message).strip()
        if not raw:
            return []
        selections = [item.strip() for item in raw.split(",") if item.strip()]
        resolved: List[str] = []
        invalid_choice = False
        for selection in selections:
            column = _resolve_column_name(selection, columns)
            if column is None:
                print(f"Coluna '{selection}' não encontrada.")
                invalid_choice = True
                break
            kind = columns_info.get(column, {}).get("kind")
            if allowed_kinds and kind not in allowed_kinds:
                print(f"Coluna '{column}' não é numérica e não pode ser usada neste eixo.")
                invalid_choice = True
                break
            if column not in resolved:
                resolved.append(column)
        if invalid_choice:
            continue
        if not allow_multiple and len(resolved) > 1:
            print("Selecione apenas uma coluna para este eixo.")
            continue
        return resolved


def _resolve_column_name(selection: str, columns: List[str]) -> Optional[str]:
    if selection.isdigit():
        index = int(selection) - 1
        if 0 <= index < len(columns):
            return columns[index]
        return None
    matches = [column for column in columns if column.lower() == selection.lower()]
    if len(matches) == 1:
        return matches[0]
    return None


def _compute_historical_lowest_prices(df: pd.DataFrame) -> Dict[str, float]:
    valid = df.loc[df["preco_vendido"].notna()]
    return (
        valid.groupby("cd_produto")["preco_vendido"].min()
        .round(2)
        .to_dict()
    )


def _prompt_period_range(df: pd.DataFrame) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    base_series = df.get("data", pd.Series(dtype="datetime64[ns]"))
    available_dates = pd.to_datetime(base_series, errors="coerce")
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
        return df.copy()
    start, end = period_range
    if "data" in df.columns:
        data_series = pd.to_datetime(df["data"], errors="coerce")
        mask = data_series.between(start, end)
        return df.loc[mask].copy()
    mask = df["periodo"].between(start.to_period("M"), end.to_period("M"))
    return df.loc[mask].copy()


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
