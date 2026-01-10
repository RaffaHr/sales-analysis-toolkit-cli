from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Optional
import pandas as pd

from analysis.reporting.low_cost import build_low_cost_reputation_analysis
from analysis.reporting.potential import build_potential_sku_analysis
from analysis.reporting.product_focus import build_product_focus_analysis
from analysis.reporting.returns import build_return_analysis
from analysis.reporting.top_history import build_top_history_analysis

from ..data.loader import DatasetManager, PeriodFilter


@dataclass(frozen=True)
class AnalysisResponse:
    message: str
    tables: Dict[str, pd.DataFrame]
    metadata: Dict[str, object]


def _parse_period_filter(args: Dict[str, str]) -> PeriodFilter:
    start = _parse_date(args.get("inicio"))
    end = _parse_date(args.get("fim"))
    if start is not None and end is not None and start > end:
        start, end = end, start
    return PeriodFilter(start=start, end=end)


def _parse_date(raw: Optional[str]) -> Optional[pd.Timestamp]:
    if not raw:
        return None
    try:
        parsed = pd.to_datetime(raw, dayfirst=True, errors="raise")
    except (ValueError, TypeError):
        return None
    return parsed.normalize()


def _parse_rank(args: Dict[str, str], default: int) -> int:
    raw = args.get("rank") or args.get("top")
    if raw and raw.isdigit():
        return max(1, int(raw))
    return default


def _parse_recent_window(args: Dict[str, str], default: int) -> int:
    raw = args.get("janela") or args.get("janela_recente")
    if raw and raw.isdigit():
        return max(1, int(raw))
    return default


def _parse_recent_periods(args: Dict[str, str]) -> Optional[list[str]]:
    raw = args.get("periodos_recentes")
    if not raw:
        return None
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    valid: list[str] = []
    for part in parts:
        try:
            period = pd.Period(part, freq="M")
            valid.append(period.strftime("%Y-%m"))
        except (ValueError, TypeError):
            continue
    return valid or None


def run_return_analysis(dataset: DatasetManager, args: Dict[str, str]) -> AnalysisResponse:
    category = args.get("categoria")
    period_filter = _parse_period_filter(args)
    df_period = dataset.load_filtered(period_filter)
    tables = build_return_analysis(df_period, category)
    message = "Análise de devolução concluída. As tabelas refletem o filtro aplicado."
    return AnalysisResponse(message=message, tables=tables, metadata={"categoria": category})


def run_potential_analysis(
    dataset: DatasetManager,
    args: Dict[str, str],
    *,
    default_rank: int,
    default_recent_window: int,
) -> AnalysisResponse:
    category = args.get("categoria")
    rank = _parse_rank(args, default_rank)
    recent_window = _parse_recent_window(args, default_recent_window)
    recent_periods = _parse_recent_periods(args)
    period_filter = _parse_period_filter(args)
    df_period = dataset.load_filtered(period_filter)
    tables = build_potential_sku_analysis(
        df_period,
        category,
        rank_size=rank,
        historical_prices=dataset.historical_prices(),
        recent_periods=recent_periods,
        recent_window=recent_window,
    )
    message = (
        "Análise de potenciais finalizada. Ajuste os parâmetros de rank e janela recente conforme necessário."
    )
    metadata = {
        "categoria": category,
        "rank": rank,
        "janela_recente": recent_window,
        "periodos_recentes": recent_periods,
    }
    return AnalysisResponse(message=message, tables=tables, metadata=metadata)


def run_top_history_analysis(
    dataset: DatasetManager,
    args: Dict[str, str],
    *,
    default_rank: int,
) -> AnalysisResponse:
    category = args.get("categoria")
    rank = _parse_rank(args, default_rank)
    period_filter = _parse_period_filter(args)
    df_period = dataset.load_filtered(period_filter)
    tables = build_top_history_analysis(
        df_period,
        category,
        rank_size=rank,
        historical_prices=dataset.historical_prices(),
    )
    message = "Ranking histórico gerado com sucesso."
    metadata = {"categoria": category, "rank": rank}
    return AnalysisResponse(message=message, tables=tables, metadata=metadata)


def run_low_cost_analysis(dataset: DatasetManager, args: Dict[str, str]) -> AnalysisResponse:
    category = args.get("categoria")
    period_filter = _parse_period_filter(args)
    df_period = dataset.load_filtered(period_filter)
    tables = build_low_cost_reputation_analysis(
        df_period,
        category,
        historical_prices=dataset.historical_prices(),
    )
    message = "Produtos de baixo custo calculados. Use os filtros para refinar o portfólio."
    metadata = {"categoria": category}
    return AnalysisResponse(message=message, tables=tables, metadata=metadata)


def run_product_focus_analysis(dataset: DatasetManager, args: Dict[str, str]) -> AnalysisResponse:
    category = args.get("categoria")
    raw_codes = args.get("codigos") or args.get("cd_anuncio")
    product_codes: Optional[list[str]] = None
    if raw_codes:
        product_codes = [code.strip() for code in raw_codes.split(",") if code.strip()]
    period_filter = _parse_period_filter(args)
    df_period = dataset.load_filtered(period_filter)
    tables = build_product_focus_analysis(
        df_period,
        category,
        product_codes=product_codes,
    )
    message = "Resumo de performance gerado."
    metadata = {"categoria": category, "product_codes": product_codes}
    return AnalysisResponse(message=message, tables=tables, metadata=metadata)


COMMAND_REGISTRY: Dict[str, Callable[[DatasetManager, Dict[str, str]], AnalysisResponse]] = {}


def initialize_registry(
    *,
    default_rank: int,
    default_recent_window: int,
) -> Dict[str, Callable[[DatasetManager, Dict[str, str]], AnalysisResponse]]:
    if COMMAND_REGISTRY:
        return COMMAND_REGISTRY

    COMMAND_REGISTRY.update(
        {
            "analise_devolucao": run_return_analysis,
            "analise_potencial": lambda dataset, args: run_potential_analysis(
                dataset,
                args,
                default_rank=default_rank,
                default_recent_window=default_recent_window,
            ),
            "analise_top": lambda dataset, args: run_top_history_analysis(
                dataset,
                args,
                default_rank=default_rank,
            ),
            "analise_reputacao": run_low_cost_analysis,
            "analise_focus": run_product_focus_analysis,
        }
    )
    return COMMAND_REGISTRY
