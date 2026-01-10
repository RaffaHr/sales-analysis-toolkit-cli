from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Optional
import shlex

import pandas as pd

from ..config import AppConfig
from ..data.loader import DatasetManager
from ..services.analysis_runner import AnalysisResponse, initialize_registry


@dataclass(frozen=True)
class CommandResult:
    reply: str
    tables: Dict[str, pd.DataFrame] = field(default_factory=dict)
    metadata: Dict[str, object] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass(frozen=True)
class CommandContext:
    config: AppConfig
    dataset_manager: DatasetManager


@dataclass(frozen=True)
class CommandSpec:
    name: str
    description: str
    usage: str


COMMAND_SPECS: Dict[str, CommandSpec] = {
    "ajuda": CommandSpec(
        name="ajuda",
        description="Lista os comandos disponíveis e exemplos de uso.",
        usage="/ajuda",
    ),
    "analise_devolucao": CommandSpec(
        name="analise_devolucao",
        description="Gera as planilhas de devolução filtradas pelo período/categoria.",
        usage="/analise_devolucao categoria=Eletronicos inicio=2024-01-01 fim=2024-06-30",
    ),
    "analise_potencial": CommandSpec(
        name="analise_potencial",
        description="Destaca anúncios com queda recente e histórico forte.",
        usage="/analise_potencial categoria=Eletronicos rank=30 janela=3",
    ),
    "analise_top": CommandSpec(
        name="analise_top",
        description="Ranking dos SKUs com melhor recorrência histórica.",
        usage="/analise_top categoria=Games rank=50",
    ),
    "analise_reputacao": CommandSpec(
        name="analise_reputacao",
        description="Lista itens baratos com baixa devolução para fortalecer reputação.",
        usage="/analise_reputacao categoria=Utilidades",
    ),
    "analise_focus": CommandSpec(
        name="analise_focus",
        description="Resumo de performance por categoria ou lista de anúncios.",
        usage="/analise_focus categoria=Eletronicos inicio=2025-01-01 fim=2025-03-31",
    ),
}


def dispatch_command(message: str, context: CommandContext) -> CommandResult:
    parsed = _parse_command(message)
    if parsed is None:
        return CommandResult(reply="", error="Comando inválido. Tente /ajuda para ver exemplos.")

    command_name, args = parsed
    if command_name == "ajuda":
        specs = [spec for spec in list_available_commands() if spec.name != "ajuda"]
        lines = [f"/{spec.name} — {spec.description}" for spec in specs]
        joined = "\n".join(lines) or "Nenhum comando disponível."
        return CommandResult(reply=f"Comandos disponíveis:\n{joined}")

    registry = initialize_registry(
        default_rank=context.config.default_rank_size,
        default_recent_window=context.config.default_recent_window,
    )
    handler = registry.get(command_name)
    if handler is None:
        return CommandResult(reply="", error="Comando não reconhecido. Use /ajuda para listar opções.")

    response: AnalysisResponse
    try:
        response = handler(context.dataset_manager, args)
    except Exception as exc:  # noqa: BLE001
        return CommandResult(reply="", error=f"Erro ao executar o comando: {exc}")

    return CommandResult(
        reply=response.message,
        tables=response.tables,
        metadata=response.metadata,
    )


def _parse_command(message: str) -> tuple[str, Dict[str, str]] | None:
    if not message.startswith("/"):
        return None
    try:
        tokens = shlex.split(message)
    except ValueError:
        return None
    if not tokens:
        return None
    command_token = tokens[0][1:]
    args: Dict[str, str] = {}
    for token in tokens[1:]:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        args[key.strip().lower()] = value.strip()
    return command_token.lower(), args


def list_available_commands() -> list[CommandSpec]:
    return list(COMMAND_SPECS.values())
