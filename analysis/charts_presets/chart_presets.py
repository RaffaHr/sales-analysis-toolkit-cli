from __future__ import annotations

import json
import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

DEFAULT_PRESET_FILE = Path("chart_presets.json")
ENV_PRESET_FILE = "CHART_PRESETS_FILE"
SUPPORTED_VERSION = 1


class ChartPresetError(RuntimeError):
    """Erro ao interpretar os presets de gráficos."""


@dataclass(slots=True)
class SeriesConfig:
    column: str
    label: Optional[str] = None
    chart_type: Optional[str] = None

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "SeriesConfig":
        if "column" not in payload:
            raise ChartPresetError("Cada série precisa informar a chave 'column'.")
        column = str(payload["column"]).strip()
        if not column:
            raise ChartPresetError("Nome de coluna da série não pode ser vazio.")
        label = payload.get("label")
        label = str(label).strip() if label else None
        chart_type = payload.get("chart_type")
        chart_type = str(chart_type).strip() if chart_type else None
        return cls(column=column, label=label, chart_type=chart_type)

    def to_payload(self) -> Dict[str, str]:
        data: Dict[str, str] = {"column": self.column}
        if self.label:
            data["label"] = self.label
        if self.chart_type:
            data["chart_type"] = self.chart_type
        return data

    def required_columns(self) -> set[str]:
        return {self.column}


@dataclass(slots=True)
class ChartPreset:
    key: str
    label: str
    chart_type: str
    x_column: str
    primary_series: List[SeriesConfig] = field(default_factory=list)
    secondary_series: List[SeriesConfig] = field(default_factory=list)
    filter_column: Optional[str] = None
    dropdown_label: Optional[str] = None
    title: Optional[str] = None
    x_axis_label: Optional[str] = None
    y_axis_label: Optional[str] = None
    y2_axis_label: Optional[str] = None
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ChartPreset":
        required = {"key", "label", "chart_type", "x_column"}
        missing = required - payload.keys()
        if missing:
            raise ChartPresetError(f"Preset sem os campos obrigatórios: {', '.join(sorted(missing))}")

        primary_payload = payload.get("primary_series") or []
        secondary_payload = payload.get("secondary_series") or []
        if not isinstance(primary_payload, list) or not primary_payload:
            raise ChartPresetError("'primary_series' precisa ser uma lista com ao menos uma série.")
        primary = [SeriesConfig.from_dict(item) for item in primary_payload]
        secondary = [SeriesConfig.from_dict(item) for item in secondary_payload]

        preset = cls(
            key=str(payload["key"]).strip(),
            label=str(payload["label"]).strip(),
            chart_type=str(payload["chart_type"]).strip(),
            x_column=str(payload["x_column"]).strip(),
            primary_series=primary,
            secondary_series=secondary,
            filter_column=str(payload.get("filter_column", "")).strip() or None,
            dropdown_label=str(payload.get("dropdown_label", "")).strip() or None,
            title=str(payload.get("title", "")).strip() or None,
            x_axis_label=str(payload.get("x_axis_label", "")).strip() or None,
            y_axis_label=str(payload.get("y_axis_label", "")).strip() or None,
            y2_axis_label=str(payload.get("y2_axis_label", "")).strip() or None,
            description=str(payload.get("description", "")).strip() or None,
        )

        if not preset.key:
            raise ChartPresetError("O campo 'key' não pode ser vazio.")
        if not preset.label:
            raise ChartPresetError("O campo 'label' não pode ser vazio.")
        if not preset.chart_type:
            raise ChartPresetError("O campo 'chart_type' não pode ser vazio.")
        if not preset.x_column:
            raise ChartPresetError("O campo 'x_column' não pode ser vazio.")

        return preset

    def required_columns(self) -> set[str]:
        required = {self.x_column}
        required.update(series.column for series in self.primary_series)
        required.update(series.column for series in self.secondary_series)
        if self.filter_column:
            required.add(self.filter_column)
        return required

    def is_applicable(self, available_columns: Iterable[str]) -> bool:
        available = {str(column) for column in available_columns}
        return self.required_columns().issubset(available)

    def to_chart_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "chart_type": self.chart_type,
            "x_column": self.x_column,
            "primary_series": [series.to_payload() for series in self.primary_series],
            "secondary_series": [series.to_payload() for series in self.secondary_series],
        }
        if self.filter_column:
            payload["filter_column"] = self.filter_column
        if self.dropdown_label:
            payload["dropdown_label"] = self.dropdown_label
        if self.title:
            payload["title"] = self.title
        if self.x_axis_label:
            payload["x_axis_label"] = self.x_axis_label
        if self.y_axis_label:
            payload["y_axis_label"] = self.y_axis_label
        if self.y2_axis_label:
            payload["y2_axis_label"] = self.y2_axis_label
        return payload


@dataclass(slots=True)
class ChartPresetBinding:
    preset: ChartPreset
    is_default: bool = False


class ChartPresetCatalog:
    """Agrupa presets por análise e aba, facilitando validação e consulta."""

    def __init__(
        self,
        bindings: Dict[Tuple[str, str], List[ChartPresetBinding]],
        presets: Dict[str, ChartPreset],
        source: Optional[Path] = None,
    ) -> None:
        self._bindings = bindings
        self._presets = presets
        self._source = source

    @property
    def source(self) -> Optional[Path]:
        return self._source

    def presets_for(self, analysis: str, sheet: str) -> List[ChartPreset]:
        key = (analysis.upper(), sheet)
        return [binding.preset for binding in self._bindings.get(key, ())]

    def bindings_for(self, analysis: str, sheet: str) -> List[ChartPresetBinding]:
        key = (analysis.upper(), sheet)
        return list(self._bindings.get(key, ()))

    def applicable_presets(
        self,
        analysis: str,
        sheet: str,
        available_columns: Iterable[str],
    ) -> List[ChartPreset]:
        available_set = list(available_columns)
        return [
            binding.preset
            for binding in self.bindings_for(analysis, sheet)
            if binding.preset.is_applicable(available_set)
        ]

    def default_preset(
        self,
        analysis: str,
        sheet: str,
        available_columns: Iterable[str],
    ) -> Optional[ChartPreset]:
        available_set = list(available_columns)
        bindings = self.bindings_for(analysis, sheet)
        for binding in bindings:
            if binding.is_default and binding.preset.is_applicable(available_set):
                return binding.preset
        for binding in bindings:
            if binding.preset.is_applicable(available_set):
                return binding.preset
        return None

    def is_empty(self) -> bool:
        return not bool(self._bindings)

    def all_presets(self) -> Dict[str, ChartPreset]:
        return dict(self._presets)


def _resolve_preset_path(custom_path: Optional[Path] = None) -> Optional[Path]:
    if custom_path is not None:
        return custom_path
    env_path = os.getenv(ENV_PRESET_FILE)
    if env_path:
        return Path(env_path)
    return DEFAULT_PRESET_FILE


def load_chart_presets(path: Optional[Path] = None) -> ChartPresetCatalog:
    preset_path = _resolve_preset_path(path)
    if preset_path is None or not Path(preset_path).is_file():
        return ChartPresetCatalog({}, {}, source=preset_path)

    try:
        with open(preset_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return ChartPresetCatalog({}, {}, source=preset_path)
    except json.JSONDecodeError as exc:
        warnings.warn(f"Não foi possível ler '{preset_path}': {exc}")
        return ChartPresetCatalog({}, {}, source=preset_path)

    version = data.get("version", SUPPORTED_VERSION)
    if version != SUPPORTED_VERSION:
        warnings.warn(
            f"Versão de preset não suportada ({version}). Esperado: {SUPPORTED_VERSION}. Presets serão ignorados."
        )
        return ChartPresetCatalog({}, {}, source=preset_path)

    raw_presets = data.get("presets", [])
    if not isinstance(raw_presets, list):
        warnings.warn("Estrutura de presets inválida: 'presets' deve ser uma lista.")
        return ChartPresetCatalog({}, {}, source=preset_path)

    presets: Dict[str, ChartPreset] = {}
    for index, entry in enumerate(raw_presets):
        if not isinstance(entry, dict):
            warnings.warn(f"Preset na posição {index} ignorado por não ser um objeto.")
            continue
        try:
            preset = ChartPreset.from_dict(entry)
        except ChartPresetError as exc:
            warnings.warn(f"Preset ignorado por conteudo inválido: {exc}")
            continue
        if preset.key in presets:
            warnings.warn(f"Preset com chave duplicada '{preset.key}' ignorado.")
            continue
        presets[preset.key] = preset

    page_entries = data.get("pages", [])
    if page_entries is None:
        page_entries = []
    if not isinstance(page_entries, list):
        warnings.warn("Estrutura de páginas inválida: 'pages' deve ser uma lista.")
        page_entries = []

    bindings: Dict[Tuple[str, str], List[ChartPresetBinding]] = {}
    for index, page in enumerate(page_entries):
        if not isinstance(page, dict):
            warnings.warn(f"Entrada de página na posição {index} ignorada por não ser um objeto.")
            continue
        analysis = str(page.get("analysis", "")).strip().upper()
        sheet = str(page.get("sheet", "")).strip()
        if not analysis or not sheet:
            warnings.warn("Entrada de página ignorada: 'analysis' e 'sheet' são obrigatórios.")
            continue

        raw_preset_keys = page.get("presets", [])
        if raw_preset_keys is None:
            raw_preset_keys = []
        if isinstance(raw_preset_keys, str):
            raw_preset_keys = [raw_preset_keys]
        if not isinstance(raw_preset_keys, list):
            warnings.warn(
                f"Página ({analysis}/{sheet}) ignorou a chave 'presets' por não ser lista nem string."
            )
            raw_preset_keys = []

        preset_keys = [str(key).strip() for key in raw_preset_keys if str(key).strip()]

        default_key = page.get("default")
        default_key = str(default_key).strip() if default_key else None
        if default_key and default_key not in preset_keys:
            preset_keys.append(default_key)

        if not preset_keys:
            warnings.warn(
                f"Página ({analysis}/{sheet}) não possui presets associados e será ignorada."
            )
            continue

        page_bindings: List[ChartPresetBinding] = []
        for preset_key in preset_keys:
            preset = presets.get(preset_key)
            if not preset:
                warnings.warn(
                    f"Página ({analysis}/{sheet}) faz referência ao preset desconhecido '{preset_key}'."
                )
                continue
            page_bindings.append(
                ChartPresetBinding(
                    preset=preset,
                    is_default=default_key is not None and preset_key == default_key,
                )
            )

        if default_key and not any(binding.is_default for binding in page_bindings):
            warnings.warn(
                f"Página ({analysis}/{sheet}) possui default '{default_key}' não encontrado entre os presets válidos."
            )

        if page_bindings:
            bindings.setdefault((analysis, sheet), []).extend(page_bindings)

    return ChartPresetCatalog(bindings, presets, source=preset_path)