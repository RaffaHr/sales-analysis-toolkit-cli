from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from xlsxwriter.utility import xl_rowcol_to_cell, xl_col_to_name

DEFAULT_OUTPUT_DIR = Path("output")


def export_to_excel(dataframes: Dict[str, pd.DataFrame], base_name: str, output_dir: Path | str = DEFAULT_OUTPUT_DIR) -> Path:
    """Gera um arquivo Excel novo reunindo os DataFrames fornecidos."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    payload = dict(dataframes)
    charts = payload.pop("__charts__", []) if "__charts__" in payload else []
    used_table_names: set[str] = set()

    safe_base = base_name.replace(" ", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_path / f"{safe_base}_{timestamp}.xlsx"

    with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
        workbook = writer.book
        sheet_name_map: Dict[str, str] = {}
        table_name_map: Dict[str, str] = {}
        chart_context: Dict[str, Any] = {
            "helper_sheet": None,
            "helper_sheet_name": None,
            "helper_row": 0,
            "formats": {},
        }

        for sheet_name, df in payload.items():
            sanitized_sheet = sheet_name[:31]
            sheet_name_map[sheet_name] = sanitized_sheet
            df.to_excel(writer, sheet_name=sanitized_sheet, index=False)
            worksheet = writer.sheets[sanitized_sheet]
            table_name = _unique_table_name(sanitized_sheet, used_table_names)
            _add_table_layout(worksheet, df, table_name)
            table_name_map[sheet_name] = table_name

        for chart in charts:
            _handle_chart(
                workbook,
                writer.sheets,
                sheet_name_map,
                table_name_map,
                chart,
                used_table_names,
                chart_context,
            )

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


def _handle_chart(
    workbook,
    worksheets: Dict[str, object],
    sheet_map: Dict[str, str],
    table_map: Dict[str, str],
    chart_spec: Dict[str, object],
    used_table_names: set[str],
    context: Dict[str, Any],
) -> None:
    if not isinstance(chart_spec, dict):
        return
    _add_dynamic_chart(
        workbook,
        worksheets,
        sheet_map,
        table_map,
        chart_spec,
        used_table_names,
        context,
    )


def _add_dynamic_chart(
    workbook,
    worksheets: Dict[str, object],
    sheet_map: Dict[str, str],
    table_map: Dict[str, str],
    chart_spec: Dict[str, object],
    used_table_names: set[str],
    context: Dict[str, Any],
) -> None:
    target_sheet_key = chart_spec.get("sheet")
    df: pd.DataFrame | None = chart_spec.get("dataframe")
    if not target_sheet_key or df is None or df.empty:
        return

    target_sheet_name = sheet_map.get(target_sheet_key, target_sheet_key[:31])
    target_ws = worksheets.get(target_sheet_name)
    if target_ws is None:
        return

    table_name = table_map.get(target_sheet_key)
    if not table_name:
        return

    filter_column = chart_spec.get("filter_column")
    if filter_column not in df.columns:
        return

    unique_values = sorted({str(val).strip() for val in df[filter_column].dropna().astype(str) if str(val).strip()})
    if not unique_values:
        return

    dropdown_range_name = _register_dropdown_values(workbook, unique_values, used_table_names, context)
    dropdown_label = chart_spec.get("dropdown_label", "Selecione o produto:")

    last_used_col = target_ws.dim_colmax if target_ws.dim_colmax is not None else -1
    helper_col = last_used_col + 3
    label_row = 0
    dropdown_col = helper_col + 1

    target_ws.write(label_row, helper_col, dropdown_label)
    target_ws.write(label_row, dropdown_col, unique_values[0])
    target_ws.data_validation(
        label_row,
        dropdown_col,
        label_row,
        dropdown_col,
        {
            "validate": "list",
            "source": f"={dropdown_range_name}",
        },
    )

    columns_info: Dict[str, Dict[str, Any]] = chart_spec.get("columns_info", {})
    x_column: str = chart_spec.get("x_column")
    primary_series: List[Dict[str, str]] = chart_spec.get("primary_series", [])
    secondary_series: List[Dict[str, str]] = chart_spec.get("secondary_series", [])

    data_header_row = label_row + 2
    formula_row = data_header_row + 1
    current_col = helper_col

    column_positions: Dict[str, int] = {}

    # X axis column
    target_ws.write(data_header_row, current_col, x_column.upper())
    dropdown_ref = xl_rowcol_to_cell(label_row, dropdown_col, row_abs=True, col_abs=True)
    formula = _build_filtered_formula(table_name, x_column, filter_column, dropdown_ref, columns_info.get(x_column, {}))
    target_ws.write_dynamic_array_formula(formula_row, current_col, formula_row, current_col, formula)
    _apply_column_format(workbook, target_ws, current_col, columns_info.get(x_column, {}))
    column_positions[x_column] = current_col
    current_col += 1

    # Primary series
    for series in primary_series:
        col_name = series["column"]
        label = series.get("label", col_name)
        target_ws.write(data_header_row, current_col, label.upper())
        formula = _build_filtered_formula(table_name, col_name, filter_column, dropdown_ref, columns_info.get(col_name, {}))
        target_ws.write_dynamic_array_formula(formula_row, current_col, formula_row, current_col, formula)
        _apply_column_format(workbook, target_ws, current_col, columns_info.get(col_name, {}))
        column_positions[("primary", col_name)] = current_col
        current_col += 1

    # Secondary series (if any)
    for series in secondary_series:
        col_name = series["column"]
        label = series.get("label", col_name)
        target_ws.write(data_header_row, current_col, label.upper())
        formula = _build_filtered_formula(table_name, col_name, filter_column, dropdown_ref, columns_info.get(col_name, {}))
        target_ws.write_dynamic_array_formula(formula_row, current_col, formula_row, current_col, formula)
        _apply_column_format(workbook, target_ws, current_col, columns_info.get(col_name, {}))
        column_positions[("secondary", col_name)] = current_col
        current_col += 1

    chart_type = chart_spec.get("chart_type", "column")
    chart = workbook.add_chart({"type": chart_type})
    chart_title = chart_spec.get("title") or f"{target_sheet_key}"
    chart.set_title({"name": chart_title})

    x_axis_label = chart_spec.get("x_axis_label", x_column.upper())
    y_axis_label = chart_spec.get("y_axis_label", "Valores")
    y2_axis_label = chart_spec.get("y2_axis_label", "")

    if chart_type not in {"pie", "doughnut"}:
        x_axis_options = {"name": x_axis_label}
        if columns_info.get(x_column, {}).get("kind") == "date":
            x_axis_options["num_format"] = "dd/mm/yyyy"
            x_axis_options["date_axis"] = True
        chart.set_x_axis(x_axis_options)
        chart.set_y_axis({"name": y_axis_label})
        if secondary_series:
            chart.set_y2_axis({"name": y2_axis_label or "Serie secundária"})

    safe_sheet_name = target_sheet_name.replace("'", "''")
    categories_range = _build_series_range(safe_sheet_name, formula_row, column_positions[x_column])

    for series in primary_series:
        col_name = series["column"]
        series_label = series.get("label", col_name)
        value_col = column_positions.get(("primary", col_name))
        if value_col is None:
            continue
        value_range = _build_series_range(safe_sheet_name, formula_row, value_col)
        series_options = {
            "name": series_label,
            "categories": categories_range,
            "values": value_range,
        }
        specific_type = series.get("chart_type")
        if specific_type:
            series_options["chart_type"] = specific_type
        chart.add_series(series_options)

    secondary_chart_type = chart_spec.get("secondary_chart_type")
    for series in secondary_series:
        col_name = series["column"]
        series_label = series.get("label", col_name)
        value_col = column_positions.get(("secondary", col_name))
        if value_col is None:
            continue
        value_range = _build_series_range(safe_sheet_name, formula_row, value_col)
        series_options = {
            "name": series_label,
            "values": value_range,
            "y2_axis": True,
        }
        series_options["categories"] = categories_range
        specific_type = series.get("chart_type") or secondary_chart_type
        if specific_type:
            series_options["chart_type"] = specific_type
        chart.add_series(series_options)

    chart.set_style(10)
    chart.set_legend({"position": "bottom"})

    chart_row = data_header_row
    chart_col = current_col + 1
    chart.set_size({"width": 760, "height": 420})
    target_ws.insert_chart(chart_row, chart_col, chart, {"x_offset": 15, "y_offset": 10})


def _unique_sheet_name(workbook, base: str) -> str:
    existing = {ws.get_name() for ws in workbook.worksheets()}
    candidate = base[:31]
    counter = 1
    while candidate in existing:
        suffix = f"_{counter}"
        candidate = f"{base[:31 - len(suffix)]}{suffix}"
        counter += 1
    return candidate


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


def _register_dropdown_values(
    workbook,
    values: List[str],
    used_table_names: set[str],
    context: Dict[str, Any],
) -> str:
    helper_sheet = _ensure_helper_sheet(workbook, context)
    helper_name = context["helper_sheet_name"]
    start_row = context.get("helper_row", 0)
    for offset, value in enumerate(values):
        helper_sheet.write(start_row + offset, 0, value)
    first_cell = xl_rowcol_to_cell(start_row, 0, row_abs=True, col_abs=True)
    last_cell = xl_rowcol_to_cell(start_row + len(values) - 1, 0, row_abs=True, col_abs=True)
    range_name = _unique_table_name("dropdown_lista", used_table_names)
    workbook.define_name(range_name, f"='{helper_name}'!{first_cell}:{last_cell}")
    context["helper_row"] = start_row + len(values) + 1
    return range_name


def _ensure_helper_sheet(workbook, context: Dict[str, Any]):
    sheet = context.get("helper_sheet")
    if sheet is None:
        helper_name = _unique_sheet_name(workbook, "chart_helper")
        sheet = workbook.add_worksheet(helper_name)
        sheet.hide()
        context["helper_sheet"] = sheet
        context["helper_sheet_name"] = helper_name
        context["helper_row"] = 0
    return sheet


def _structured_reference(table_name: str, column_name: str) -> str:
    safe_column = column_name.replace("]", "]]")
    return f"{table_name}[{safe_column}]"


def _build_series_range(sheet_name: str, start_row: int, col: int) -> str:
    start_row_excel = start_row + 1
    col_letter = xl_col_to_name(col)
    return f"='{sheet_name}'!${col_letter}${start_row_excel}:${col_letter}$1048576"


def _build_filtered_formula(
    table_name: str,
    column_name: str,
    filter_column: str,
    dropdown_ref: str,
    column_info: Dict[str, Any],
) -> str:
    column_ref = _structured_reference(table_name, column_name)
    filter_ref = _structured_reference(table_name, filter_column)
    _ = column_info  # mantido para compatibilidade futura
    return f"=FILTER({column_ref},{filter_ref}={dropdown_ref},\"\")"


def _apply_column_format(workbook, worksheet, column_index: int, column_info: Dict[str, Any]) -> None:
    kind = column_info.get("kind")
    if not hasattr(workbook, "_chart_format_cache"):
        workbook._chart_format_cache = {}
    cache: Dict[str, Any] = workbook._chart_format_cache

    def _get_format(key: str, format_dict: Dict[str, Any]):
        if key not in cache:
            cache[key] = workbook.add_format(format_dict)
        return cache[key]

    if kind == "date":
        fmt = _get_format("date", {"num_format": "dd/mm/yyyy"})
        worksheet.set_column(column_index, column_index, 14, fmt)
    elif kind in {"percentage", "percentage_text"}:
        fmt = _get_format("percent", {"num_format": "0,00%"})
        worksheet.set_column(column_index, column_index, 12, fmt)
    else:
        worksheet.set_column(column_index, column_index, 12)
