"""
Google Sheets Helper Functions

Shared utilities for Google Sheets operations including A1 parsing, color
conversion, data validation rule building, named range resolution, and
conditional formatting helpers.

Most helpers are ported from the upstream `google_workspace_mcp` project so
that future merges stay clean. New helpers added in this repo:
- _build_data_validation_condition
- _resolve_named_range_id
"""

import asyncio
import json
import logging
import re
from typing import List, Optional, Union

logger = logging.getLogger(__name__)


class UserInputError(Exception):
    """Raised when a tool receives invalid user input."""

    pass


MAX_GRID_METADATA_CELLS = 5000

A1_PART_REGEX = re.compile(r"^([A-Za-z]*)(\d*)$")
SHEET_TITLE_SAFE_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _column_to_index(column: str) -> Optional[int]:
    """Convert column letters (A, B, AA) to zero-based index."""
    if not column:
        return None
    result = 0
    for char in column.upper():
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result - 1


def _index_to_column(index: int) -> str:
    """Convert a zero-based column index to column letters (0 -> A, 25 -> Z, 26 -> AA)."""
    if index < 0:
        raise UserInputError(f"Column index must be non-negative, got {index}.")

    result = []
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(ord("A") + remainder))
    return "".join(reversed(result))


def _parse_a1_part(
    part: str, pattern: re.Pattern[str] = A1_PART_REGEX
) -> tuple[Optional[int], Optional[int]]:
    """Parse a single A1 part like 'B2' or 'C' into zero-based column/row indexes."""
    clean_part = part.replace("$", "")
    match = pattern.match(clean_part)
    if not match:
        raise UserInputError(f"Invalid A1 range part: '{part}'.")
    col_letters, row_digits = match.groups()
    col_idx = _column_to_index(col_letters) if col_letters else None
    row_idx = int(row_digits) - 1 if row_digits else None
    return col_idx, row_idx


def _split_sheet_and_range(range_name: str) -> tuple[Optional[str], str]:
    """Split an A1 notation into (sheet_name, range_part), handling quoted sheet names."""
    if "!" not in range_name:
        return None, range_name

    if range_name.startswith("'"):
        closing = range_name.find("'!")
        if closing != -1:
            sheet_name = range_name[1:closing].replace("''", "'")
            a1_range = range_name[closing + 2 :]
            return sheet_name, a1_range

    sheet_name, a1_range = range_name.split("!", 1)
    return sheet_name.strip().strip("'"), a1_range


def _parse_a1_range(range_name: str, sheets: List[dict]) -> dict:
    """Convert an A1-style range (with optional sheet name) into a GridRange."""
    sheet_name, a1_range = _split_sheet_and_range(range_name)

    if not sheets:
        raise UserInputError("Spreadsheet has no sheets.")

    target_sheet = None
    if sheet_name:
        for sheet in sheets:
            if sheet.get("properties", {}).get("title") == sheet_name:
                target_sheet = sheet
                break
        if target_sheet is None:
            available_titles = [
                sheet.get("properties", {}).get("title", "Untitled") for sheet in sheets
            ]
            available_list = ", ".join(available_titles) if available_titles else "none"
            raise UserInputError(
                f"Sheet '{sheet_name}' not found in spreadsheet. Available sheets: {available_list}."
            )
    else:
        target_sheet = sheets[0]

    props = target_sheet.get("properties", {})
    sheet_id = props.get("sheetId")

    if not a1_range:
        raise UserInputError("A1-style range must not be empty (e.g., 'A1', 'A1:B10').")

    if ":" in a1_range:
        start, end = a1_range.split(":", 1)
    else:
        start = end = a1_range

    start_col, start_row = _parse_a1_part(start)
    end_col, end_row = _parse_a1_part(end)

    grid_range = {"sheetId": sheet_id}
    if start_row is not None:
        grid_range["startRowIndex"] = start_row
    if start_col is not None:
        grid_range["startColumnIndex"] = start_col
    if end_row is not None:
        grid_range["endRowIndex"] = end_row + 1
    if end_col is not None:
        grid_range["endColumnIndex"] = end_col + 1

    return grid_range


def _parse_hex_color(color: Optional[str]) -> Optional[dict]:
    """Convert a hex color like '#RRGGBB' to Sheets API color (0-1 floats)."""
    if not color:
        return None

    trimmed = color.strip()
    if trimmed.startswith("#"):
        trimmed = trimmed[1:]

    if len(trimmed) != 6:
        raise UserInputError(f"Color '{color}' must be in format #RRGGBB or RRGGBB.")

    try:
        red = int(trimmed[0:2], 16) / 255
        green = int(trimmed[2:4], 16) / 255
        blue = int(trimmed[4:6], 16) / 255
    except ValueError as exc:
        raise UserInputError(f"Color '{color}' is not valid hex.") from exc

    return {"red": red, "green": green, "blue": blue}


def _color_to_hex(color: Optional[dict]) -> Optional[str]:
    """Convert a Sheets color object back to #RRGGBB hex string for display."""
    if not color:
        return None

    def _component(value: Optional[float]) -> int:
        try:
            return max(0, min(255, int(round(float(value or 0) * 255))))
        except (TypeError, ValueError):
            return 0

    red = _component(color.get("red"))
    green = _component(color.get("green"))
    blue = _component(color.get("blue"))
    return f"#{red:02X}{green:02X}{blue:02X}"


def _quote_sheet_title_for_a1(sheet_title: str) -> str:
    """Quote a sheet title for use in A1 notation if necessary."""
    if SHEET_TITLE_SAFE_RE.match(sheet_title or ""):
        return sheet_title
    escaped = (sheet_title or "").replace("'", "''")
    return f"'{escaped}'"


def _format_a1_cell(sheet_title: str, row_index: int, col_index: int) -> str:
    """Format a cell reference in A1 notation given a sheet title and zero-based indices."""
    return f"{_quote_sheet_title_for_a1(sheet_title)}!{_index_to_column(col_index)}{row_index + 1}"


def _coerce_int(value: object, default: int = 0) -> int:
    """Safely convert a value to an integer, returning a default if conversion fails."""
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _to_extended_value(val) -> dict:
    """Convert a Python value to a Sheets API ExtendedValue dict."""
    if isinstance(val, bool):
        return {"boolValue": val}
    if isinstance(val, (int, float)):
        return {"numberValue": val}
    s = str(val)
    if s.startswith("="):
        return {"formulaValue": s}
    return {"stringValue": s}


def _is_sheets_error_token(value: object) -> bool:
    """Detect whether a cell value represents a Google Sheets error token."""
    if not isinstance(value, str):
        return False
    candidate = value.strip()
    if not candidate.startswith("#"):
        return False
    upper_candidate = candidate.upper()
    if upper_candidate == "#N/A":
        return True
    return upper_candidate.endswith(("!", "?"))


def _values_contain_sheets_errors(values: List[List[object]]) -> bool:
    """Check whether a 2D array of cell values contains any Sheets error tokens."""
    for row in values:
        for cell in row:
            if _is_sheets_error_token(cell):
                return True
    return False


def _a1_range_for_values(a1_range: str, values: List[List[object]]) -> Optional[str]:
    """Compute a tight A1 range for a returned values matrix."""
    sheet_name, range_part = _split_sheet_and_range(a1_range)
    if not range_part:
        return None

    start_part = range_part.split(":", 1)[0]
    start_col, start_row = _parse_a1_part(start_part)
    if start_col is None or start_row is None:
        return None

    height = len(values)
    width = max((len(row) for row in values), default=0)
    if height <= 0 or width <= 0:
        return None

    end_row = start_row + height - 1
    end_col = start_col + width - 1

    start_label = f"{_index_to_column(start_col)}{start_row + 1}"
    end_label = f"{_index_to_column(end_col)}{end_row + 1}"
    range_ref = (
        start_label if start_label == end_label else f"{start_label}:{end_label}"
    )

    if sheet_name:
        return f"{_quote_sheet_title_for_a1(sheet_name)}!{range_ref}"
    return range_ref


def _a1_range_cell_count(a1_range: str) -> Optional[int]:
    """Return cell count for an explicit rectangular A1 range. None when open-ended."""
    _, range_part = _split_sheet_and_range(a1_range)
    if not range_part:
        return None

    if ":" in range_part:
        start_part, end_part = range_part.split(":", 1)
    else:
        start_part = end_part = range_part

    try:
        start_col, start_row = _parse_a1_part(start_part)
        end_col, end_row = _parse_a1_part(end_part)
    except UserInputError:
        return None

    if None in (start_col, start_row, end_col, end_row):
        return None
    if end_col < start_col or end_row < start_row:
        return None

    return (end_col - start_col + 1) * (end_row - start_row + 1)


def _extract_cell_errors_from_grid(spreadsheet: dict) -> list[dict[str, Optional[str]]]:
    """Extracts error information from spreadsheet grid data."""
    errors: list[dict[str, Optional[str]]] = []
    for sheet in spreadsheet.get("sheets", []) or []:
        sheet_title = sheet.get("properties", {}).get("title") or "Unknown"
        for grid in sheet.get("data", []) or []:
            start_row = _coerce_int(grid.get("startRow"), default=0)
            start_col = _coerce_int(grid.get("startColumn"), default=0)
            for row_offset, row_data in enumerate(grid.get("rowData", []) or []):
                if not row_data:
                    continue
                for col_offset, cell_data in enumerate(
                    row_data.get("values", []) or []
                ):
                    if not cell_data:
                        continue
                    error_value = (cell_data.get("effectiveValue") or {}).get(
                        "errorValue"
                    ) or None
                    if not error_value:
                        continue
                    errors.append(
                        {
                            "cell": _format_a1_cell(
                                sheet_title,
                                start_row + row_offset,
                                start_col + col_offset,
                            ),
                            "type": error_value.get("type"),
                            "message": error_value.get("message"),
                        }
                    )
    return errors


def _extract_cell_hyperlinks_from_grid(spreadsheet: dict) -> list[dict[str, str]]:
    """Extract hyperlink URLs from spreadsheet grid data."""
    hyperlinks: list[dict[str, str]] = []
    for sheet in spreadsheet.get("sheets", []) or []:
        sheet_title = sheet.get("properties", {}).get("title") or "Unknown"
        for grid in sheet.get("data", []) or []:
            start_row = _coerce_int(grid.get("startRow"), default=0)
            start_col = _coerce_int(grid.get("startColumn"), default=0)
            for row_offset, row_data in enumerate(grid.get("rowData", []) or []):
                if not row_data:
                    continue
                for col_offset, cell_data in enumerate(
                    row_data.get("values", []) or []
                ):
                    if not cell_data:
                        continue
                    cell_urls: list[str] = []
                    seen_urls: set[str] = set()

                    hyperlink = cell_data.get("hyperlink")
                    if (
                        isinstance(hyperlink, str)
                        and hyperlink
                        and hyperlink not in seen_urls
                    ):
                        seen_urls.add(hyperlink)
                        cell_urls.append(hyperlink)

                    for text_run in cell_data.get("textFormatRuns", []) or []:
                        if not isinstance(text_run, dict):
                            continue
                        link_uri = (
                            (text_run.get("format") or {}).get("link") or {}
                        ).get("uri")
                        if not isinstance(link_uri, str) or not link_uri:
                            continue
                        if link_uri in seen_urls:
                            continue
                        seen_urls.add(link_uri)
                        cell_urls.append(link_uri)

                    if not cell_urls:
                        continue
                    cell_ref = _format_a1_cell(
                        sheet_title, start_row + row_offset, start_col + col_offset
                    )
                    for url in cell_urls:
                        hyperlinks.append({"cell": cell_ref, "url": url})
    return hyperlinks


def _extract_cell_notes_from_grid(spreadsheet: dict) -> list[dict[str, str]]:
    """Extract cell notes from spreadsheet grid data."""
    notes: list[dict[str, str]] = []
    for sheet in spreadsheet.get("sheets", []) or []:
        sheet_title = sheet.get("properties", {}).get("title") or "Unknown"
        for grid in sheet.get("data", []) or []:
            start_row = _coerce_int(grid.get("startRow"), default=0)
            start_col = _coerce_int(grid.get("startColumn"), default=0)
            for row_offset, row_data in enumerate(grid.get("rowData", []) or []):
                if not row_data:
                    continue
                for col_offset, cell_data in enumerate(
                    row_data.get("values", []) or []
                ):
                    if not cell_data:
                        continue
                    note = cell_data.get("note")
                    if not note:
                        continue
                    notes.append(
                        {
                            "cell": _format_a1_cell(
                                sheet_title,
                                start_row + row_offset,
                                start_col + col_offset,
                            ),
                            "note": note,
                        }
                    )
    return notes


async def _fetch_detailed_sheet_errors(
    service, spreadsheet_id: str, a1_range: str
) -> list[dict[str, Optional[str]]]:
    response = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            ranges=[a1_range],
            includeGridData=True,
            fields="sheets(properties(title),data(startRow,startColumn,rowData(values(effectiveValue(errorValue(type,message))))))",
        )
        .execute
    )
    return _extract_cell_errors_from_grid(response)


async def _fetch_cell_formulas(
    service,
    spreadsheet_id: str,
    resolved_range: str,
) -> list[dict[str, str]]:
    """Fetch formula strings for cells in the given range.

    Returns a list of {cell, formula} entries. Returns empty list on failure
    so the read tool can degrade gracefully.
    """
    try:
        result = await asyncio.to_thread(
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=resolved_range,
                valueRenderOption="FORMULA",
            )
            .execute
        )
    except Exception as exc:
        logger.warning(
            "[read_sheet_values] Failed fetching formula values for range '%s': %s",
            resolved_range,
            exc,
        )
        return []

    formula_values = result.get("values", [])
    formulas: list[dict[str, str]] = []

    sheet_name, range_part = _split_sheet_and_range(resolved_range)
    start_part = range_part.split(":")[0] if ":" in range_part else range_part
    start_col_idx, start_row_idx = _parse_a1_part(start_part)
    base_col = start_col_idx if start_col_idx is not None else 0
    base_row = start_row_idx if start_row_idx is not None else 0

    for row_offset, formula_row in enumerate(formula_values):
        for col_offset, cell_value in enumerate(formula_row):
            if isinstance(cell_value, str) and cell_value.startswith("="):
                abs_col = base_col + col_offset
                abs_row = base_row + row_offset
                cell_ref = f"{_index_to_column(abs_col)}{abs_row + 1}"
                if sheet_name:
                    cell_ref = f"{_quote_sheet_title_for_a1(sheet_name)}!{cell_ref}"
                formulas.append({"cell": cell_ref, "formula": cell_value})

    return formulas


async def _fetch_grid_metadata(
    service,
    spreadsheet_id: str,
    resolved_range: str,
    values: List[List[object]],
    include_hyperlinks: bool = False,
    include_notes: bool = False,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Fetch hyperlinks and/or notes for a range via a single spreadsheets.get.

    Returns (hyperlinks, notes) — each is an empty list when the corresponding
    flag is False, no data was found, or the range exceeded the cell-count cap.
    """
    if not include_hyperlinks and not include_notes:
        return [], []

    tight_range = _a1_range_for_values(resolved_range, values)
    if not tight_range:
        logger.info(
            "[read_sheet_values] Skipping grid metadata fetch for range '%s': "
            "unable to determine tight bounds",
            resolved_range,
        )
        return [], []

    cell_count = _a1_range_cell_count(tight_range) or sum(len(row) for row in values)
    if cell_count > MAX_GRID_METADATA_CELLS:
        logger.info(
            "[read_sheet_values] Skipping grid metadata fetch for large range "
            "'%s' (%d cells > %d limit)",
            tight_range,
            cell_count,
            MAX_GRID_METADATA_CELLS,
        )
        return [], []

    value_fields: list[str] = []
    if include_hyperlinks:
        value_fields.extend(["hyperlink", "textFormatRuns(format(link(uri)))"])
    if include_notes:
        value_fields.append("note")

    fields = (
        "sheets(properties(title),data(startRow,startColumn,"
        f"rowData(values({','.join(value_fields)}))))"
    )

    try:
        response = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                ranges=[tight_range],
                includeGridData=True,
                fields=fields,
            )
            .execute
        )
    except Exception as exc:
        logger.warning(
            "[read_sheet_values] Failed fetching grid metadata for range '%s': %s",
            tight_range,
            exc,
        )
        return [], []

    hyperlinks: list[dict[str, str]] = []
    if include_hyperlinks:
        hyperlinks = _extract_cell_hyperlinks_from_grid(response)

    notes: list[dict[str, str]] = []
    if include_notes:
        notes = _extract_cell_notes_from_grid(response)

    return hyperlinks, notes


def _grid_range_to_a1(grid_range: dict, sheet_titles: dict[int, str]) -> str:
    """Convert a GridRange to an A1-like string using known sheet titles."""
    sheet_id = grid_range.get("sheetId")
    sheet_title = sheet_titles.get(sheet_id, f"Sheet {sheet_id}")

    start_row = grid_range.get("startRowIndex")
    end_row = grid_range.get("endRowIndex")
    start_col = grid_range.get("startColumnIndex")
    end_col = grid_range.get("endColumnIndex")

    if start_row is None and end_row is None and start_col is None and end_col is None:
        return sheet_title

    def row_label(idx: Optional[int]) -> str:
        return str(idx + 1) if idx is not None else ""

    def col_label(idx: Optional[int]) -> str:
        return _index_to_column(idx) if idx is not None else ""

    start_label = f"{col_label(start_col)}{row_label(start_row)}"
    end_label = f"{col_label(end_col - 1 if end_col is not None else None)}{row_label(end_row - 1 if end_row is not None else None)}"

    if start_label and end_label:
        range_ref = (
            start_label if start_label == end_label else f"{start_label}:{end_label}"
        )
    elif start_label:
        range_ref = start_label
    elif end_label:
        range_ref = end_label
    else:
        range_ref = ""

    return f"{sheet_title}!{range_ref}" if range_ref else sheet_title


def _summarize_conditional_rule(
    rule: dict, index: int, sheet_titles: dict[int, str]
) -> dict:
    """Produce a structured summary of a conditional formatting rule."""
    ranges = rule.get("ranges", [])
    range_labels = [_grid_range_to_a1(rng, sheet_titles) for rng in ranges]

    if "booleanRule" in rule:
        boolean_rule = rule["booleanRule"]
        condition = boolean_rule.get("condition", {})
        cond_values = [
            val.get("userEnteredValue")
            for val in condition.get("values", [])
            if isinstance(val, dict) and "userEnteredValue" in val
        ]
        fmt = boolean_rule.get("format", {})
        return {
            "index": index,
            "kind": "boolean",
            "condition_type": condition.get("type", "UNKNOWN"),
            "condition_values": cond_values,
            "background_color": _color_to_hex(fmt.get("backgroundColor")),
            "text_color": _color_to_hex(
                fmt.get("textFormat", {}).get("foregroundColor")
            ),
            "ranges": range_labels,
        }

    if "gradientRule" in rule:
        gradient_rule = rule["gradientRule"]
        points = []
        for point_name in ("minpoint", "midpoint", "maxpoint"):
            point = gradient_rule.get(point_name)
            if not point:
                continue
            points.append(
                {
                    "position": point_name,
                    "type": point.get("type", point_name),
                    "value": point.get("value"),
                    "color": _color_to_hex(point.get("color")),
                }
            )
        return {
            "index": index,
            "kind": "gradient",
            "gradient_points": points,
            "ranges": range_labels,
        }

    return {
        "index": index,
        "kind": "unknown",
        "ranges": range_labels,
    }


def _summarize_conditional_rules(
    rules: List[dict], sheet_titles: dict[int, str]
) -> List[dict]:
    """Produce structured summaries for a list of rules."""
    return [
        _summarize_conditional_rule(rule, idx, sheet_titles)
        for idx, rule in enumerate(rules)
    ]


CONDITION_TYPES = {
    "NUMBER_GREATER",
    "NUMBER_GREATER_THAN_EQ",
    "NUMBER_LESS",
    "NUMBER_LESS_THAN_EQ",
    "NUMBER_EQ",
    "NUMBER_NOT_EQ",
    "TEXT_CONTAINS",
    "TEXT_NOT_CONTAINS",
    "TEXT_STARTS_WITH",
    "TEXT_ENDS_WITH",
    "TEXT_EQ",
    "DATE_BEFORE",
    "DATE_ON_OR_BEFORE",
    "DATE_AFTER",
    "DATE_ON_OR_AFTER",
    "DATE_EQ",
    "DATE_NOT_EQ",
    "DATE_BETWEEN",
    "DATE_NOT_BETWEEN",
    "NOT_BLANK",
    "BLANK",
    "CUSTOM_FORMULA",
    "ONE_OF_RANGE",
    "ONE_OF_LIST",
    "NUMBER_BETWEEN",
    "NUMBER_NOT_BETWEEN",
    "BOOLEAN",
}

GRADIENT_POINT_TYPES = {"MIN", "MAX", "NUMBER", "PERCENT", "PERCENTILE"}


async def _fetch_sheets_with_rules(
    service, spreadsheet_id: str
) -> tuple[List[dict], dict[int, str]]:
    """Fetch sheets with titles and conditional format rules in a single request."""
    response = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title),conditionalFormats)",
        )
        .execute
    )
    sheets = response.get("sheets", []) or []
    sheet_titles: dict[int, str] = {}
    for sheet in sheets:
        props = sheet.get("properties", {})
        sid = props.get("sheetId")
        if sid is not None:
            sheet_titles[sid] = props.get("title", f"Sheet {sid}")
    return sheets, sheet_titles


def _select_sheet(sheets: List[dict], sheet_name: Optional[str]) -> dict:
    """Select a sheet by name, or default to the first sheet if name is not provided."""
    if not sheets:
        raise UserInputError("Spreadsheet has no sheets.")

    if sheet_name is None:
        return sheets[0]

    for sheet in sheets:
        if sheet.get("properties", {}).get("title") == sheet_name:
            return sheet

    available_titles = [
        sheet.get("properties", {}).get("title", "Untitled") for sheet in sheets
    ]
    raise UserInputError(
        f"Sheet '{sheet_name}' not found. Available sheets: {', '.join(available_titles)}."
    )


def _parse_condition_values(
    condition_values: Optional[Union[str, List[Union[str, int, float]]]],
) -> Optional[List[Union[str, int, float]]]:
    """Normalize and validate condition_values into a list of strings/numbers."""
    parsed = condition_values
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except json.JSONDecodeError as exc:
            raise UserInputError(
                "condition_values must be a list or a JSON-encoded list (e.g., '[\"=$B2>1000\"]')."
            ) from exc

    if parsed is not None and not isinstance(parsed, list):
        parsed = [parsed]

    if parsed:
        for idx, val in enumerate(parsed):
            if not isinstance(val, (str, int, float)):
                raise UserInputError(
                    f"condition_values[{idx}] must be a string or number, got {type(val).__name__}."
                )

    return parsed


def _parse_gradient_points(
    gradient_points: Optional[Union[str, List[dict]]],
) -> Optional[List[dict]]:
    """Normalize gradient points into a list of dicts with type/value/color."""
    if gradient_points is None:
        return None

    parsed = gradient_points
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except json.JSONDecodeError as exc:
            raise UserInputError(
                "gradient_points must be a list or JSON-encoded list of points "
                '(e.g., \'[{"type":"MIN","color":"#ffffff"}, {"type":"MAX","color":"#ff0000"}]\').'
            ) from exc

    if not isinstance(parsed, list):
        raise UserInputError("gradient_points must be a list of point objects.")

    if len(parsed) < 2 or len(parsed) > 3:
        raise UserInputError("Provide 2 or 3 gradient points (min/max or min/mid/max).")

    normalized_points: List[dict] = []
    for idx, point in enumerate(parsed):
        if not isinstance(point, dict):
            raise UserInputError(
                f"gradient_points[{idx}] must be an object with type/color."
            )

        point_type = point.get("type")
        if not point_type or point_type.upper() not in GRADIENT_POINT_TYPES:
            raise UserInputError(
                f"gradient_points[{idx}].type must be one of {sorted(GRADIENT_POINT_TYPES)}."
            )
        color_raw = point.get("color")
        color_dict = (
            _parse_hex_color(color_raw)
            if not isinstance(color_raw, dict)
            else color_raw
        )
        if not color_dict:
            raise UserInputError(f"gradient_points[{idx}].color is required.")

        normalized = {"type": point_type.upper(), "color": color_dict}
        if "value" in point and point["value"] is not None:
            normalized["value"] = str(point["value"])
        normalized_points.append(normalized)

    return normalized_points


def _build_boolean_rule(
    ranges: List[dict],
    condition_type: str,
    condition_values: Optional[List[Union[str, int, float]]],
    background_color: Optional[str],
    text_color: Optional[str],
) -> tuple[dict, str]:
    """Build a Sheets boolean conditional formatting rule payload."""
    if not background_color and not text_color:
        raise UserInputError(
            "Provide at least one of background_color or text_color for the rule format."
        )

    cond_type_normalized = condition_type.upper()
    if cond_type_normalized not in CONDITION_TYPES:
        raise UserInputError(
            f"condition_type must be one of {sorted(CONDITION_TYPES)}."
        )

    condition = {"type": cond_type_normalized}
    if condition_values:
        condition["values"] = [
            {"userEnteredValue": str(value)} for value in condition_values
        ]

    bg_color_parsed = _parse_hex_color(background_color)
    text_color_parsed = _parse_hex_color(text_color)

    format_obj = {}
    if bg_color_parsed:
        format_obj["backgroundColor"] = bg_color_parsed
    if text_color_parsed:
        format_obj["textFormat"] = {"foregroundColor": text_color_parsed}

    return (
        {
            "ranges": ranges,
            "booleanRule": {
                "condition": condition,
                "format": format_obj,
            },
        },
        cond_type_normalized,
    )


def _build_gradient_rule(
    ranges: List[dict],
    gradient_points: List[dict],
) -> dict:
    """Build a Sheets gradient conditional formatting rule payload."""
    rule_body: dict = {"ranges": ranges, "gradientRule": {}}
    if len(gradient_points) == 2:
        rule_body["gradientRule"]["minpoint"] = gradient_points[0]
        rule_body["gradientRule"]["maxpoint"] = gradient_points[1]
    else:
        rule_body["gradientRule"]["minpoint"] = gradient_points[0]
        rule_body["gradientRule"]["midpoint"] = gradient_points[1]
        rule_body["gradientRule"]["maxpoint"] = gradient_points[2]
    return rule_body


# Data validation helpers ---------------------------------------------------

# Map of friendly rule_type → DataValidationRule.condition.type. Some Sheets
# condition types (ONE_OF_LIST / ONE_OF_RANGE / BOOLEAN) only exist as data
# validation conditions, not as conditional formatting conditions.
DATA_VALIDATION_RULE_TYPES = {
    "ONE_OF_LIST",
    "ONE_OF_RANGE",
    "NUMBER_BETWEEN",
    "NUMBER_NOT_BETWEEN",
    "NUMBER_GREATER",
    "NUMBER_GREATER_THAN_EQ",
    "NUMBER_LESS",
    "NUMBER_LESS_THAN_EQ",
    "NUMBER_EQ",
    "NUMBER_NOT_EQ",
    "TEXT_CONTAINS",
    "TEXT_NOT_CONTAINS",
    "TEXT_STARTS_WITH",
    "TEXT_ENDS_WITH",
    "TEXT_EQ",
    "TEXT_IS_EMAIL",
    "TEXT_IS_URL",
    "DATE_BEFORE",
    "DATE_ON_OR_BEFORE",
    "DATE_AFTER",
    "DATE_ON_OR_AFTER",
    "DATE_EQ",
    "DATE_BETWEEN",
    "DATE_NOT_BETWEEN",
    "DATE_IS_VALID",
    "BOOLEAN",
    "BLANK",
    "NOT_BLANK",
    "CUSTOM_FORMULA",
}


def _build_data_validation_condition(
    rule_type: str,
    values: Optional[Union[str, List[Union[str, int, float]]]],
    source_range: Optional[str],
) -> dict:
    """Map a friendly rule_type + inputs to a DataValidationRule.condition dict."""
    rule_type_normalized = rule_type.strip().upper()
    if rule_type_normalized not in DATA_VALIDATION_RULE_TYPES:
        raise UserInputError(
            f"rule_type must be one of {sorted(DATA_VALIDATION_RULE_TYPES)}, got '{rule_type}'."
        )

    parsed_values: Optional[List[Union[str, int, float]]]
    if isinstance(values, str):
        try:
            parsed_values = json.loads(values)
            if not isinstance(parsed_values, list):
                parsed_values = [parsed_values]
        except json.JSONDecodeError:
            parsed_values = [values]
    elif values is None:
        parsed_values = None
    elif isinstance(values, list):
        parsed_values = values
    else:
        parsed_values = [values]

    condition: dict = {"type": rule_type_normalized}

    if rule_type_normalized == "ONE_OF_LIST":
        if not parsed_values:
            raise UserInputError(
                "values is required for ONE_OF_LIST (provide the dropdown choices)."
            )
        condition["values"] = [{"userEnteredValue": str(v)} for v in parsed_values]
    elif rule_type_normalized == "ONE_OF_RANGE":
        if not source_range:
            raise UserInputError(
                "source_range is required for ONE_OF_RANGE (e.g., 'Lookup!A2:A20')."
            )
        condition["values"] = [{"userEnteredValue": f"={source_range}"}]
    elif rule_type_normalized in {
        "BOOLEAN",
        "BLANK",
        "NOT_BLANK",
        "DATE_IS_VALID",
        "TEXT_IS_EMAIL",
        "TEXT_IS_URL",
    }:
        # These conditions take no values.
        if parsed_values:
            logger.info(
                "[add_data_validation] Ignoring values for %s rule; type takes no values.",
                rule_type_normalized,
            )
    else:
        if not parsed_values:
            raise UserInputError(f"values is required for {rule_type_normalized}.")
        condition["values"] = [{"userEnteredValue": str(v)} for v in parsed_values]

    return condition


# Named range helpers -------------------------------------------------------


async def _resolve_named_range_id(
    service, spreadsheet_id: str, name: str
) -> Optional[str]:
    """Look up a named range by name, returning its namedRangeId if found."""
    response = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="namedRanges(namedRangeId,name)",
        )
        .execute
    )
    for named in response.get("namedRanges", []) or []:
        if named.get("name") == name:
            return named.get("namedRangeId")
    return None
