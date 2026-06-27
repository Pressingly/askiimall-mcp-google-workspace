"""
Google Sheets MCP Tools

This module provides MCP tools for interacting with Google Sheets API.
"""

import logging
import asyncio
import copy
import json
from typing import List, Optional, Dict, Any, Union

from pydantic import Field

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors
from core.response import success_response
from core.comments import create_comment_tools
from gsheets.sheets_helpers import (
    CONDITION_TYPES,
    UserInputError,
    _a1_range_for_values,
    _build_boolean_rule,
    _build_data_validation_condition,
    _build_gradient_rule,
    _column_to_index,
    _fetch_cell_formulas,
    _fetch_detailed_sheet_errors,
    _fetch_grid_metadata,
    _fetch_sheets_with_rules,
    _parse_a1_range,
    _parse_condition_values,
    _parse_gradient_points,
    _parse_hex_color,
    _resolve_named_range_id,
    _select_sheet,
    _summarize_conditional_rules,
    _to_extended_value,
    _values_contain_sheets_errors,
)

logger = logging.getLogger(__name__)


def _map_spreadsheet(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw Drive file entry for a spreadsheet to a clean shape."""
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "modified": raw.get("modifiedTime"),
        "link": raw.get("webViewLink"),
    }


def _map_sheet(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw Sheets API sheet object to a clean shape."""
    props = raw.get("properties", {})
    grid = props.get("gridProperties", {})
    return {
        "id": props.get("sheetId"),
        "title": props.get("title"),
        "rows": grid.get("rowCount"),
        "cols": grid.get("columnCount"),
    }


@server.tool()
@handle_http_errors("list_spreadsheets", is_read_only=True, service_type="sheets")
@require_google_service("drive", "drive_read")
async def list_spreadsheets(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    max_results: int = Field(
        25, description="Maximum number of spreadsheets to return. Defaults to 25."
    ),
) -> str:
    """
    Lists spreadsheets from Google Drive that the user has access to.

    Returns:
        str: A formatted list of spreadsheet files (name, ID, modified time).
    """
    logger.info(f"[list_spreadsheets] Invoked. Email: '{user_google_email}'")

    files_response = await asyncio.to_thread(
        service.files()
        .list(
            q="mimeType='application/vnd.google-apps.spreadsheet'",
            pageSize=max_results,
            fields="files(id,name,modifiedTime,webViewLink)",
            orderBy="modifiedTime desc",
        )
        .execute
    )

    files = files_response.get("files", [])
    mapped = [_map_spreadsheet(f) for f in files]

    logger.info(
        f"Successfully listed {len(mapped)} spreadsheets for {user_google_email}."
    )
    return success_response({"spreadsheets": mapped, "count": len(mapped)})


@server.tool()
@handle_http_errors("get_spreadsheet_info", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def get_spreadsheet_info(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(
        ...,
        description="The ID of the spreadsheet to get info for. Use the FULL ID exactly from list_spreadsheets or create_spreadsheet - do NOT truncate or modify it.",
    ),
) -> str:
    """
    Gets information about a specific spreadsheet including its sheets.

    Returns:
        str: Formatted spreadsheet information including title and sheets list.
    """
    logger.info(
        f"[get_spreadsheet_info] Invoked. Email: '{user_google_email}', Spreadsheet ID: {spreadsheet_id}"
    )

    spreadsheet = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="properties.title,spreadsheetId,spreadsheetUrl,sheets.properties",
        )
        .execute
    )

    title = spreadsheet.get("properties", {}).get("title")
    sheets = spreadsheet.get("sheets", [])
    mapped_sheets = [_map_sheet(s) for s in sheets]

    logger.info(
        f"Successfully retrieved info for spreadsheet {spreadsheet_id} for {user_google_email}."
    )
    return success_response(
        {
            "id": spreadsheet.get("spreadsheetId"),
            "title": title,
            "link": spreadsheet.get("spreadsheetUrl"),
            "sheets": mapped_sheets,
            "sheet_count": len(mapped_sheets),
        }
    )


@server.tool()
@handle_http_errors("read_sheet_values", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def read_sheet_values(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(
        ...,
        description="The ID of the spreadsheet. Use the FULL ID exactly from list_spreadsheets or create_spreadsheet - do NOT truncate or modify it.",
    ),
    range_name: str = Field(
        "A1:Z1000",
        description="The range to read in A1 notation. Examples: 'Sheet1!A1:D10' (specific sheet and range), 'A1:D10' (current sheet), 'A:Z' (entire columns A through Z). Defaults to 'A1:Z1000'.",
    ),
    include_hyperlinks: bool = Field(
        False,
        description="If True, also fetch hyperlink metadata for the range. Off by default because it triggers an extra includeGridData request.",
    ),
    include_notes: bool = Field(
        False,
        description="If True, also fetch cell notes for the range. Off by default because it triggers an extra includeGridData request.",
    ),
    include_formulas: bool = Field(
        False,
        description="If True, also fetch raw formula strings for cells that contain formulas. Off by default because it triggers an extra API call.",
    ),
) -> str:
    """
    Reads values from a specific range in a Google Sheet.

    Optional flags add hyperlinks/notes/formulas via additional API calls. When
    cells contain Sheets error tokens (#REF!, #N/A, ...) detailed error metadata
    is automatically attached.

    Returns:
        str: The values from the specified range, plus optional metadata sections.
    """
    logger.info(
        f"[read_sheet_values] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, "
        f"Range: {range_name}, hyperlinks={include_hyperlinks}, notes={include_notes}, formulas={include_formulas}"
    )

    result = await asyncio.to_thread(
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute
    )

    values = result.get("values", [])
    resolved_range = result.get("range", range_name)

    payload: Dict[str, Any] = {
        "values": values,
        "rows": len(values),
        "cols": max((len(row) for row in values), default=0),
        "range": resolved_range,
    }

    hyperlinks: List[Dict[str, str]] = []
    notes: List[Dict[str, str]] = []
    if include_hyperlinks or include_notes:
        hyperlinks, notes = await _fetch_grid_metadata(
            service,
            spreadsheet_id,
            resolved_range,
            values,
            include_hyperlinks=include_hyperlinks,
            include_notes=include_notes,
        )
        if include_hyperlinks:
            payload["hyperlinks"] = hyperlinks
        if include_notes:
            payload["notes"] = notes

    if include_formulas:
        payload["formulas"] = await _fetch_cell_formulas(
            service, spreadsheet_id, resolved_range
        )

    if values and _values_contain_sheets_errors(values):
        detailed_range = _a1_range_for_values(resolved_range, values) or resolved_range
        try:
            payload["errors"] = await _fetch_detailed_sheet_errors(
                service, spreadsheet_id, detailed_range
            )
        except Exception as exc:
            logger.warning(
                "[read_sheet_values] Failed fetching detailed error messages for range '%s': %s",
                detailed_range,
                exc,
            )

    logger.info(f"Successfully read {len(values)} rows for {user_google_email}.")
    return success_response(payload)


@server.tool()
@handle_http_errors("modify_sheet_values", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def modify_sheet_values(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(
        ...,
        description="The ID of the spreadsheet. Use the FULL ID exactly from list_spreadsheets or create_spreadsheet - do NOT truncate or modify it.",
    ),
    range_name: str = Field(
        ...,
        description="The range to modify in A1 notation. Examples: 'Sheet1!A1:D10' (specific sheet and range), 'A1:D10' (current sheet).",
    ),
    values: Optional[List[List[str]]] = Field(
        None,
        description="2D array of values to write/update. Can be a Python list of lists. Each inner list represents a row. Example: [['Header1', 'Header2'], ['Value1', 'Value2']]. Required unless clear_values=True.",
    ),
    value_input_option: str = Field(
        "USER_ENTERED",
        description="How to interpret input values. Options: 'RAW' (values are stored exactly as entered, formulas are stored as text) or 'USER_ENTERED' (values are parsed as if typed into the UI, formulas are evaluated). Defaults to 'USER_ENTERED'.",
    ),
    clear_values: bool = Field(
        False,
        description="If True, clears the range instead of writing values. When True, the 'values' parameter is ignored. Defaults to False.",
    ),
) -> str:
    """
    Modifies values in a specific range of a Google Sheet - can write, update, or clear values.

    Returns:
        str: Confirmation message of the successful modification operation.
    """
    operation = "clear" if clear_values else "write"
    logger.info(
        f"[modify_sheet_values] Invoked. Operation: {operation}, Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Range: {range_name}"
    )

    # Parse values if it's a JSON string (MCP passes parameters as JSON strings)
    if values is not None and isinstance(values, str):
        try:
            parsed_values = json.loads(values)
            if not isinstance(parsed_values, list):
                raise ValueError(
                    f"Values must be a list, got {type(parsed_values).__name__}"
                )
            for i, row in enumerate(parsed_values):
                if not isinstance(row, list):
                    raise ValueError(
                        f"Row {i} must be a list, got {type(row).__name__}"
                    )
            values = parsed_values
            logger.info(
                f"[modify_sheet_values] Parsed JSON string to Python list with {len(values)} rows"
            )
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON format for values: {e}")
        except ValueError as e:
            raise Exception(f"Invalid values structure: {e}")

    if not clear_values and not values:
        raise Exception(
            "Either 'values' must be provided or 'clear_values' must be True."
        )

    if clear_values:
        result = await asyncio.to_thread(
            service.spreadsheets()
            .values()
            .clear(spreadsheetId=spreadsheet_id, range=range_name)
            .execute
        )

        cleared_range = result.get("clearedRange", range_name)
        logger.info(
            f"Successfully cleared range '{cleared_range}' for {user_google_email}."
        )
        return success_response({"cleared": True, "range": cleared_range})
    else:
        body = {"values": values}

        result = await asyncio.to_thread(
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body,
            )
            .execute
        )

        logger.info(
            f"Successfully updated {result.get('updatedCells', 0)} cells for {user_google_email}."
        )
        return success_response(
            {
                "updated_cells": result.get("updatedCells", 0),
                "updated_rows": result.get("updatedRows", 0),
                "updated_columns": result.get("updatedColumns", 0),
                "range": result.get("updatedRange"),
            }
        )


@server.tool()
@handle_http_errors("create_spreadsheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def create_spreadsheet(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    title: str = Field(..., description="The title of the new spreadsheet."),
    sheet_names: Optional[List[str]] = Field(
        None,
        description="List of sheet names to create. If not provided, creates one sheet with the default name 'Sheet1'. Example: ['Data', 'Summary', 'Charts'].",
    ),
) -> str:
    """
    Creates a new Google Spreadsheet.

    Returns:
        str: Information about the newly created spreadsheet including ID and URL.
    """
    logger.info(
        f"[create_spreadsheet] Invoked. Email: '{user_google_email}', Title: {title}"
    )

    spreadsheet_body = {"properties": {"title": title}}

    if sheet_names:
        spreadsheet_body["sheets"] = [
            {"properties": {"title": sheet_name}} for sheet_name in sheet_names
        ]

    spreadsheet = await asyncio.to_thread(
        service.spreadsheets().create(body=spreadsheet_body).execute
    )

    logger.info(
        f"Successfully created spreadsheet for {user_google_email}. ID: {spreadsheet.get('spreadsheetId')}"
    )
    return success_response(
        {
            "id": spreadsheet.get("spreadsheetId"),
            "title": title,
            "link": spreadsheet.get("spreadsheetUrl"),
        }
    )


@server.tool()
@handle_http_errors("create_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def create_sheet(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(
        ...,
        description="The ID of the spreadsheet to add a sheet to. Use the FULL ID exactly from list_spreadsheets or create_spreadsheet - do NOT truncate or modify it.",
    ),
    sheet_name: str = Field(..., description="The name of the new sheet to create."),
) -> str:
    """
    Creates a new sheet within an existing spreadsheet.

    Returns:
        str: Confirmation message of the successful sheet creation.
    """
    logger.info(
        f"[create_sheet] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Sheet: {sheet_name}"
    )

    request_body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}

    response = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]

    logger.info(
        f"Successfully created sheet for {user_google_email}. Sheet ID: {sheet_id}"
    )
    return success_response(
        {
            "sheet": {"id": sheet_id, "title": sheet_name},
            "spreadsheet_id": spreadsheet_id,
        }
    )


@server.tool()
@handle_http_errors("format_sheet_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def format_sheet_range(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet."),
    range_name: str = Field(
        ...,
        description="A1-style range, optionally with sheet name. Example: 'Sheet1!A1:C10'.",
    ),
    background_color: Optional[str] = Field(
        None, description="Hex background color, e.g. '#FFEECC'."
    ),
    text_color: Optional[str] = Field(
        None, description="Hex text color, e.g. '#000000'."
    ),
    number_format_type: Optional[str] = Field(
        None,
        description="One of NUMBER, NUMBER_WITH_GROUPING, CURRENCY, PERCENT, SCIENTIFIC, DATE, TIME, DATE_TIME, TEXT.",
    ),
    number_format_pattern: Optional[str] = Field(
        None,
        description="Optional custom pattern for the number format (e.g. '#,##0.00').",
    ),
    wrap_strategy: Optional[str] = Field(
        None, description="WRAP, CLIP, or OVERFLOW_CELL."
    ),
    horizontal_alignment: Optional[str] = Field(
        None, description="LEFT, CENTER, or RIGHT."
    ),
    vertical_alignment: Optional[str] = Field(
        None, description="TOP, MIDDLE, or BOTTOM."
    ),
    bold: Optional[bool] = Field(None, description="Apply bold formatting."),
    italic: Optional[bool] = Field(None, description="Apply italic formatting."),
    font_size: Optional[int] = Field(None, description="Font size in points."),
) -> str:
    """
    Apply visual formatting to a range: background/text color, number format,
    text wrapping, alignment, and font styling.

    Returns:
        str: Summary of applied formatting and the affected range.
    """
    logger.info(
        f"[format_sheet_range] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Range: {range_name}"
    )

    if not any(
        [
            background_color,
            text_color,
            number_format_type,
            wrap_strategy,
            horizontal_alignment,
            vertical_alignment,
            bold is not None,
            italic is not None,
            font_size is not None,
        ]
    ):
        raise UserInputError(
            "Provide at least one formatting option (background_color, text_color, "
            "number_format_type, wrap_strategy, horizontal_alignment, "
            "vertical_alignment, bold, italic, or font_size)."
        )

    bg_color_parsed = _parse_hex_color(background_color)
    text_color_parsed = _parse_hex_color(text_color)

    number_format = None
    if number_format_type:
        allowed = {
            "NUMBER",
            "NUMBER_WITH_GROUPING",
            "CURRENCY",
            "PERCENT",
            "SCIENTIFIC",
            "DATE",
            "TIME",
            "DATE_TIME",
            "TEXT",
        }
        normalized = number_format_type.upper()
        if normalized not in allowed:
            raise UserInputError(
                f"number_format_type must be one of {sorted(allowed)}."
            )
        number_format = {"type": normalized}
        if number_format_pattern:
            number_format["pattern"] = number_format_pattern

    wrap_normalized = None
    if wrap_strategy:
        allowed = {"WRAP", "CLIP", "OVERFLOW_CELL"}
        wrap_normalized = wrap_strategy.upper()
        if wrap_normalized not in allowed:
            raise UserInputError(f"wrap_strategy must be one of {sorted(allowed)}.")

    h_align = None
    if horizontal_alignment:
        allowed = {"LEFT", "CENTER", "RIGHT"}
        h_align = horizontal_alignment.upper()
        if h_align not in allowed:
            raise UserInputError(
                f"horizontal_alignment must be one of {sorted(allowed)}."
            )

    v_align = None
    if vertical_alignment:
        allowed = {"TOP", "MIDDLE", "BOTTOM"}
        v_align = vertical_alignment.upper()
        if v_align not in allowed:
            raise UserInputError(
                f"vertical_alignment must be one of {sorted(allowed)}."
            )

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute
    )
    grid_range = _parse_a1_range(range_name, metadata.get("sheets", []))

    user_entered_format: Dict[str, Any] = {}
    fields: List[str] = []

    if bg_color_parsed:
        user_entered_format["backgroundColor"] = bg_color_parsed
        fields.append("userEnteredFormat.backgroundColor")

    text_format: Dict[str, Any] = {}
    text_format_fields: List[str] = []
    if text_color_parsed:
        text_format["foregroundColor"] = text_color_parsed
        text_format_fields.append("userEnteredFormat.textFormat.foregroundColor")
    if bold is not None:
        text_format["bold"] = bold
        text_format_fields.append("userEnteredFormat.textFormat.bold")
    if italic is not None:
        text_format["italic"] = italic
        text_format_fields.append("userEnteredFormat.textFormat.italic")
    if font_size is not None:
        text_format["fontSize"] = font_size
        text_format_fields.append("userEnteredFormat.textFormat.fontSize")
    if text_format:
        user_entered_format["textFormat"] = text_format
        fields.extend(text_format_fields)

    if number_format:
        user_entered_format["numberFormat"] = number_format
        fields.append("userEnteredFormat.numberFormat")
    if wrap_normalized:
        user_entered_format["wrapStrategy"] = wrap_normalized
        fields.append("userEnteredFormat.wrapStrategy")
    if h_align:
        user_entered_format["horizontalAlignment"] = h_align
        fields.append("userEnteredFormat.horizontalAlignment")
    if v_align:
        user_entered_format["verticalAlignment"] = v_align
        fields.append("userEnteredFormat.verticalAlignment")

    request_body = {
        "requests": [
            {
                "repeatCell": {
                    "range": grid_range,
                    "cell": {"userEnteredFormat": user_entered_format},
                    "fields": ",".join(fields),
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    applied = {
        "background_color": background_color,
        "text_color": text_color,
        "number_format": number_format,
        "wrap_strategy": wrap_normalized,
        "horizontal_alignment": h_align,
        "vertical_alignment": v_align,
        "bold": bold,
        "italic": italic,
        "font_size": font_size,
    }
    applied = {k: v for k, v in applied.items() if v is not None}

    logger.info(f"Successfully formatted range '{range_name}' for {user_google_email}.")
    return success_response(
        {
            "spreadsheet_id": spreadsheet_id,
            "range": range_name,
            "applied": applied,
        }
    )


@server.tool()
@handle_http_errors("manage_conditional_formatting", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_conditional_formatting(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet."),
    action: str = Field(..., description="One of 'add', 'update', or 'delete'."),
    range_name: Optional[str] = Field(
        None,
        description="A1-style range. Required for 'add'. Optional for 'update'. Ignored for 'delete'.",
    ),
    condition_type: Optional[str] = Field(
        None,
        description="Sheets condition type, e.g. NUMBER_GREATER, TEXT_CONTAINS, DATE_BEFORE, CUSTOM_FORMULA. Required for 'add' boolean rules.",
    ),
    condition_values: Optional[Union[str, List[Union[str, int, float]]]] = Field(
        None, description="List (or JSON string list) of values for the condition."
    ),
    background_color: Optional[str] = Field(
        None, description="Hex background color to apply when the condition matches."
    ),
    text_color: Optional[str] = Field(
        None, description="Hex text color to apply when the condition matches."
    ),
    rule_index: Optional[int] = Field(
        None,
        description="0-based rule index. Required for 'update' and 'delete'. Optional insertion position for 'add'.",
    ),
    gradient_points: Optional[Union[str, List[dict]]] = Field(
        None,
        description="List (or JSON list) of gradient points, e.g. [{'type':'MIN','color':'#fff'},{'type':'MAX','color':'#f00'}]. Creates a gradient rule and ignores boolean params.",
    ),
    sheet_name: Optional[str] = Field(
        None,
        description="Sheet name to locate the rule when range_name is omitted (used by 'update' / 'delete').",
    ),
) -> str:
    """
    Add, update, or delete conditional formatting rules on a Google Sheet.

    Supports both boolean rules (e.g. NUMBER_GREATER 5 → green background) and
    gradient color scales. Returns the resulting rule state for the affected sheet.
    """
    allowed_actions = {"add", "update", "delete"}
    action_normalized = action.strip().lower()
    if action_normalized not in allowed_actions:
        raise UserInputError(
            f"action must be one of {sorted(allowed_actions)}, got '{action}'."
        )

    logger.info(
        f"[manage_conditional_formatting] action='{action_normalized}', email='{user_google_email}', spreadsheet={spreadsheet_id}"
    )

    if action_normalized == "add":
        if not range_name:
            raise UserInputError("range_name is required for action 'add'.")
        if not condition_type and not gradient_points:
            raise UserInputError(
                "condition_type (or gradient_points) is required for action 'add'."
            )
        if rule_index is not None and (
            not isinstance(rule_index, int) or rule_index < 0
        ):
            raise UserInputError(
                "rule_index must be a non-negative integer when provided."
            )

        gradient_list = _parse_gradient_points(gradient_points)
        condition_list = (
            None if gradient_list else _parse_condition_values(condition_values)
        )

        sheets, sheet_titles = await _fetch_sheets_with_rules(service, spreadsheet_id)
        grid_range = _parse_a1_range(range_name, sheets)

        target_sheet = next(
            (
                s
                for s in sheets
                if s.get("properties", {}).get("sheetId") == grid_range.get("sheetId")
            ),
            None,
        )
        if target_sheet is None:
            raise UserInputError(
                "Target sheet not found while adding conditional formatting."
            )

        current_rules = target_sheet.get("conditionalFormats", []) or []
        insert_at = rule_index if rule_index is not None else len(current_rules)
        if insert_at > len(current_rules):
            raise UserInputError(
                f"rule_index {insert_at} is out of range (current count: {len(current_rules)})."
            )

        if gradient_list:
            new_rule = _build_gradient_rule([grid_range], gradient_list)
        else:
            new_rule, _ = _build_boolean_rule(
                [grid_range],
                condition_type,
                condition_list,
                background_color,
                text_color,
            )

        new_rules_state = copy.deepcopy(current_rules)
        new_rules_state.insert(insert_at, new_rule)

        add_rule_request: Dict[str, Any] = {"rule": new_rule}
        if rule_index is not None:
            add_rule_request["index"] = rule_index

        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addConditionalFormatRule": add_rule_request}]},
            )
            .execute
        )

        return success_response(
            {
                "spreadsheet_id": spreadsheet_id,
                "action": "add",
                "sheet": target_sheet.get("properties", {}).get("title"),
                "inserted_at": insert_at,
                "rules": _summarize_conditional_rules(new_rules_state, sheet_titles),
            }
        )

    if action_normalized == "update":
        if rule_index is None:
            raise UserInputError("rule_index is required for action 'update'.")
        if not isinstance(rule_index, int) or rule_index < 0:
            raise UserInputError("rule_index must be a non-negative integer.")

        gradient_list = _parse_gradient_points(gradient_points)
        condition_list = (
            None
            if gradient_list is not None
            else _parse_condition_values(condition_values)
        )

        sheets, sheet_titles = await _fetch_sheets_with_rules(service, spreadsheet_id)

        target_sheet = None
        grid_range = None
        if range_name:
            grid_range = _parse_a1_range(range_name, sheets)
            target_sheet = next(
                (
                    s
                    for s in sheets
                    if s.get("properties", {}).get("sheetId")
                    == grid_range.get("sheetId")
                ),
                None,
            )
        else:
            target_sheet = _select_sheet(sheets, sheet_name)

        if target_sheet is None:
            raise UserInputError(
                "Target sheet not found while updating conditional formatting."
            )

        sheet_props = target_sheet.get("properties", {})
        sheet_id = sheet_props.get("sheetId")

        rules = target_sheet.get("conditionalFormats", []) or []
        if rule_index >= len(rules):
            raise UserInputError(
                f"rule_index {rule_index} is out of range (current count: {len(rules)})."
            )

        existing_rule = rules[rule_index]
        ranges_to_use = existing_rule.get("ranges", [])
        if range_name:
            ranges_to_use = [grid_range]
        if not ranges_to_use:
            ranges_to_use = [{"sheetId": sheet_id}]

        if gradient_list is not None:
            new_rule = _build_gradient_rule(ranges_to_use, gradient_list)
        elif "gradientRule" in existing_rule:
            if any([background_color, text_color, condition_type, condition_list]):
                raise UserInputError(
                    "Existing rule is a gradient rule. Provide gradient_points to update it, "
                    "or omit formatting/condition parameters to keep it unchanged."
                )
            new_rule = {
                "ranges": ranges_to_use,
                "gradientRule": existing_rule.get("gradientRule", {}),
            }
        else:
            existing_boolean = existing_rule.get("booleanRule", {})
            existing_condition = existing_boolean.get("condition", {})
            existing_format = copy.deepcopy(existing_boolean.get("format", {}))

            cond_type = (condition_type or existing_condition.get("type", "")).upper()
            if not cond_type:
                raise UserInputError("condition_type is required for boolean rules.")
            if cond_type not in CONDITION_TYPES:
                raise UserInputError(
                    f"condition_type must be one of {sorted(CONDITION_TYPES)}."
                )

            if condition_list is not None:
                cond_values = [{"userEnteredValue": str(v)} for v in condition_list]
            else:
                cond_values = existing_condition.get("values")

            new_format = copy.deepcopy(existing_format) if existing_format else {}
            if background_color is not None:
                bg_parsed = _parse_hex_color(background_color)
                if bg_parsed:
                    new_format["backgroundColor"] = bg_parsed
                elif "backgroundColor" in new_format:
                    del new_format["backgroundColor"]
            if text_color is not None:
                text_parsed = _parse_hex_color(text_color)
                text_format = copy.deepcopy(new_format.get("textFormat", {}))
                if text_parsed:
                    text_format["foregroundColor"] = text_parsed
                elif "foregroundColor" in text_format:
                    del text_format["foregroundColor"]
                if text_format:
                    new_format["textFormat"] = text_format
                elif "textFormat" in new_format:
                    del new_format["textFormat"]

            if not new_format:
                raise UserInputError(
                    "At least one format option must remain on the rule."
                )

            new_rule = {
                "ranges": ranges_to_use,
                "booleanRule": {
                    "condition": {"type": cond_type},
                    "format": new_format,
                },
            }
            if cond_values:
                new_rule["booleanRule"]["condition"]["values"] = cond_values

        new_rules_state = copy.deepcopy(rules)
        new_rules_state[rule_index] = new_rule

        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "updateConditionalFormatRule": {
                                "index": rule_index,
                                "sheetId": sheet_id,
                                "rule": new_rule,
                            }
                        }
                    ]
                },
            )
            .execute
        )

        return success_response(
            {
                "spreadsheet_id": spreadsheet_id,
                "action": "update",
                "sheet": sheet_props.get("title"),
                "updated_at": rule_index,
                "rules": _summarize_conditional_rules(new_rules_state, sheet_titles),
            }
        )

    # delete
    if rule_index is None:
        raise UserInputError("rule_index is required for action 'delete'.")
    if not isinstance(rule_index, int) or rule_index < 0:
        raise UserInputError("rule_index must be a non-negative integer.")

    sheets, sheet_titles = await _fetch_sheets_with_rules(service, spreadsheet_id)
    target_sheet = _select_sheet(sheets, sheet_name)
    sheet_props = target_sheet.get("properties", {})
    sheet_id = sheet_props.get("sheetId")
    rules = target_sheet.get("conditionalFormats", []) or []
    if rule_index >= len(rules):
        raise UserInputError(
            f"rule_index {rule_index} is out of range (current count: {len(rules)})."
        )

    new_rules_state = copy.deepcopy(rules)
    del new_rules_state[rule_index]

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "deleteConditionalFormatRule": {
                            "index": rule_index,
                            "sheetId": sheet_id,
                        }
                    }
                ]
            },
        )
        .execute
    )

    return success_response(
        {
            "spreadsheet_id": spreadsheet_id,
            "action": "delete",
            "sheet": sheet_props.get("title"),
            "deleted_at": rule_index,
            "rules": _summarize_conditional_rules(new_rules_state, sheet_titles),
        }
    )


@server.tool()
@handle_http_errors("list_sheet_tables", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def list_sheet_tables(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet."),
) -> str:
    """
    Lists all structured tables in a spreadsheet with their IDs, names, ranges,
    and column details. Use this to find table IDs for append_table_rows.

    Returns:
        str: List of {table_id, name, sheet, range, columns} entries.
    """
    logger.info(
        f"[list_sheet_tables] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}"
    )

    spreadsheet = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(title,sheetId),tables)",
        )
        .execute
    )

    tables: List[Dict[str, Any]] = []
    for sheet in spreadsheet.get("sheets", []):
        sheet_props = sheet.get("properties", {})
        sheet_title = sheet_props.get("title", "Unknown")
        for table in sheet.get("tables", []) or []:
            range_info = table.get("range", {})
            tables.append(
                {
                    "table_id": table.get("tableId"),
                    "name": table.get("name"),
                    "sheet": sheet_title,
                    "sheet_id": sheet_props.get("sheetId"),
                    "range": {
                        "start_row": range_info.get("startRowIndex"),
                        "end_row": range_info.get("endRowIndex"),
                        "start_col": range_info.get("startColumnIndex"),
                        "end_col": range_info.get("endColumnIndex"),
                    },
                    "columns": [
                        col.get("columnName")
                        for col in table.get("columnProperties", []) or []
                    ],
                }
            )

    logger.info(
        f"[list_sheet_tables] Found {len(tables)} tables for {user_google_email}."
    )
    return success_response({"tables": tables, "count": len(tables)})


@server.tool()
@handle_http_errors("append_table_rows", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def append_table_rows(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet."),
    table_id: str = Field(
        ...,
        description="The ID of the table to append to (get from list_sheet_tables).",
    ),
    values: Union[str, List[List[Any]]] = Field(
        ...,
        description="2D array of values to append. Each inner list is one row. Can be a JSON string.",
    ),
) -> str:
    """
    Append rows to a structured table. The rows are added to the end of the
    table body, automatically extending the table range. Use list_sheet_tables
    first to find the table ID.
    """
    logger.info(
        f"[append_table_rows] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Table: {table_id}"
    )

    if isinstance(values, str):
        try:
            values = json.loads(values)
        except json.JSONDecodeError as e:
            raise UserInputError(f"Invalid JSON in values parameter: {e}")

    if not values or not isinstance(values, list):
        raise UserInputError("values must be a non-empty 2D list of cell values.")

    spreadsheet = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId),tables(tableId))",
        )
        .execute
    )

    sheet_id = None
    for sheet in spreadsheet.get("sheets", []):
        for table in sheet.get("tables", []) or []:
            if table.get("tableId") == table_id:
                sheet_id = sheet["properties"]["sheetId"]
                break
        if sheet_id is not None:
            break

    if sheet_id is None:
        raise UserInputError(
            f"Table '{table_id}' not found in spreadsheet {spreadsheet_id}. "
            f"Use list_sheet_tables to find valid table IDs."
        )

    rows = []
    for row_values in values:
        if not isinstance(row_values, list):
            raise UserInputError(
                'Each row in values must be a list. Expected: [["v1","v2"], ["v3","v4"]]'
            )
        rows.append(
            {
                "values": [
                    {"userEnteredValue": _to_extended_value(v)} for v in row_values
                ]
            }
        )

    request_body = {
        "requests": [
            {
                "appendCells": {
                    "sheetId": sheet_id,
                    "tableId": table_id,
                    "rows": rows,
                    "fields": "userEnteredValue",
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    logger.info(
        f"[append_table_rows] Appended {len(values)} rows for {user_google_email}."
    )
    return success_response(
        {
            "spreadsheet_id": spreadsheet_id,
            "table_id": table_id,
            "appended_rows": len(values),
        }
    )


def _build_column_visibility_requests(sheet_id, letters, hidden, label):
    if not isinstance(letters, list):
        raise UserInputError(f"{label} must be a list of column letters.")
    reqs = []
    for col_letter in letters:
        col_idx = _column_to_index(str(col_letter).upper())
        if col_idx is None:
            raise UserInputError(f"Invalid column letter in {label}: '{col_letter}'.")
        reqs.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": col_idx,
                        "endIndex": col_idx + 1,
                    },
                    "properties": {"hiddenByUser": hidden},
                    "fields": "hiddenByUser",
                }
            }
        )
    return reqs


def _build_row_visibility_requests(sheet_id, row_nums, hidden, label):
    if not isinstance(row_nums, list):
        raise UserInputError(f"{label} must be a list of row numbers.")
    reqs = []
    for row_num in row_nums:
        try:
            row_num = int(row_num)
        except (TypeError, ValueError) as exc:
            raise UserInputError(
                f"Row number must be an integer in {label}, got {row_num}."
            ) from exc
        if row_num < 1:
            raise UserInputError(f"Row number must be >= 1 in {label}, got {row_num}.")
        reqs.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": row_num - 1,
                        "endIndex": row_num,
                    },
                    "properties": {"hiddenByUser": hidden},
                    "fields": "hiddenByUser",
                }
            }
        )
    return reqs


@server.tool()
@handle_http_errors("resize_sheet_dimensions", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def resize_sheet_dimensions(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet."),
    sheet_name: Optional[str] = Field(
        None, description="Sheet to target. Defaults to the first sheet."
    ),
    column_sizes: Optional[Union[str, dict]] = Field(
        None,
        description="Dict mapping column letters to pixel widths, e.g. {'A':200,'C':300}.",
    ),
    row_sizes: Optional[Union[str, dict]] = Field(
        None,
        description="Dict mapping 1-based row numbers to pixel heights, e.g. {'1':40,'3':60}.",
    ),
    auto_resize_columns: Optional[Union[str, List[str]]] = Field(
        None, description="List of column letters to auto-resize to fit content."
    ),
    auto_resize_rows: Optional[Union[str, List[int]]] = Field(
        None, description="List of 1-based row numbers to auto-resize to fit content."
    ),
    frozen_row_count: Optional[int] = Field(
        None, description="Rows to freeze from the top. Use 0 to unfreeze."
    ),
    frozen_column_count: Optional[int] = Field(
        None, description="Columns to freeze from the left. Use 0 to unfreeze."
    ),
    hide_columns: Optional[Union[str, List[str]]] = Field(
        None, description="List of column letters to hide."
    ),
    unhide_columns: Optional[Union[str, List[str]]] = Field(
        None, description="List of column letters to unhide."
    ),
    hide_rows: Optional[Union[str, List[int]]] = Field(
        None, description="List of 1-based row numbers to hide."
    ),
    unhide_rows: Optional[Union[str, List[int]]] = Field(
        None, description="List of 1-based row numbers to unhide."
    ),
    insert_rows: Optional[int] = Field(None, description="Number of rows to insert."),
    insert_rows_at: Optional[int] = Field(
        None,
        description="1-based row number to insert before. Appends to end if omitted.",
    ),
    insert_columns: Optional[int] = Field(
        None, description="Number of columns to insert."
    ),
    insert_columns_at: Optional[str] = Field(
        None,
        description="Column letter to insert before, e.g. 'C'. Appends to end if omitted.",
    ),
    delete_rows: Optional[Union[str, List[int]]] = Field(
        None, description="List of 1-based row numbers to delete."
    ),
    delete_columns: Optional[Union[str, List[str]]] = Field(
        None, description="List of column letters to delete."
    ),
) -> str:
    """
    Manage sheet-level dimension properties: resize columns/rows, auto-resize
    to fit content, freeze rows/columns, hide/unhide rows/columns, and
    insert/delete rows/columns. All requested changes apply in a single batch.
    """
    has_any = any(
        [
            column_sizes,
            row_sizes,
            auto_resize_columns,
            auto_resize_rows,
            frozen_row_count is not None,
            frozen_column_count is not None,
            hide_columns,
            unhide_columns,
            hide_rows,
            unhide_rows,
            insert_rows is not None,
            insert_columns is not None,
            delete_rows,
            delete_columns,
        ]
    )
    if not has_any:
        raise UserInputError(
            "Provide at least one of: column_sizes, row_sizes, auto_resize_columns, "
            "auto_resize_rows, frozen_row_count, frozen_column_count, hide_columns, "
            "unhide_columns, hide_rows, unhide_rows, insert_rows, insert_columns, "
            "delete_rows, or delete_columns."
        )

    def _parse_json(value, name):
        if not isinstance(value, str):
            return value
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            raise UserInputError(f"Invalid JSON for {name}: {e}")

    column_sizes = _parse_json(column_sizes, "column_sizes")
    row_sizes = _parse_json(row_sizes, "row_sizes")
    auto_resize_columns = _parse_json(auto_resize_columns, "auto_resize_columns")
    auto_resize_rows = _parse_json(auto_resize_rows, "auto_resize_rows")
    hide_columns = _parse_json(hide_columns, "hide_columns")
    unhide_columns = _parse_json(unhide_columns, "unhide_columns")
    hide_rows = _parse_json(hide_rows, "hide_rows")
    unhide_rows = _parse_json(unhide_rows, "unhide_rows")
    delete_rows = _parse_json(delete_rows, "delete_rows")
    delete_columns = _parse_json(delete_columns, "delete_columns")

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute
    )
    sheets = metadata.get("sheets", [])
    if not sheets:
        raise UserInputError("No sheets found in spreadsheet.")

    target_sheet = _select_sheet(sheets, sheet_name)
    sheet_id = target_sheet["properties"]["sheetId"]

    requests: List[Dict[str, Any]] = []
    applied: Dict[str, Any] = {}

    if column_sizes:
        if not isinstance(column_sizes, dict):
            raise UserInputError(
                "column_sizes must be a dict mapping column letters to pixel widths."
            )
        for col_letter, pixel_size in column_sizes.items():
            col_idx = _column_to_index(col_letter.upper())
            if col_idx is None:
                raise UserInputError(f"Invalid column letter: '{col_letter}'.")
            if not isinstance(pixel_size, (int, float)) or pixel_size <= 0:
                raise UserInputError(
                    f"Pixel size for column '{col_letter}' must be a positive number."
                )
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx,
                            "endIndex": col_idx + 1,
                        },
                        "properties": {"pixelSize": int(pixel_size)},
                        "fields": "pixelSize",
                    }
                }
            )
        applied["column_sizes"] = column_sizes

    if row_sizes:
        if not isinstance(row_sizes, dict):
            raise UserInputError(
                "row_sizes must be a dict mapping row numbers to pixel heights."
            )
        for row_num_str, pixel_size in row_sizes.items():
            try:
                row_num = int(row_num_str)
            except (TypeError, ValueError) as exc:
                raise UserInputError(
                    f"Row number must be an integer >= 1, got {row_num_str}."
                ) from exc
            if row_num < 1:
                raise UserInputError(f"Row number must be >= 1, got {row_num}.")
            if not isinstance(pixel_size, (int, float)) or pixel_size <= 0:
                raise UserInputError(
                    f"Pixel size for row {row_num} must be a positive number."
                )
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": row_num - 1,
                            "endIndex": row_num,
                        },
                        "properties": {"pixelSize": int(pixel_size)},
                        "fields": "pixelSize",
                    }
                }
            )
        applied["row_sizes"] = row_sizes

    if auto_resize_columns:
        if not isinstance(auto_resize_columns, list):
            raise UserInputError(
                "auto_resize_columns must be a list of column letters."
            )
        for col_letter in auto_resize_columns:
            col_idx = _column_to_index(str(col_letter).upper())
            if col_idx is None:
                raise UserInputError(f"Invalid column letter: '{col_letter}'.")
            requests.append(
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx,
                            "endIndex": col_idx + 1,
                        }
                    }
                }
            )
        applied["auto_resize_columns"] = auto_resize_columns

    if auto_resize_rows:
        if not isinstance(auto_resize_rows, list):
            raise UserInputError("auto_resize_rows must be a list of row numbers.")
        for row_num in auto_resize_rows:
            try:
                parsed_row_num = int(row_num)
            except (TypeError, ValueError) as exc:
                raise UserInputError(
                    f"Row number must be an integer >= 1, got {row_num}."
                ) from exc
            if parsed_row_num < 1:
                raise UserInputError(f"Row number must be >= 1, got {parsed_row_num}.")
            requests.append(
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": parsed_row_num - 1,
                            "endIndex": parsed_row_num,
                        }
                    }
                }
            )
        applied["auto_resize_rows"] = auto_resize_rows

    grid_properties: Dict[str, Any] = {}
    grid_fields: List[str] = []
    if frozen_row_count is not None:
        if not isinstance(frozen_row_count, int) or frozen_row_count < 0:
            raise UserInputError("frozen_row_count must be a non-negative integer.")
        grid_properties["frozenRowCount"] = frozen_row_count
        grid_fields.append("gridProperties.frozenRowCount")
        applied["frozen_row_count"] = frozen_row_count
    if frozen_column_count is not None:
        if not isinstance(frozen_column_count, int) or frozen_column_count < 0:
            raise UserInputError("frozen_column_count must be a non-negative integer.")
        grid_properties["frozenColumnCount"] = frozen_column_count
        grid_fields.append("gridProperties.frozenColumnCount")
        applied["frozen_column_count"] = frozen_column_count

    if grid_properties:
        requests.append(
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": grid_properties,
                    },
                    "fields": ",".join(grid_fields),
                }
            }
        )

    if hide_columns:
        requests.extend(
            _build_column_visibility_requests(
                sheet_id, hide_columns, True, "hide_columns"
            )
        )
        applied["hide_columns"] = hide_columns
    if unhide_columns:
        requests.extend(
            _build_column_visibility_requests(
                sheet_id, unhide_columns, False, "unhide_columns"
            )
        )
        applied["unhide_columns"] = unhide_columns
    if hide_rows:
        requests.extend(
            _build_row_visibility_requests(sheet_id, hide_rows, True, "hide_rows")
        )
        applied["hide_rows"] = hide_rows
    if unhide_rows:
        requests.extend(
            _build_row_visibility_requests(sheet_id, unhide_rows, False, "unhide_rows")
        )
        applied["unhide_rows"] = unhide_rows

    if insert_rows is not None:
        if not isinstance(insert_rows, int) or insert_rows < 1:
            raise UserInputError("insert_rows must be a positive integer.")
        if insert_rows_at is not None:
            if not isinstance(insert_rows_at, int) or insert_rows_at < 1:
                raise UserInputError(
                    "insert_rows_at must be a positive integer (1-based)."
                )
            start_idx = insert_rows_at - 1
            requests.append(
                {
                    "insertDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": start_idx,
                            "endIndex": start_idx + insert_rows,
                        },
                        "inheritFromBefore": start_idx > 0,
                    }
                }
            )
            applied["insert_rows"] = {"count": insert_rows, "at": insert_rows_at}
        else:
            requests.append(
                {
                    "appendDimension": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "length": insert_rows,
                    }
                }
            )
            applied["insert_rows"] = {"count": insert_rows, "at": "end"}

    if insert_columns is not None:
        if not isinstance(insert_columns, int) or insert_columns < 1:
            raise UserInputError("insert_columns must be a positive integer.")
        if insert_columns_at is not None:
            col_idx = _column_to_index(str(insert_columns_at).upper())
            if col_idx is None:
                raise UserInputError(
                    f"Invalid column letter for insert_columns_at: '{insert_columns_at}'."
                )
            requests.append(
                {
                    "insertDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx,
                            "endIndex": col_idx + insert_columns,
                        },
                        "inheritFromBefore": col_idx > 0,
                    }
                }
            )
            applied["insert_columns"] = {
                "count": insert_columns,
                "at": insert_columns_at,
            }
        else:
            requests.append(
                {
                    "appendDimension": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "length": insert_columns,
                    }
                }
            )
            applied["insert_columns"] = {"count": insert_columns, "at": "end"}

    if delete_rows:
        if not isinstance(delete_rows, list):
            raise UserInputError("delete_rows must be a list of row numbers.")
        parsed = []
        for row_num in delete_rows:
            try:
                parsed.append(int(row_num))
            except (TypeError, ValueError) as exc:
                raise UserInputError(
                    f"Row number must be an integer >= 1 in delete_rows, got {row_num}."
                ) from exc
        for row_num in sorted(parsed, reverse=True):
            if row_num < 1:
                raise UserInputError(
                    f"Row number must be >= 1 in delete_rows, got {row_num}."
                )
            requests.append(
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": row_num - 1,
                            "endIndex": row_num,
                        }
                    }
                }
            )
        applied["delete_rows"] = delete_rows

    if delete_columns:
        if not isinstance(delete_columns, list):
            raise UserInputError("delete_columns must be a list of column letters.")
        col_indices = []
        for col_letter in delete_columns:
            col_idx = _column_to_index(str(col_letter).upper())
            if col_idx is None:
                raise UserInputError(
                    f"Invalid column letter in delete_columns: '{col_letter}'."
                )
            col_indices.append(col_idx)
        for col_idx in sorted(col_indices, reverse=True):
            requests.append(
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx,
                            "endIndex": col_idx + 1,
                        }
                    }
                }
            )
        applied["delete_columns"] = delete_columns

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
        .execute
    )

    logger.info(
        f"[resize_sheet_dimensions] Applied {len(requests)} request(s) for {user_google_email}."
    )
    return success_response(
        {
            "spreadsheet_id": spreadsheet_id,
            "sheet": target_sheet["properties"].get("title"),
            "request_count": len(requests),
            "applied": applied,
        }
    )


@server.tool()
@handle_http_errors("merge_cells", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def merge_cells(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet."),
    range_name: str = Field(
        ..., description="A1-style range to merge or unmerge, e.g. 'Sheet1!A1:C1'."
    ),
    merge_type: str = Field(
        "MERGE_ALL",
        description="MERGE_ALL (single merged cell), MERGE_COLUMNS (one merge per column), or MERGE_ROWS (one merge per row). Ignored when unmerge=True.",
    ),
    unmerge: bool = Field(
        False,
        description="If True, unmerge any merged cells inside the range instead of merging.",
    ),
) -> str:
    """
    Merge or unmerge cells in a Google Sheet range.
    """
    logger.info(
        f"[merge_cells] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, "
        f"Range: {range_name}, unmerge={unmerge}"
    )

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute
    )
    grid_range = _parse_a1_range(range_name, metadata.get("sheets", []))

    if unmerge:
        request = {"unmergeCells": {"range": grid_range}}
        merge_type_normalized = None
    else:
        allowed = {"MERGE_ALL", "MERGE_COLUMNS", "MERGE_ROWS"}
        merge_type_normalized = merge_type.upper()
        if merge_type_normalized not in allowed:
            raise UserInputError(f"merge_type must be one of {sorted(allowed)}.")
        request = {
            "mergeCells": {
                "range": grid_range,
                "mergeType": merge_type_normalized,
            }
        }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [request]})
        .execute
    )

    return success_response(
        {
            "spreadsheet_id": spreadsheet_id,
            "range": range_name,
            "operation": "unmerge" if unmerge else "merge",
            "merge_type": merge_type_normalized,
        }
    )


@server.tool()
@handle_http_errors("add_data_validation", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def add_data_validation(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet."),
    range_name: str = Field(
        ..., description="A1-style range to apply validation to, e.g. 'Sheet1!D2:D100'."
    ),
    rule_type: str = Field(
        "ONE_OF_LIST",
        description="Validation rule type. Common: ONE_OF_LIST (dropdown), ONE_OF_RANGE (dropdown from another range), NUMBER_BETWEEN, NUMBER_GREATER, DATE_AFTER, TEXT_CONTAINS, BOOLEAN (checkbox), CUSTOM_FORMULA. Ignored when clear=True.",
    ),
    values: Optional[Union[str, List[Union[str, int, float]]]] = Field(
        None,
        description="Values for the rule (e.g. dropdown choices for ONE_OF_LIST, or numeric bounds for NUMBER_BETWEEN). Can be a JSON-encoded list.",
    ),
    source_range: Optional[str] = Field(
        None, description="A1 source range for ONE_OF_RANGE, e.g. 'Lookups!A2:A20'."
    ),
    strict: bool = Field(
        True,
        description="If True, reject invalid input. If False, only show a warning.",
    ),
    show_custom_ui: bool = Field(
        True,
        description="If True, show a dropdown chip in the UI for list/range rules.",
    ),
    input_message: Optional[str] = Field(
        None, description="Optional helper message shown to the user."
    ),
    clear: bool = Field(
        False, description="If True, remove any data validation rule from the range."
    ),
) -> str:
    """
    Apply or remove a data validation rule on a range. Supports list dropdowns,
    range-sourced dropdowns, numeric bounds, dates, text, custom formulas, and
    checkboxes.
    """
    logger.info(
        f"[add_data_validation] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Range: {range_name}, clear={clear}"
    )

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute
    )
    grid_range = _parse_a1_range(range_name, metadata.get("sheets", []))

    if clear:
        request = {"setDataValidation": {"range": grid_range}}
        rule_summary: Optional[Dict[str, Any]] = None
    else:
        condition = _build_data_validation_condition(rule_type, values, source_range)
        rule: Dict[str, Any] = {
            "condition": condition,
            "strict": strict,
            "showCustomUi": show_custom_ui,
        }
        if input_message:
            rule["inputMessage"] = input_message
        request = {
            "setDataValidation": {
                "range": grid_range,
                "rule": rule,
            }
        }
        rule_summary = {
            "rule_type": condition["type"],
            "values": [v.get("userEnteredValue") for v in condition.get("values", [])],
            "strict": strict,
            "show_custom_ui": show_custom_ui,
            "input_message": input_message,
        }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [request]})
        .execute
    )

    return success_response(
        {
            "spreadsheet_id": spreadsheet_id,
            "range": range_name,
            "operation": "clear" if clear else "set",
            "rule": rule_summary,
        }
    )


@server.tool()
@handle_http_errors("manage_named_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_named_range(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet."),
    action: str = Field(..., description="One of 'add', 'update', or 'delete'."),
    name: Optional[str] = Field(
        None,
        description="The named range name. Required for 'add' and 'update'. Used to look up the named range for 'delete' if named_range_id is not given.",
    ),
    range_name: Optional[str] = Field(
        None,
        description="A1-style range, e.g. 'Sheet1!A1:B10'. Required for 'add' and 'update'.",
    ),
    named_range_id: Optional[str] = Field(
        None,
        description="Existing namedRangeId. Required for 'update' if name lookup is ambiguous; resolved automatically for 'delete' when name is provided.",
    ),
) -> str:
    """
    Add, update, or delete a named range. Named ranges can be referenced from
    formulas (e.g. =SUM(Prices)) and survive row/column edits.
    """
    allowed = {"add", "update", "delete"}
    action_normalized = action.strip().lower()
    if action_normalized not in allowed:
        raise UserInputError(
            f"action must be one of {sorted(allowed)}, got '{action}'."
        )

    logger.info(
        f"[manage_named_range] action='{action_normalized}', email='{user_google_email}', spreadsheet={spreadsheet_id}"
    )

    if action_normalized == "add":
        if not name:
            raise UserInputError("name is required for action 'add'.")
        if not range_name:
            raise UserInputError("range_name is required for action 'add'.")

        metadata = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))"
            )
            .execute
        )
        grid_range = _parse_a1_range(range_name, metadata.get("sheets", []))

        response = await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "addNamedRange": {
                                "namedRange": {"name": name, "range": grid_range}
                            }
                        }
                    ]
                },
            )
            .execute
        )

        new_id = (
            response.get("replies", [{}])[0]
            .get("addNamedRange", {})
            .get("namedRange", {})
            .get("namedRangeId")
        )
        return success_response(
            {
                "spreadsheet_id": spreadsheet_id,
                "action": "add",
                "named_range_id": new_id,
                "name": name,
                "range": range_name,
            }
        )

    if action_normalized == "update":
        if not name and not named_range_id:
            raise UserInputError("Provide name or named_range_id for action 'update'.")
        if not range_name:
            raise UserInputError("range_name is required for action 'update'.")

        resolved_id = named_range_id or await _resolve_named_range_id(
            service, spreadsheet_id, name
        )
        if not resolved_id:
            raise UserInputError(
                f"Named range '{name}' not found in spreadsheet {spreadsheet_id}."
            )

        metadata = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))"
            )
            .execute
        )
        grid_range = _parse_a1_range(range_name, metadata.get("sheets", []))

        named_range_payload: Dict[str, Any] = {
            "namedRangeId": resolved_id,
            "range": grid_range,
        }
        fields = ["range"]
        if name:
            named_range_payload["name"] = name
            fields.append("name")

        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "updateNamedRange": {
                                "namedRange": named_range_payload,
                                "fields": ",".join(fields),
                            }
                        }
                    ]
                },
            )
            .execute
        )

        return success_response(
            {
                "spreadsheet_id": spreadsheet_id,
                "action": "update",
                "named_range_id": resolved_id,
                "name": name,
                "range": range_name,
            }
        )

    # delete
    if not name and not named_range_id:
        raise UserInputError("Provide name or named_range_id for action 'delete'.")

    resolved_id = named_range_id or await _resolve_named_range_id(
        service, spreadsheet_id, name
    )
    if not resolved_id:
        raise UserInputError(
            f"Named range '{name}' not found in spreadsheet {spreadsheet_id}."
        )

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"deleteNamedRange": {"namedRangeId": resolved_id}}]},
        )
        .execute
    )

    return success_response(
        {
            "spreadsheet_id": spreadsheet_id,
            "action": "delete",
            "named_range_id": resolved_id,
            "name": name,
        }
    )


# Create comment management tools for sheets
_comment_tools = create_comment_tools("spreadsheet", "spreadsheet_id")

# Extract and register the functions
read_sheet_comments = _comment_tools["read_comments"]
create_sheet_comment = _comment_tools["create_comment"]
reply_to_sheet_comment = _comment_tools["reply_to_comment"]
resolve_sheet_comment = _comment_tools["resolve_comment"]
