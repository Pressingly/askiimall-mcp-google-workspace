"""
Google Sheets MCP Tools

This module provides MCP tools for interacting with Google Sheets API.
"""

import logging
import asyncio
import json
from typing import List, Optional, Dict, Any

from pydantic import Field

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors
from core.response import success_response
from core.comments import create_comment_tools

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
    max_results: int = Field(25, description="Maximum number of spreadsheets to return. Defaults to 25."),
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

    logger.info(f"Successfully listed {len(mapped)} spreadsheets for {user_google_email}.")
    return success_response({"spreadsheets": mapped, "count": len(mapped)})


@server.tool()
@handle_http_errors("get_spreadsheet_info", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def get_spreadsheet_info(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet to get info for. Use the FULL ID exactly from list_spreadsheets or create_spreadsheet - do NOT truncate or modify it."),
) -> str:
    """
    Gets information about a specific spreadsheet including its sheets.

    Returns:
        str: Formatted spreadsheet information including title and sheets list.
    """
    logger.info(f"[get_spreadsheet_info] Invoked. Email: '{user_google_email}', Spreadsheet ID: {spreadsheet_id}")

    spreadsheet = await asyncio.to_thread(
        service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="properties.title,spreadsheetId,spreadsheetUrl,sheets.properties"
        ).execute
    )

    title = spreadsheet.get("properties", {}).get("title")
    sheets = spreadsheet.get("sheets", [])
    mapped_sheets = [_map_sheet(s) for s in sheets]

    logger.info(f"Successfully retrieved info for spreadsheet {spreadsheet_id} for {user_google_email}.")
    return success_response({
        "id": spreadsheet.get("spreadsheetId"),
        "title": title,
        "link": spreadsheet.get("spreadsheetUrl"),
        "sheets": mapped_sheets,
        "sheet_count": len(mapped_sheets),
    })


@server.tool()
@handle_http_errors("read_sheet_values", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def read_sheet_values(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet. Use the FULL ID exactly from list_spreadsheets or create_spreadsheet - do NOT truncate or modify it."),
    range_name: str = Field("A1:Z1000", description="The range to read in A1 notation. Examples: 'Sheet1!A1:D10' (specific sheet and range), 'A1:D10' (current sheet), 'A:Z' (entire columns A through Z). Defaults to 'A1:Z1000'."),
) -> str:
    """
    Reads values from a specific range in a Google Sheet.

    Returns:
        str: The formatted values from the specified range.
    """
    logger.info(f"[read_sheet_values] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Range: {range_name}")

    result = await asyncio.to_thread(
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute
    )

    values = result.get("values", [])

    logger.info(f"Successfully read {len(values)} rows for {user_google_email}.")
    return success_response({
        "values": values,
        "rows": len(values),
        "cols": max((len(row) for row in values), default=0),
    })


@server.tool()
@handle_http_errors("modify_sheet_values", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def modify_sheet_values(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet. Use the FULL ID exactly from list_spreadsheets or create_spreadsheet - do NOT truncate or modify it."),
    range_name: str = Field(..., description="The range to modify in A1 notation. Examples: 'Sheet1!A1:D10' (specific sheet and range), 'A1:D10' (current sheet)."),
    values: Optional[List[List[str]]] = Field(None, description="2D array of values to write/update. Can be a Python list of lists. Each inner list represents a row. Example: [['Header1', 'Header2'], ['Value1', 'Value2']]. Required unless clear_values=True."),
    value_input_option: str = Field("USER_ENTERED", description="How to interpret input values. Options: 'RAW' (values are stored exactly as entered, formulas are stored as text) or 'USER_ENTERED' (values are parsed as if typed into the UI, formulas are evaluated). Defaults to 'USER_ENTERED'."),
    clear_values: bool = Field(False, description="If True, clears the range instead of writing values. When True, the 'values' parameter is ignored. Defaults to False."),
) -> str:
    """
    Modifies values in a specific range of a Google Sheet - can write, update, or clear values.

    Returns:
        str: Confirmation message of the successful modification operation.
    """
    operation = "clear" if clear_values else "write"
    logger.info(f"[modify_sheet_values] Invoked. Operation: {operation}, Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Range: {range_name}")

    # Parse values if it's a JSON string (MCP passes parameters as JSON strings)
    if values is not None and isinstance(values, str):
        try:
            parsed_values = json.loads(values)
            if not isinstance(parsed_values, list):
                raise ValueError(f"Values must be a list, got {type(parsed_values).__name__}")
            for i, row in enumerate(parsed_values):
                if not isinstance(row, list):
                    raise ValueError(f"Row {i} must be a list, got {type(row).__name__}")
            values = parsed_values
            logger.info(f"[modify_sheet_values] Parsed JSON string to Python list with {len(values)} rows")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON format for values: {e}")
        except ValueError as e:
            raise Exception(f"Invalid values structure: {e}")

    if not clear_values and not values:
        raise Exception("Either 'values' must be provided or 'clear_values' must be True.")

    if clear_values:
        result = await asyncio.to_thread(
            service.spreadsheets()
            .values()
            .clear(spreadsheetId=spreadsheet_id, range=range_name)
            .execute
        )

        cleared_range = result.get("clearedRange", range_name)
        logger.info(f"Successfully cleared range '{cleared_range}' for {user_google_email}.")
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

        logger.info(f"Successfully updated {result.get('updatedCells', 0)} cells for {user_google_email}.")
        return success_response({
            "updated_cells": result.get("updatedCells", 0),
            "updated_rows": result.get("updatedRows", 0),
            "updated_columns": result.get("updatedColumns", 0),
            "range": result.get("updatedRange"),
        })


@server.tool()
@handle_http_errors("create_spreadsheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def create_spreadsheet(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    title: str = Field(..., description="The title of the new spreadsheet."),
    sheet_names: Optional[List[str]] = Field(None, description="List of sheet names to create. If not provided, creates one sheet with the default name 'Sheet1'. Example: ['Data', 'Summary', 'Charts']."),
) -> str:
    """
    Creates a new Google Spreadsheet.

    Returns:
        str: Information about the newly created spreadsheet including ID and URL.
    """
    logger.info(f"[create_spreadsheet] Invoked. Email: '{user_google_email}', Title: {title}")

    spreadsheet_body = {
        "properties": {"title": title}
    }

    if sheet_names:
        spreadsheet_body["sheets"] = [
            {"properties": {"title": sheet_name}} for sheet_name in sheet_names
        ]

    spreadsheet = await asyncio.to_thread(
        service.spreadsheets().create(body=spreadsheet_body).execute
    )

    logger.info(f"Successfully created spreadsheet for {user_google_email}. ID: {spreadsheet.get('spreadsheetId')}")
    return success_response({
        "id": spreadsheet.get("spreadsheetId"),
        "title": title,
        "link": spreadsheet.get("spreadsheetUrl"),
    })


@server.tool()
@handle_http_errors("create_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def create_sheet(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    spreadsheet_id: str = Field(..., description="The ID of the spreadsheet to add a sheet to. Use the FULL ID exactly from list_spreadsheets or create_spreadsheet - do NOT truncate or modify it."),
    sheet_name: str = Field(..., description="The name of the new sheet to create."),
) -> str:
    """
    Creates a new sheet within an existing spreadsheet.

    Returns:
        str: Confirmation message of the successful sheet creation.
    """
    logger.info(f"[create_sheet] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Sheet: {sheet_name}")

    request_body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {"title": sheet_name}
                }
            }
        ]
    }

    response = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]

    logger.info(f"Successfully created sheet for {user_google_email}. Sheet ID: {sheet_id}")
    return success_response({
        "sheet": {"id": sheet_id, "title": sheet_name},
        "spreadsheet_id": spreadsheet_id,
    })


# Create comment management tools for sheets
_comment_tools = create_comment_tools("spreadsheet", "spreadsheet_id")

# Extract and register the functions
read_sheet_comments = _comment_tools['read_comments']
create_sheet_comment = _comment_tools['create_comment']
reply_to_sheet_comment = _comment_tools['reply_to_comment']
resolve_sheet_comment = _comment_tools['resolve_comment']
