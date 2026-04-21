"""
Google Docs MCP Tools

This module provides MCP tools for interacting with Google Docs API and managing Google Docs via Drive.
"""
import logging
import asyncio
import io

from googleapiclient.http import MediaIoBaseDownload
from pydantic import Field

# Auth & server utilities
from auth.service_decorator import require_google_service, require_multiple_services
from core.utils import extract_office_xml_text, handle_http_errors
from core.server import server
from core.comments import create_comment_tools
from core.response import success_response

# Import helper functions for document operations
from gdocs.docs_helpers import (
    create_insert_text_request,
    create_delete_range_request,
    create_format_text_request,
    create_find_replace_request,
    create_insert_table_request,
    create_insert_page_break_request,
    create_insert_image_request,
    create_bullet_list_request,
    create_paragraph_style_request,
    create_delete_bullets_request,
    create_insert_table_row_request,
    create_insert_table_column_request,
    create_delete_table_row_request,
    create_delete_table_column_request,
    create_merge_table_cells_request,
    create_unmerge_table_cells_request,
    hex_to_rgb_color,
)

# Import document structure and table utilities
from gdocs.docs_structure import (
    parse_document_structure,
    find_tables,
    analyze_document_complexity
)
from gdocs.docs_tables import (
    extract_table_as_data,
    build_table_style_requests,
)

# Import operation managers for complex business logic
from gdocs.managers import (
    TableOperationManager,
    HeaderFooterManager,
    ValidationManager,
    BatchOperationManager
)

logger = logging.getLogger(__name__)

@server.tool()
@handle_http_errors("search_docs", is_read_only=True, service_type="docs")
@require_google_service("drive", "drive_read")
async def search_docs(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    query: str = Field(..., description="Search query string to match against document names. Supports partial matches (e.g., 'report' will match 'Monthly Report', 'Report 2024', etc.)."),
    page_size: int = Field(10, description="Maximum number of documents to return. Defaults to 10."),
) -> str:
    """
    Searches for Google Docs by name using Drive API (mimeType filter).

    Returns:
        str: A formatted list of Google Docs matching the search query.
    """
    logger.info(f"[search_docs] Email={user_google_email}, Query='{query}'")

    escaped_query = query.replace("'", "\\'")

    response = await asyncio.to_thread(
        service.files().list(
            q=f"name contains '{escaped_query}' and mimeType='application/vnd.google-apps.document' and trashed=false",
            pageSize=page_size,
            fields="files(id, name, createdTime, modifiedTime, webViewLink)"
        ).execute
    )
    files = response.get('files', [])
    mapped = [{
        "id": f.get("id"),
        "name": f.get("name"),
        "created": f.get("createdTime"),
        "modified": f.get("modifiedTime"),
        "link": f.get("webViewLink"),
    } for f in files]
    return success_response({"documents": mapped, "count": len(mapped)})

@server.tool()
@handle_http_errors("get_doc_content", is_read_only=True, service_type="docs")
@require_multiple_services([
    {"service_type": "drive", "scopes": "drive_read", "param_name": "drive_service"},
    {"service_type": "docs", "scopes": "docs_read", "param_name": "docs_service"}
])
async def get_doc_content(
    drive_service,
    docs_service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the Google Doc or Drive file to retrieve. For Google Docs, use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it. For Office files (.docx, etc.), use the Drive file ID."),
) -> str:
    """
    Retrieves content of a Google Doc or a Drive file (like .docx) identified by document_id.
    - Native Google Docs: Fetches content via Docs API.
    - Office files (.docx, etc.) stored in Drive: Downloads via Drive API and extracts text.

    Returns:
        str: The document content with metadata header.
    """
    logger.info(f"[get_doc_content] Invoked. Document/File ID: '{document_id}' for user '{user_google_email}'")

    # Step 2: Get file metadata from Drive
    file_metadata = await asyncio.to_thread(
        drive_service.files().get(
            fileId=document_id, fields="id, name, mimeType, webViewLink"
        ).execute
    )
    mime_type = file_metadata.get("mimeType", "")
    file_name = file_metadata.get("name", "Unknown File")
    web_view_link = file_metadata.get("webViewLink", "#")

    logger.info(f"[get_doc_content] File '{file_name}' (ID: {document_id}) has mimeType: '{mime_type}'")

    body_text = "" # Initialize body_text

    # Step 3: Process based on mimeType
    if mime_type == "application/vnd.google-apps.document":
        logger.info("[get_doc_content] Processing as native Google Doc.")
        doc_data = await asyncio.to_thread(
            docs_service.documents().get(
                documentId=document_id,
                includeTabsContent=True
            ).execute
        )
        # Tab header format constant
        TAB_HEADER_FORMAT = "\n--- TAB: {tab_name} ---\n"

        def extract_text_from_elements(elements, tab_name=None, depth=0):
            """Extract text from document elements (paragraphs, tables, etc.)"""
            # Prevent infinite recursion by limiting depth
            if depth > 5:
                return ""
            text_lines = []
            if tab_name:
                text_lines.append(TAB_HEADER_FORMAT.format(tab_name=tab_name))

            for element in elements:
                if 'paragraph' in element:
                    paragraph = element.get('paragraph', {})
                    para_elements = paragraph.get('elements', [])
                    current_line_text = ""
                    for pe in para_elements:
                        text_run = pe.get('textRun', {})
                        if text_run and 'content' in text_run:
                            current_line_text += text_run['content']
                    if current_line_text.strip():
                        text_lines.append(current_line_text)
                elif 'table' in element:
                    # Handle table content
                    table = element.get('table', {})
                    table_rows = table.get('tableRows', [])
                    for row in table_rows:
                        row_cells = row.get('tableCells', [])
                        for cell in row_cells:
                            cell_content = cell.get('content', [])
                            cell_text = extract_text_from_elements(cell_content, depth=depth + 1)
                            if cell_text.strip():
                                text_lines.append(cell_text)
            return "".join(text_lines)

        def process_tab_hierarchy(tab, level=0):
            """Process a tab and its nested child tabs recursively"""
            tab_text = ""

            if 'documentTab' in tab:
                tab_title = tab.get('documentTab', {}).get('title', 'Untitled Tab')
                # Add indentation for nested tabs to show hierarchy
                if level > 0:
                    tab_title = "    " * level + tab_title
                tab_body = tab.get('documentTab', {}).get('body', {}).get('content', [])
                tab_text += extract_text_from_elements(tab_body, tab_title)

            # Process child tabs (nested tabs)
            child_tabs = tab.get('childTabs', [])
            for child_tab in child_tabs:
                tab_text += process_tab_hierarchy(child_tab, level + 1)

            return tab_text

        processed_text_lines = []

        # Process main document body
        body_elements = doc_data.get('body', {}).get('content', [])
        main_content = extract_text_from_elements(body_elements)
        if main_content.strip():
            processed_text_lines.append(main_content)

        # Process all tabs
        tabs = doc_data.get('tabs', [])
        for tab in tabs:
            tab_content = process_tab_hierarchy(tab)
            if tab_content.strip():
                processed_text_lines.append(tab_content)

        body_text = "".join(processed_text_lines)
    else:
        logger.info(f"[get_doc_content] Processing as Drive file (e.g., .docx, other). MimeType: {mime_type}")

        export_mime_type_map = {
                # Example: "application/vnd.google-apps.spreadsheet"z: "text/csv",
                # Native GSuite types that are not Docs would go here if this function
                # was intended to export them. For .docx, direct download is used.
        }
        effective_export_mime = export_mime_type_map.get(mime_type)

        request_obj = (
            drive_service.files().export_media(fileId=document_id, mimeType=effective_export_mime)
            if effective_export_mime
            else drive_service.files().get_media(fileId=document_id)
        )

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request_obj)
        loop = asyncio.get_event_loop()
        done = False
        while not done:
            status, done = await loop.run_in_executor(None, downloader.next_chunk)

        file_content_bytes = fh.getvalue()

        office_text = extract_office_xml_text(file_content_bytes, mime_type)
        if office_text:
            body_text = office_text
        else:
            try:
                body_text = file_content_bytes.decode("utf-8")
            except UnicodeDecodeError:
                body_text = (
                    f"[Binary or unsupported text encoding for mimeType '{mime_type}' - "
                    f"{len(file_content_bytes)} bytes]"
                )

    return success_response({
        "file": {
            "id": document_id,
            "name": file_name,
            "type": mime_type,
            "link": web_view_link,
        },
        "content": body_text,
    })

@server.tool()
@handle_http_errors("list_docs_in_folder", is_read_only=True, service_type="docs")
@require_google_service("drive", "drive_read")
async def list_docs_in_folder(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    folder_id: str = Field('root', description="The ID of the Drive folder to list documents from. Use 'root' for the root of My Drive. Use the FULL ID exactly from Drive search or list operations - do NOT truncate or modify it."),
    page_size: int = Field(100, description="Maximum number of documents to return. Defaults to 100.")
) -> str:
    """
    Lists Google Docs within a specific Drive folder.

    Returns:
        str: A formatted list of Google Docs in the specified folder.
    """
    logger.info(f"[list_docs_in_folder] Invoked. Email: '{user_google_email}', Folder ID: '{folder_id}'")

    rsp = await asyncio.to_thread(
        service.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false",
            pageSize=page_size,
            fields="files(id, name, modifiedTime, webViewLink)"
        ).execute
    )
    items = rsp.get('files', [])
    mapped = [{
        "id": f.get("id"),
        "name": f.get("name"),
        "modified": f.get("modifiedTime"),
        "link": f.get("webViewLink"),
    } for f in items]
    return success_response({"folder_id": folder_id, "documents": mapped, "count": len(mapped)})

@server.tool()
@handle_http_errors("create_doc", service_type="docs")
@require_google_service("docs", "docs_write")
async def create_doc(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    title: str = Field(..., description="The title of the new Google Doc."),
    content: str = Field('', description="Optional initial content to insert into the document. If not provided, creates an empty document."),
) -> str:
    """
    Creates a new Google Doc and optionally inserts initial content.

    Returns:
        str: Confirmation message with document ID and link.
    """
    logger.info(f"[create_doc] Invoked. Email: '{user_google_email}', Title='{title}'")

    doc = await asyncio.to_thread(service.documents().create(body={'title': title}).execute)
    doc_id = doc.get('documentId')
    if content:
        requests = [{'insertText': {'location': {'index': 1}, 'text': content}}]
        await asyncio.to_thread(service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute)
    link = f"https://docs.google.com/document/d/{doc_id}/edit"
    logger.info(f"Successfully created Google Doc '{title}' (ID: {doc_id}) for {user_google_email}.")
    return success_response({"id": doc_id, "title": title, "link": link})


@server.tool()
@handle_http_errors("modify_doc_text", service_type="docs")
@require_google_service("docs", "docs_write")
async def modify_doc_text(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    start_index: int = Field(..., description="Start position for the operation (0-based index). Index 0 is the beginning of the document. Use get_doc_content to find the appropriate index."),
    end_index: int = Field(None, description="End position for text replacement or formatting. If not provided with 'text', text is inserted at 'start_index'. Required when applying formatting to existing text."),
    text: str = Field(None, description="New text to insert or replace with. If provided with 'end_index', replaces text in that range. If provided without 'end_index', inserts text at 'start_index'. Optional if only applying formatting."),
    bold: bool = Field(None, description="Whether to make text bold. Options: True (bold), False (not bold), None (leave unchanged)."),
    italic: bool = Field(None, description="Whether to make text italic. Options: True (italic), False (not italic), None (leave unchanged)."),
    underline: bool = Field(None, description="Whether to underline text. Options: True (underlined), False (not underlined), None (leave unchanged)."),
    strikethrough: bool = Field(None, description="Whether to apply strikethrough. Options: True, False, None (leave unchanged)."),
    small_caps: bool = Field(None, description="Whether to use small caps. Options: True, False, None (leave unchanged)."),
    font_size: int = Field(None, description="Font size in points (e.g., 12, 14, 16). If None, leaves font size unchanged."),
    font_family: str = Field(None, description="Font family name (e.g., 'Arial', 'Times New Roman', 'Calibri'). If None, leaves font family unchanged."),
    foreground_color: str = Field(None, description="Text color as hex string (e.g., '#FF0000' for red, '#0000FF' for blue). If None, leaves color unchanged."),
    background_color: str = Field(None, description="Text highlight/background color as hex string (e.g., '#FFFF00' for yellow). If None, leaves unchanged."),
    baseline_offset: str = Field(None, description="Baseline offset for superscript/subscript. Options: 'SUPERSCRIPT', 'SUBSCRIPT', 'NONE'. If None, leaves unchanged."),
    link_url: str = Field(None, description="URL to create a hyperlink on the text (e.g., 'https://example.com'). If None, leaves unchanged."),
) -> str:
    """
    Modifies text in a Google Doc - can insert/replace text and/or apply formatting in a single operation.

    Returns:
        str: Confirmation message with operation details
    """
    logger.info(f"[modify_doc_text] Doc={document_id}, start={start_index}, end={end_index}, text={text is not None}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    # Check if any formatting parameter is provided
    all_formatting_params = [bold, italic, underline, strikethrough, small_caps,
                             font_size, font_family, foreground_color, background_color,
                             baseline_offset, link_url]
    has_formatting = any(p is not None for p in all_formatting_params)

    # Validate that we have something to do
    if text is None and not has_formatting:
        return "Error: Must provide either 'text' to insert/replace, or formatting parameters (bold, italic, underline, strikethrough, small_caps, font_size, font_family, foreground_color, background_color, baseline_offset, link_url)."

    # Validate text formatting params if provided
    if has_formatting:
        # Only run the legacy validator when one of the params it knows about is set;
        # has_formatting already guarantees at least one param (possibly a newer one).
        if any(p is not None for p in (bold, italic, underline, font_size, font_family)):
            is_valid, error_msg = validator.validate_text_formatting_params(bold, italic, underline, font_size, font_family)
            if not is_valid:
                return f"Error: {error_msg}"

        # Validate baseline_offset
        if baseline_offset is not None and baseline_offset not in ['SUPERSCRIPT', 'SUBSCRIPT', 'NONE']:
            return "Error: baseline_offset must be 'SUPERSCRIPT', 'SUBSCRIPT', or 'NONE'."

        # For formatting, we need end_index
        if end_index is None:
            return "Error: 'end_index' is required when applying formatting."

        is_valid, error_msg = validator.validate_index_range(start_index, end_index)
        if not is_valid:
            return f"Error: {error_msg}"

    requests = []
    operations = []

    # Handle text insertion/replacement
    if text is not None:
        if end_index is not None and end_index > start_index:
            # Text replacement
            if start_index == 0:
                # Special case: Cannot delete at index 0 (first section break)
                # Instead, we insert new text at index 1 and then delete the old text
                requests.append(create_insert_text_request(1, text))
                adjusted_end = end_index + len(text)
                requests.append(create_delete_range_request(1 + len(text), adjusted_end))
                operations.append(f"Replaced text from index {start_index} to {end_index}")
            else:
                # Normal replacement: delete old text, then insert new text
                requests.extend([
                    create_delete_range_request(start_index, end_index),
                    create_insert_text_request(start_index, text)
                ])
                operations.append(f"Replaced text from index {start_index} to {end_index}")
        else:
            # Text insertion
            actual_index = 1 if start_index == 0 else start_index
            requests.append(create_insert_text_request(actual_index, text))
            operations.append(f"Inserted text at index {start_index}")

    # Handle formatting
    if has_formatting:
        # Adjust range for formatting based on text operations
        format_start = start_index
        format_end = end_index

        if text is not None:
            if end_index is not None and end_index > start_index:
                # Text was replaced - format the new text
                format_end = start_index + len(text)
            else:
                # Text was inserted - format the inserted text
                actual_index = 1 if start_index == 0 else start_index
                format_start = actual_index
                format_end = actual_index + len(text)

        # Handle special case for formatting at index 0
        if format_start == 0:
            format_start = 1
        if format_end is not None and format_end <= format_start:
            format_end = format_start + 1

        requests.append(create_format_text_request(
            format_start, format_end, bold, italic, underline, font_size, font_family,
            strikethrough=strikethrough, small_caps=small_caps,
            foreground_color=foreground_color, background_color=background_color,
            baseline_offset=baseline_offset, link_url=link_url
        ))

        format_details = []
        if bold is not None:
            format_details.append(f"bold={bold}")
        if italic is not None:
            format_details.append(f"italic={italic}")
        if underline is not None:
            format_details.append(f"underline={underline}")
        if strikethrough is not None:
            format_details.append(f"strikethrough={strikethrough}")
        if small_caps is not None:
            format_details.append(f"small_caps={small_caps}")
        if font_size:
            format_details.append(f"font_size={font_size}")
        if font_family:
            format_details.append(f"font_family={font_family}")
        if foreground_color:
            format_details.append(f"foreground_color={foreground_color}")
        if background_color:
            format_details.append(f"background_color={background_color}")
        if baseline_offset:
            format_details.append(f"baseline_offset={baseline_offset}")
        if link_url:
            format_details.append(f"link_url={link_url}")

        operations.append(f"Applied formatting ({', '.join(format_details)}) to range {format_start}-{format_end}")

    await asyncio.to_thread(
        service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': requests}
        ).execute
    )

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return success_response({
        "document_id": document_id,
        "operations": operations,
        "text_length": len(text) if text else None,
        "link": link,
    })

@server.tool()
@handle_http_errors("find_and_replace_doc", service_type="docs")
@require_google_service("docs", "docs_write")
async def find_and_replace_doc(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    find_text: str = Field(..., description="The text to search for. All occurrences of this text will be replaced."),
    replace_text: str = Field(..., description="The text to replace 'find_text' with. Can be an empty string to remove the found text."),
    match_case: bool = Field(False, description="Whether to match case exactly. If True, 'Hello' will not match 'hello'. If False, case is ignored. Defaults to False."),
) -> str:
    """
    Finds and replaces text throughout a Google Doc.

    Returns:
        str: Confirmation message with replacement count
    """
    logger.info(f"[find_and_replace_doc] Doc={document_id}, find='{find_text}', replace='{replace_text}'")

    requests = [create_find_replace_request(find_text, replace_text, match_case)]

    result = await asyncio.to_thread(
        service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': requests}
        ).execute
    )

    # Extract number of replacements from response
    replacements = 0
    if 'replies' in result and result['replies']:
        reply = result['replies'][0]
        if 'replaceAllText' in reply:
            replacements = reply['replaceAllText'].get('occurrencesChanged', 0)

    return success_response({
        "document_id": document_id,
        "find_text": find_text,
        "replace_text": replace_text,
        "occurrences_changed": replacements,
        "link": f"https://docs.google.com/document/d/{document_id}/edit",
    })


@server.tool()
@handle_http_errors("insert_doc_elements", service_type="docs")
@require_google_service("docs", "docs_write")
async def insert_doc_elements(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    element_type: str = Field(..., description="Type of element to insert. Options: 'table' (inserts a table), 'list' (inserts a bulleted or numbered list), 'page_break' (inserts a page break)."),
    index: int = Field(..., description="Position to insert element (0-based index). Index 0 is the beginning of the document. Use get_doc_content to find the appropriate index."),
    rows: int = Field(None, description="Number of rows for table. Required when element_type is 'table'. Ignored for other element types."),
    columns: int = Field(None, description="Number of columns for table. Required when element_type is 'table'. Ignored for other element types."),
    list_type: str = Field(None, description="Type of list. Options: 'UNORDERED' (bulleted list), 'ORDERED' (numbered list). Required when element_type is 'list'. Ignored for other element types."),
    text: str = Field(None, description="Initial text content for list items. Used when element_type is 'list'. If not provided, defaults to 'List item'. Ignored for other element types."),
) -> str:
    """
    Inserts structural elements like tables, lists, or page breaks into a Google Doc.

    Returns:
        str: Confirmation message with insertion details
    """
    logger.info(f"[insert_doc_elements] Doc={document_id}, type={element_type}, index={index}")

    # Handle the special case where we can't insert at the first section break
    # If index is 0, bump it to 1 to avoid the section break
    if index == 0:
        logger.debug("Adjusting index from 0 to 1 to avoid first section break")
        index = 1

    requests = []

    if element_type == "table":
        if not rows or not columns:
            return "Error: 'rows' and 'columns' parameters are required for table insertion."

        requests.append(create_insert_table_request(index, rows, columns))
        description = f"table ({rows}x{columns})"

    elif element_type == "list":
        if not list_type:
            return "Error: 'list_type' parameter is required for list insertion ('UNORDERED' or 'ORDERED')."

        if not text:
            text = "List item"

        # Insert text first, then create list
        requests.extend([
            create_insert_text_request(index, text + '\n'),
            create_bullet_list_request(index, index + len(text), list_type)
        ])
        description = f"{list_type.lower()} list"

    elif element_type == "page_break":
        requests.append(create_insert_page_break_request(index))
        description = "page break"

    else:
        return f"Error: Unsupported element type '{element_type}'. Supported types: 'table', 'list', 'page_break'."

    await asyncio.to_thread(
        service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': requests}
        ).execute
    )

    return success_response({
        "document_id": document_id,
        "element_type": element_type,
        "description": description,
        "index": index,
        "link": f"https://docs.google.com/document/d/{document_id}/edit",
    })

@server.tool()
@handle_http_errors("insert_doc_image", service_type="docs")
@require_multiple_services([
    {"service_type": "docs", "scopes": "docs_write", "param_name": "docs_service"},
    {"service_type": "drive", "scopes": "drive_read", "param_name": "drive_service"}
])
async def insert_doc_image(
    docs_service,
    drive_service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    image_source: str = Field(..., description="Source of the image. Can be a Google Drive file ID (for images stored in Drive) or a public image URL (must start with 'http://' or 'https://')."),
    index: int = Field(..., description="Position to insert image (0-based index). Index 0 is the beginning of the document. Use get_doc_content to find the appropriate index."),
    width: int = Field(None, description="Image width in points. If not provided, the image will use its natural width. If only width is provided, height will be scaled proportionally."),
    height: int = Field(None, description="Image height in points. If not provided, the image will use its natural height. If only height is provided, width will be scaled proportionally."),
) -> str:
    """
    Inserts an image into a Google Doc from Drive or a URL.

    Returns:
        str: Confirmation message with insertion details
    """
    logger.info(f"[insert_doc_image] Doc={document_id}, source={image_source}, index={index}")

    # Handle the special case where we can't insert at the first section break
    # If index is 0, bump it to 1 to avoid the section break
    if index == 0:
        logger.debug("Adjusting index from 0 to 1 to avoid first section break")
        index = 1

    # Determine if source is a Drive file ID or URL
    is_drive_file = not (image_source.startswith('http://') or image_source.startswith('https://'))

    if is_drive_file:
        # Verify Drive file exists and get metadata
        try:
            file_metadata = await asyncio.to_thread(
                drive_service.files().get(
                    fileId=image_source,
                    fields="id, name, mimeType"
                ).execute
            )
            mime_type = file_metadata.get('mimeType', '')
            if not mime_type.startswith('image/'):
                return f"Error: File {image_source} is not an image (MIME type: {mime_type})."

            image_uri = f"https://drive.google.com/uc?id={image_source}"
            source_description = f"Drive file {file_metadata.get('name', image_source)}"
        except Exception as e:
            return f"Error: Could not access Drive file {image_source}: {str(e)}"
    else:
        image_uri = image_source
        source_description = "URL image"

    # Use helper to create image request
    requests = [create_insert_image_request(index, image_uri, width, height)]

    await asyncio.to_thread(
        docs_service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': requests}
        ).execute
    )

    return success_response({
        "document_id": document_id,
        "source": source_description,
        "index": index,
        "width": width,
        "height": height,
        "link": f"https://docs.google.com/document/d/{document_id}/edit",
    })

@server.tool()
@handle_http_errors("update_doc_headers_footers", service_type="docs")
@require_google_service("docs", "docs_write")
async def update_doc_headers_footers(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    section_type: str = Field(..., description="Type of section to update. Options: 'header' (updates header), 'footer' (updates footer)."),
    content: str = Field(..., description="Text content for the header or footer."),
    header_footer_type: str = Field("DEFAULT", description="Type of header/footer. Options: 'DEFAULT' (applies to all pages), 'FIRST_PAGE_ONLY' (applies only to first page), 'EVEN_PAGE' (applies only to even-numbered pages). Defaults to 'DEFAULT'."),
) -> str:
    """
    Updates headers or footers in a Google Doc.

    Returns:
        str: Confirmation message with update details
    """
    logger.info(f"[update_doc_headers_footers] Doc={document_id}, type={section_type}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_header_footer_params(section_type, header_footer_type)
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_text_content(content)
    if not is_valid:
        return f"Error: {error_msg}"

    # Use HeaderFooterManager to handle the complex logic
    header_footer_manager = HeaderFooterManager(service)

    success, message = await header_footer_manager.update_header_footer_content(
        document_id, section_type, content, header_footer_type
    )

    if success:
        return success_response({
            "document_id": document_id,
            "section_type": section_type,
            "header_footer_type": header_footer_type,
            "link": f"https://docs.google.com/document/d/{document_id}/edit",
        })
    else:
        return f"Error: {message}"

@server.tool()
@handle_http_errors("batch_update_doc", service_type="docs")
@require_google_service("docs", "docs_write")
async def batch_update_doc(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    operations: list = Field(..., description="List of operation dictionaries to execute in a single batch. Each operation should contain: 'type' (operation type) and operation-specific parameters. Supported types: 'insert_text', 'delete_text', 'replace_text', 'format_text', 'format_paragraph', 'insert_table', 'insert_page_break', 'find_replace', 'delete_bullets', 'insert_table_row', 'insert_table_column', 'delete_table_row', 'delete_table_column', 'merge_table_cells', 'unmerge_table_cells'. Example: [{'type': 'insert_text', 'index': 1, 'text': 'Hello'}, {'type': 'format_paragraph', 'start_index': 1, 'end_index': 6, 'named_style_type': 'HEADING_1'}]"),
) -> str:
    """
    Executes multiple document operations in a single atomic batch update.

    Returns:
        str: Confirmation message with batch operation results
    """
    logger.debug(f"[batch_update_doc] Doc={document_id}, operations={len(operations)}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_batch_operations(operations)
    if not is_valid:
        return f"Error: {error_msg}"

    # Use BatchOperationManager to handle the complex logic
    batch_manager = BatchOperationManager(service)

    success, message, metadata = await batch_manager.execute_batch_operations(
        document_id, operations
    )

    if success:
        return success_response({
            "document_id": document_id,
            "operations_count": len(operations),
            "replies_count": metadata.get('replies_count', 0),
            "link": f"https://docs.google.com/document/d/{document_id}/edit",
        })
    else:
        return f"Error: {message}"

@server.tool()
@handle_http_errors("inspect_doc_structure", is_read_only=True, service_type="docs")
@require_google_service("docs", "docs_read")
async def inspect_doc_structure(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to inspect. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    detailed: bool = Field(False, description="Whether to return detailed structure information. If True, returns full element details. If False, returns basic analysis. Defaults to False."),
) -> str:
    """
    Essential tool for finding safe insertion points and understanding document structure.

    USE THIS FOR:
    - Finding the correct index for table insertion
    - Understanding document layout before making changes
    - Locating existing tables and their positions
    - Getting document statistics and complexity info

    CRITICAL FOR TABLE OPERATIONS:
    ALWAYS call this BEFORE creating tables to get a safe insertion index.

    WHAT THE OUTPUT SHOWS:
    - total_elements: Number of document elements
    - total_length: Maximum safe index for insertion
    - tables: Number of existing tables
    - table_details: Position and dimensions of each table

    WORKFLOW:
    Step 1: Call this function
    Step 2: Note the "total_length" value
    Step 3: Use an index < total_length for table insertion
    Step 4: Create your table.

    Returns:
        str: JSON string containing document structure and safe insertion indices
    """
    logger.debug(f"[inspect_doc_structure] Doc={document_id}, detailed={detailed}")

    # Get the document
    doc = await asyncio.to_thread(
        service.documents().get(documentId=document_id).execute
    )

    if detailed:
        # Return full parsed structure
        structure = parse_document_structure(doc)

        # Simplify for JSON serialization
        result = {
            'title': structure['title'],
            'total_length': structure['total_length'],
            'statistics': {
                'elements': len(structure['body']),
                'tables': len(structure['tables']),
                'paragraphs': sum(1 for e in structure['body'] if e.get('type') == 'paragraph'),
                'has_headers': bool(structure['headers']),
                'has_footers': bool(structure['footers'])
            },
            'elements': []
        }

        # Add element summaries
        for element in structure['body']:
            elem_summary = {
                'type': element['type'],
                'start_index': element['start_index'],
                'end_index': element['end_index']
            }

            if element['type'] == 'table':
                elem_summary['rows'] = element['rows']
                elem_summary['columns'] = element['columns']
                elem_summary['cell_count'] = len(element.get('cells', []))
            elif element['type'] == 'paragraph':
                elem_summary['text_preview'] = element.get('text', '')[:100]

            result['elements'].append(elem_summary)

        # Add table details
        if structure['tables']:
            result['tables'] = []
            for i, table in enumerate(structure['tables']):
                table_data = extract_table_as_data(table)
                result['tables'].append({
                    'index': i,
                    'position': {'start': table['start_index'], 'end': table['end_index']},
                    'dimensions': {'rows': table['rows'], 'columns': table['columns']},
                    'preview': table_data[:3] if table_data else []  # First 3 rows
                })

    else:
        # Return basic analysis
        result = analyze_document_complexity(doc)

        # Add table information
        tables = find_tables(doc)
        if tables:
            result['table_details'] = []
            for i, table in enumerate(tables):
                result['table_details'].append({
                    'index': i,
                    'rows': table['rows'],
                    'columns': table['columns'],
                    'start_index': table['start_index'],
                    'end_index': table['end_index']
                })

    result["document_id"] = document_id
    result["link"] = f"https://docs.google.com/document/d/{document_id}/edit"
    return success_response(result)

@server.tool()
@handle_http_errors("create_table_with_data", service_type="docs")
@require_google_service("docs", "docs_write")
async def create_table_with_data(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    table_data: list = Field(..., description="2D list of strings representing table data. Each inner list is one row. First row typically contains headers. Example: [['Header1', 'Header2'], ['Data1', 'Data2'], ['Data3', 'Data4']]. All rows must have the same number of columns. Use empty strings '' for empty cells."),
    index: int = Field(..., description="Document position for table insertion (0-based index). CRITICAL: Always get this from inspect_doc_structure 'total_length' field. Never use arbitrary index values."),
    bold_headers: bool = Field(True, description="Whether to make the first row bold. If True, the first row will be formatted as bold headers. Defaults to True."),
) -> str:
    """
    Creates a table and populates it with data in one reliable operation.

    CRITICAL: YOU MUST CALL inspect_doc_structure FIRST TO GET THE INDEX!

    MANDATORY WORKFLOW - DO THESE STEPS IN ORDER:

    Step 1: ALWAYS call inspect_doc_structure first
    Step 2: Use the 'total_length' value from inspect_doc_structure as your index
    Step 3: Format data as 2D list: [["col1", "col2"], ["row1col1", "row1col2"]]
    Step 4: Call this function with the correct index and data

    EXAMPLE DATA FORMAT:
    table_data = [
        ["Header1", "Header2", "Header3"],    # Row 0 - headers
        ["Data1", "Data2", "Data3"],          # Row 1 - first data row
        ["Data4", "Data5", "Data6"]           # Row 2 - second data row
    ]

    CRITICAL INDEX REQUIREMENTS:
    - NEVER use index values like 1, 2, 10 without calling inspect_doc_structure first
    - ALWAYS get index from inspect_doc_structure 'total_length' field
    - Index must be a valid insertion point in the document

    DATA FORMAT REQUIREMENTS:
    - Must be 2D list of strings only
    - Each inner list = one table row
    - All rows MUST have same number of columns
    - Use empty strings "" for empty cells, never None
    - Use debug_table_structure after creation to verify results.

    Returns:
        str: Confirmation with table details and link
    """
    logger.debug(f"[create_table_with_data] Doc={document_id}, index={index}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"ERROR: {error_msg}"

    is_valid, error_msg = validator.validate_table_data(table_data)
    if not is_valid:
        return f"ERROR: {error_msg}"

    is_valid, error_msg = validator.validate_index(index, "Index")
    if not is_valid:
        return f"ERROR: {error_msg}"

    # Use TableOperationManager to handle the complex logic
    table_manager = TableOperationManager(service)

    # Try to create the table, and if it fails due to index being at document end, retry with index-1
    success, message, metadata = await table_manager.create_and_populate_table(
        document_id, table_data, index, bold_headers
    )

    # If it failed due to index being at or beyond document end, retry with adjusted index
    if not success and "must be less than the end index" in message:
        logger.debug(f"Index {index} is at document boundary, retrying with index {index - 1}")
        success, message, metadata = await table_manager.create_and_populate_table(
            document_id, table_data, index - 1, bold_headers
        )

    if success:
        return success_response({
            "document_id": document_id,
            "rows": metadata.get('rows', 0),
            "columns": metadata.get('columns', 0),
            "index": index,
            "link": f"https://docs.google.com/document/d/{document_id}/edit",
        })
    else:
        return f"ERROR: {message}"


@server.tool()
@handle_http_errors("debug_table_structure", is_read_only=True, service_type="docs")
@require_google_service("docs", "docs_read")
async def debug_table_structure(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to inspect. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    table_index: int = Field(0, description="Which table to debug. Use 0 for the first table, 1 for the second table, etc. Defaults to 0."),
) -> str:
    """
    ESSENTIAL DEBUGGING TOOL - Use this whenever tables don't work as expected.

    USE THIS IMMEDIATELY WHEN:
    - Table population put data in wrong cells
    - You get "table not found" errors
    - Data appears concatenated in first cell
    - Need to understand existing table structure
    - Planning to use populate_existing_table

    WHAT THIS SHOWS YOU:
    - Exact table dimensions (rows × columns)
    - Each cell's position coordinates (row,col)
    - Current content in each cell
    - Insertion indices for each cell
    - Table boundaries and ranges

    HOW TO READ THE OUTPUT:
    - "dimensions": "2x3" = 2 rows, 3 columns
    - "position": "(0,0)" = first row, first column
    - "current_content": What's actually in each cell right now
    - "insertion_index": Where new text would be inserted in that cell

    WORKFLOW INTEGRATION:
    1. After creating table → Use this to verify structure
    2. Before populating → Use this to plan your data format
    3. After population fails → Use this to see what went wrong
    4. When debugging → Compare your data array to actual table structure.

    Returns:
        str: Detailed JSON structure showing table layout, cell positions, and current content
    """
    logger.debug(f"[debug_table_structure] Doc={document_id}, table_index={table_index}")

    # Get the document
    doc = await asyncio.to_thread(
        service.documents().get(documentId=document_id).execute
    )

    # Find tables
    tables = find_tables(doc)
    if table_index >= len(tables):
        return f"Error: Table index {table_index} not found. Document has {len(tables)} table(s)."

    table_info = tables[table_index]

    # Extract detailed cell information
    debug_info = {
        'document_id': document_id,
        'table_index': table_index,
        'dimensions': f"{table_info['rows']}x{table_info['columns']}",
        'table_range': {'start': table_info['start_index'], 'end': table_info['end_index']},
        'cells': [],
        'link': f"https://docs.google.com/document/d/{document_id}/edit",
    }

    for row_idx, row in enumerate(table_info['cells']):
        row_info = []
        for col_idx, cell in enumerate(row):
            cell_debug = {
                'row': row_idx,
                'col': col_idx,
                'range': {'start': cell['start_index'], 'end': cell['end_index']},
                'insertion_index': cell.get('insertion_index'),
                'current_content': cell.get('content', ''),
                'content_elements_count': len(cell.get('content_elements', []))
            }
            row_info.append(cell_debug)
        debug_info['cells'].append(row_info)

    return success_response(debug_info)


@server.tool()
@handle_http_errors("create_doc_header_footer", service_type="docs")
@require_google_service("docs", "docs_write")
async def create_doc_header_footer(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    section_type: str = Field(..., description="Type of section to create. Options: 'header' or 'footer'."),
    header_footer_type: str = Field("DEFAULT", description="Type of header/footer. Options: 'DEFAULT' (all pages), 'FIRST_PAGE' (first page only), 'EVEN_PAGE' (even pages only). Defaults to 'DEFAULT'."),
) -> str:
    """
    Creates a new header or footer in a Google Doc.

    Use this BEFORE update_doc_headers_footers if the document doesn't have a header/footer yet.

    Returns:
        str: Confirmation message with creation details
    """
    logger.info(f"[create_doc_header_footer] Doc={document_id}, type={section_type}, hf_type={header_footer_type}")

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    header_footer_manager = HeaderFooterManager(service)
    success, message = await header_footer_manager.create_header_footer(
        document_id, section_type, header_footer_type
    )

    if success:
        return success_response({
            "document_id": document_id,
            "section_type": section_type,
            "header_footer_type": header_footer_type,
            "link": f"https://docs.google.com/document/d/{document_id}/edit",
        })
    else:
        return f"Error: {message}"


@server.tool()
@handle_http_errors("format_doc_paragraph", service_type="docs")
@require_google_service("docs", "docs_write")
async def format_doc_paragraph(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    start_index: int = Field(..., description="Start position of the paragraph range (0-based index)."),
    end_index: int = Field(..., description="End position of the paragraph range."),
    named_style_type: str = Field(None, description="Paragraph style type. Options: 'TITLE', 'SUBTITLE', 'HEADING_1' through 'HEADING_6', 'NORMAL_TEXT'."),
    alignment: str = Field(None, description="Text alignment. Options: 'START' (left), 'CENTER', 'END' (right), 'JUSTIFIED'."),
    line_spacing: float = Field(None, description="Line spacing as percentage (e.g., 100 for single spacing, 150 for 1.5x, 200 for double spacing)."),
    space_above: float = Field(None, description="Space above paragraph in points."),
    space_below: float = Field(None, description="Space below paragraph in points."),
    indent_first_line: float = Field(None, description="First line indent in points."),
    indent_start: float = Field(None, description="Left indent in points."),
    indent_end: float = Field(None, description="Right indent in points."),
) -> str:
    """
    Applies paragraph-level formatting to a range of text in a Google Doc.

    Use this to set headings (HEADING_1-6, TITLE, SUBTITLE), alignment, line spacing, and indentation.

    Returns:
        str: Confirmation message with formatting details
    """
    logger.info(f"[format_doc_paragraph] Doc={document_id}, range={start_index}-{end_index}")

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_index_range(start_index, end_index)
    if not is_valid:
        return f"Error: {error_msg}"

    # Validate named_style_type
    valid_styles = ['TITLE', 'SUBTITLE', 'HEADING_1', 'HEADING_2', 'HEADING_3',
                    'HEADING_4', 'HEADING_5', 'HEADING_6', 'NORMAL_TEXT']
    if named_style_type is not None and named_style_type not in valid_styles:
        return f"Error: Invalid named_style_type '{named_style_type}'. Must be one of: {', '.join(valid_styles)}"

    # Validate alignment
    valid_alignments = ['START', 'CENTER', 'END', 'JUSTIFIED']
    if alignment is not None and alignment not in valid_alignments:
        return f"Error: Invalid alignment '{alignment}'. Must be one of: {', '.join(valid_alignments)}"

    request = create_paragraph_style_request(
        start_index, end_index,
        named_style_type=named_style_type,
        alignment=alignment,
        line_spacing=line_spacing,
        space_above=space_above,
        space_below=space_below,
        indent_first_line=indent_first_line,
        indent_start=indent_start,
        indent_end=indent_end
    )

    if not request:
        return "Error: At least one paragraph formatting parameter must be provided."

    await asyncio.to_thread(
        service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': [request]}
        ).execute
    )

    format_details = []
    if named_style_type:
        format_details.append(f"style={named_style_type}")
    if alignment:
        format_details.append(f"alignment={alignment}")
    if line_spacing:
        format_details.append(f"line_spacing={line_spacing}%")
    if space_above is not None:
        format_details.append(f"space_above={space_above}pt")
    if space_below is not None:
        format_details.append(f"space_below={space_below}pt")

    return success_response({
        "document_id": document_id,
        "range": {"start": start_index, "end": end_index},
        "formatting": format_details,
        "link": f"https://docs.google.com/document/d/{document_id}/edit",
    })


@server.tool()
@handle_http_errors("style_doc_table_cells", service_type="docs")
@require_google_service("docs", "docs_write")
async def style_doc_table_cells(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    table_start_index: int = Field(..., description="Starting index of the table. Get this from inspect_doc_structure or debug_table_structure."),
    border_width: float = Field(None, description="Border width in points for all cell borders."),
    border_color: str = Field(None, description="Border color as hex string (e.g., '#000000' for black)."),
    background_color: str = Field(None, description="Cell background color as hex string (e.g., '#F0F0F0' for light gray). Applies to all cells."),
    header_background: str = Field(None, description="Background color for the header row (first row) as hex string (e.g., '#4285F4' for Google blue)."),
) -> str:
    """
    Applies styling to table cells in a Google Doc (background colors, borders).

    Use inspect_doc_structure or debug_table_structure first to get the table_start_index.

    Returns:
        str: Confirmation message with styling details
    """
    logger.info(f"[style_doc_table_cells] Doc={document_id}, table_start={table_start_index}")

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    style_options = {}
    if border_width is not None:
        style_options['border_width'] = border_width
    if border_color is not None:
        style_options['border_color'] = hex_to_rgb_color(border_color)
    if background_color is not None:
        style_options['background_color'] = hex_to_rgb_color(background_color)
    if header_background is not None:
        style_options['header_background'] = hex_to_rgb_color(header_background)

    if not style_options:
        return "Error: At least one styling parameter must be provided (border_width, border_color, background_color, or header_background)."

    requests = build_table_style_requests(table_start_index, style_options)

    if not requests:
        return "Error: Could not build style requests from the provided parameters."

    await asyncio.to_thread(
        service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': requests}
        ).execute
    )

    return success_response({
        "document_id": document_id,
        "table_start_index": table_start_index,
        "styles_applied": list(style_options.keys()),
        "link": f"https://docs.google.com/document/d/{document_id}/edit",
    })


@server.tool()
@handle_http_errors("modify_doc_table", service_type="docs")
@require_google_service("docs", "docs_write")
async def modify_doc_table(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    table_start_index: int = Field(..., description="Starting index of the table. Get this from inspect_doc_structure or debug_table_structure."),
    operation: str = Field(..., description="Operation to perform. Options: 'insert_row', 'insert_column', 'delete_row', 'delete_column', 'merge_cells', 'unmerge_cells'."),
    row_index: int = Field(None, description="Row index for row operations (0-based). Required for insert_row, delete_row, merge_cells, unmerge_cells."),
    column_index: int = Field(None, description="Column index for column operations (0-based). Required for insert_column, delete_column, merge_cells, unmerge_cells."),
    insert_below: bool = Field(True, description="For insert_row: insert below the reference row. Defaults to True."),
    insert_right: bool = Field(True, description="For insert_column: insert to the right of the reference column. Defaults to True."),
    row_span: int = Field(None, description="For merge/unmerge: number of rows to span. Required for merge_cells and unmerge_cells."),
    column_span: int = Field(None, description="For merge/unmerge: number of columns to span. Required for merge_cells and unmerge_cells."),
) -> str:
    """
    Modifies table structure in a Google Doc (insert/delete rows/columns, merge/unmerge cells).

    Use inspect_doc_structure or debug_table_structure first to get the table_start_index.

    Returns:
        str: Confirmation message with operation details
    """
    logger.info(f"[modify_doc_table] Doc={document_id}, table_start={table_start_index}, op={operation}")

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    valid_operations = ['insert_row', 'insert_column', 'delete_row', 'delete_column', 'merge_cells', 'unmerge_cells']
    if operation not in valid_operations:
        return f"Error: Invalid operation '{operation}'. Must be one of: {', '.join(valid_operations)}"

    requests = []

    if operation == 'insert_row':
        if row_index is None:
            return "Error: row_index is required for insert_row operation."
        requests.append(create_insert_table_row_request(table_start_index, row_index, insert_below))
        description = f"Inserted row {'below' if insert_below else 'above'} row {row_index}"

    elif operation == 'insert_column':
        if column_index is None:
            return "Error: column_index is required for insert_column operation."
        requests.append(create_insert_table_column_request(table_start_index, column_index, insert_right))
        description = f"Inserted column {'right of' if insert_right else 'left of'} column {column_index}"

    elif operation == 'delete_row':
        if row_index is None:
            return "Error: row_index is required for delete_row operation."
        requests.append(create_delete_table_row_request(table_start_index, row_index))
        description = f"Deleted row {row_index}"

    elif operation == 'delete_column':
        if column_index is None:
            return "Error: column_index is required for delete_column operation."
        requests.append(create_delete_table_column_request(table_start_index, column_index))
        description = f"Deleted column {column_index}"

    elif operation == 'merge_cells':
        if any(v is None for v in [row_index, column_index, row_span, column_span]):
            return "Error: row_index, column_index, row_span, and column_span are all required for merge_cells."
        requests.append(create_merge_table_cells_request(table_start_index, row_index, column_index, row_span, column_span))
        description = f"Merged cells at ({row_index},{column_index}) spanning {row_span}x{column_span}"

    elif operation == 'unmerge_cells':
        if any(v is None for v in [row_index, column_index, row_span, column_span]):
            return "Error: row_index, column_index, row_span, and column_span are all required for unmerge_cells."
        requests.append(create_unmerge_table_cells_request(table_start_index, row_index, column_index, row_span, column_span))
        description = f"Unmerged cells at ({row_index},{column_index}) spanning {row_span}x{column_span}"

    await asyncio.to_thread(
        service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': requests}
        ).execute
    )

    return success_response({
        "document_id": document_id,
        "operation": operation,
        "description": description,
        "link": f"https://docs.google.com/document/d/{document_id}/edit",
    })


@server.tool()
@handle_http_errors("delete_doc_bullets", service_type="docs")
@require_google_service("docs", "docs_write")
async def delete_doc_bullets(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    start_index: int = Field(..., description="Start position of the text range to remove bullets from (0-based index)."),
    end_index: int = Field(..., description="End position of the text range to remove bullets from."),
) -> str:
    """
    Removes bullet or list formatting from a range of text in a Google Doc.

    Returns:
        str: Confirmation message
    """
    logger.info(f"[delete_doc_bullets] Doc={document_id}, range={start_index}-{end_index}")

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_index_range(start_index, end_index)
    if not is_valid:
        return f"Error: {error_msg}"

    requests = [create_delete_bullets_request(start_index, end_index)]

    await asyncio.to_thread(
        service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': requests}
        ).execute
    )

    return success_response({
        "document_id": document_id,
        "range": {"start": start_index, "end": end_index},
        "link": f"https://docs.google.com/document/d/{document_id}/edit",
    })


# Now update modify_doc_text to accept extended text styling parameters
# (The existing tool is updated via the extended create_format_text_request)


# Create comment management tools for documents
_comment_tools = create_comment_tools("document", "document_id")

# Extract and register the functions
read_doc_comments = _comment_tools['read_comments']
create_doc_comment = _comment_tools['create_comment']
reply_to_comment = _comment_tools['reply_to_comment']
resolve_comment = _comment_tools['resolve_comment']
