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

# Import helper functions for document operations
from gdocs.docs_helpers import (
    create_insert_text_request,
    create_delete_range_request,
    create_format_text_request,
    create_find_replace_request,
    create_insert_table_request,
    create_insert_page_break_request,
    create_insert_image_request,
    create_bullet_list_request
)

# Import document structure and table utilities
from gdocs.docs_structure import (
    parse_document_structure,
    find_tables,
    analyze_document_complexity
)
from gdocs.docs_tables import (
    extract_table_as_data
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
    if not files:
        return f"No Google Docs found matching '{query}'."

    output = [f"Found {len(files)} Google Docs matching '{query}':"]
    for f in files:
        output.append(
            f"- {f['name']} (ID: {f['id']}) Modified: {f.get('modifiedTime')} Link: {f.get('webViewLink')}"
        )
    return "\n".join(output)

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

    header = (
        f'File: "{file_name}" (ID: {document_id}, Type: {mime_type})\n'
        f'Link: {web_view_link}\n\n--- CONTENT ---\n'
    )
    return header + body_text

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
    if not items:
        return f"No Google Docs found in folder '{folder_id}'."
    out = [f"Found {len(items)} Docs in folder '{folder_id}':"]
    for f in items:
        out.append(f"- {f['name']} (ID: {f['id']}) Modified: {f.get('modifiedTime')} Link: {f.get('webViewLink')}")
    return "\n".join(out)

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
    msg = f"Created Google Doc '{title}' (ID: {doc_id}) for {user_google_email}. Link: {link}"
    logger.info(f"Successfully created Google Doc '{title}' (ID: {doc_id}) for {user_google_email}. Link: {link}")
    return msg


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
    font_size: int = Field(None, description="Font size in points (e.g., 12, 14, 16). If None, leaves font size unchanged."),
    font_family: str = Field(None, description="Font family name (e.g., 'Arial', 'Times New Roman', 'Calibri'). If None, leaves font family unchanged."),
) -> str:
    """
    Modifies text in a Google Doc - can insert/replace text and/or apply formatting in a single operation.

    Returns:
        str: Confirmation message with operation details
    """
    logger.info(f"[modify_doc_text] Doc={document_id}, start={start_index}, end={end_index}, text={text is not None}, formatting={any([bold, italic, underline, font_size, font_family])}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    # Validate that we have something to do
    if text is None and not any([bold is not None, italic is not None, underline is not None, font_size, font_family]):
        return "Error: Must provide either 'text' to insert/replace, or formatting parameters (bold, italic, underline, font_size, font_family)."

    # Validate text formatting params if provided
    if any([bold is not None, italic is not None, underline is not None, font_size, font_family]):
        is_valid, error_msg = validator.validate_text_formatting_params(bold, italic, underline, font_size, font_family)
        if not is_valid:
            return f"Error: {error_msg}"

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
    if any([bold is not None, italic is not None, underline is not None, font_size, font_family]):
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

        requests.append(create_format_text_request(format_start, format_end, bold, italic, underline, font_size, font_family))

        format_details = []
        if bold is not None:
            format_details.append(f"bold={bold}")
        if italic is not None:
            format_details.append(f"italic={italic}")
        if underline is not None:
            format_details.append(f"underline={underline}")
        if font_size:
            format_details.append(f"font_size={font_size}")
        if font_family:
            format_details.append(f"font_family={font_family}")

        operations.append(f"Applied formatting ({', '.join(format_details)}) to range {format_start}-{format_end}")

    await asyncio.to_thread(
        service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': requests}
        ).execute
    )

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    operation_summary = "; ".join(operations)
    text_info = f" Text length: {len(text)} characters." if text else ""
    return f"{operation_summary} in document {document_id}.{text_info} Link: {link}"

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

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Replaced {replacements} occurrence(s) of '{find_text}' with '{replace_text}' in document {document_id}. Link: {link}"


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

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Inserted {description} at index {index} in document {document_id}. Link: {link}"

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

    size_info = ""
    if width or height:
        size_info = f" (size: {width or 'auto'}x{height or 'auto'} points)"

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Inserted {source_description}{size_info} at index {index} in document {document_id}. Link: {link}"

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
        link = f"https://docs.google.com/document/d/{document_id}/edit"
        return f"{message}. Link: {link}"
    else:
        return f"Error: {message}"

@server.tool()
@handle_http_errors("batch_update_doc", service_type="docs")
@require_google_service("docs", "docs_write")
async def batch_update_doc(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    document_id: str = Field(..., description="The ID of the document to update. Use the FULL ID exactly from search_docs, list_docs_in_folder, or create_doc - do NOT truncate or modify it."),
    operations: list = Field(..., description="List of operation dictionaries to execute in a single batch. Each operation should contain: 'type' (operation type) and operation-specific parameters. Supported types: 'insert_text', 'delete_text', 'replace_text', 'format_text', 'insert_table', 'insert_page_break'. Example: [{'type': 'insert_text', 'index': 1, 'text': 'Hello'}, {'type': 'format_text', 'start_index': 1, 'end_index': 6, 'bold': True}]"),
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
        link = f"https://docs.google.com/document/d/{document_id}/edit"
        replies_count = metadata.get('replies_count', 0)
        return f"{message} on document {document_id}. API replies: {replies_count}. Link: {link}"
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

    import json
    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Document structure analysis for {document_id}:\n\n{json.dumps(result, indent=2)}\n\nLink: {link}"

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
        link = f"https://docs.google.com/document/d/{document_id}/edit"
        rows = metadata.get('rows', 0)
        columns = metadata.get('columns', 0)

        return f"SUCCESS: {message}. Table: {rows}x{columns}, Index: {index}. Link: {link}"
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

    import json

    # Extract detailed cell information
    debug_info = {
        'table_index': table_index,
        'dimensions': f"{table_info['rows']}x{table_info['columns']}",
        'table_range': f"[{table_info['start_index']}-{table_info['end_index']}]",
        'cells': []
    }

    for row_idx, row in enumerate(table_info['cells']):
        row_info = []
        for col_idx, cell in enumerate(row):
            cell_debug = {
                'position': f"({row_idx},{col_idx})",
                'range': f"[{cell['start_index']}-{cell['end_index']}]",
                'insertion_index': cell.get('insertion_index', 'N/A'),
                'current_content': repr(cell.get('content', '')),
                'content_elements_count': len(cell.get('content_elements', []))
            }
            row_info.append(cell_debug)
        debug_info['cells'].append(row_info)

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Table structure debug for table {table_index}:\n\n{json.dumps(debug_info, indent=2)}\n\nLink: {link}"


# Create comment management tools for documents
_comment_tools = create_comment_tools("document", "document_id")

# Extract and register the functions
read_doc_comments = _comment_tools['read_comments']
create_doc_comment = _comment_tools['create_comment']
reply_to_comment = _comment_tools['reply_to_comment']
resolve_comment = _comment_tools['resolve_comment']
