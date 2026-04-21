"""
Google Drive MCP Tools

This module provides MCP tools for interacting with Google Drive API.
"""
import logging
import asyncio
import re
from typing import Optional, Dict, Any, List, Literal

from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io
import httpx

from pydantic import Field

from auth.service_decorator import require_google_service
from core.utils import extract_office_xml_text, handle_http_errors
from core.response import success_response
from core.server import server

logger = logging.getLogger(__name__)


def _map_file(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw Drive API file object to a clean response shape."""
    result = {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "type": raw.get("mimeType"),
        "link": raw.get("webViewLink"),
        "modified": raw.get("modifiedTime"),
    }
    if "size" in raw:
        result["size"] = raw.get("size")
    if "iconLink" in raw:
        result["icon"] = raw.get("iconLink")
    return result


def _map_file_detailed(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw Drive API file object to a detailed response shape."""
    result = _map_file(raw)
    for field in ("description", "starred", "trashed", "createdTime",
                  "owners", "sharingUser", "shared", "capabilities"):
        if field in raw:
            result[field] = raw.get(field)
    if "permissions" in raw:
        result["permissions"] = [
            {
                "id": p.get("id"),
                "role": p.get("role"),
                "type": p.get("type"),
                "emailAddress": p.get("emailAddress"),
                "displayName": p.get("displayName"),
            }
            for p in raw.get("permissions", [])
        ]
    if "parents" in raw:
        result["parents"] = raw.get("parents")
    return result


# Precompiled regex patterns for Drive query detection
DRIVE_QUERY_PATTERNS = [
    re.compile(r'\b\w+\s*(=|!=|>|<)\s*[\'"].*?[\'"]', re.IGNORECASE),  # field = 'value'
    re.compile(r'\b\w+\s*(=|!=|>|<)\s*\d+', re.IGNORECASE),            # field = number
    re.compile(r'\bcontains\b', re.IGNORECASE),                         # contains operator
    re.compile(r'\bin\s+parents\b', re.IGNORECASE),                     # in parents
    re.compile(r'\bhas\s*\{', re.IGNORECASE),                          # has {properties}
    re.compile(r'\btrashed\s*=\s*(true|false)\b', re.IGNORECASE),      # trashed=true/false
    re.compile(r'\bstarred\s*=\s*(true|false)\b', re.IGNORECASE),      # starred=true/false
    re.compile(r'[\'"][^\'"]+[\'"]\s+in\s+parents', re.IGNORECASE),    # 'parentId' in parents
    re.compile(r'\bfullText\s+contains\b', re.IGNORECASE),             # fullText contains
    re.compile(r'\bname\s*(=|contains)\b', re.IGNORECASE),             # name = or name contains
    re.compile(r'\bmimeType\s*(=|!=)\b', re.IGNORECASE),               # mimeType operators
]


def _build_drive_list_params(
    query: str,
    page_size: int,
    drive_id: Optional[str] = None,
    include_items_from_all_drives: bool = True,
    corpora: Optional[str] = None,
    page_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Helper function to build common list parameters for Drive API calls.

    Returns:
        Dictionary of parameters for Drive API list calls
    """
    list_params = {
        "q": query,
        "pageSize": page_size,
        "fields": "nextPageToken, files(id, name, mimeType, webViewLink, iconLink, modifiedTime, size)",
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": include_items_from_all_drives,
    }

    if page_token:
        list_params["pageToken"] = page_token

    if drive_id:
        list_params["driveId"] = drive_id
        if corpora:
            list_params["corpora"] = corpora
        else:
            list_params["corpora"] = "drive"
    elif corpora:
        list_params["corpora"] = corpora

    return list_params


# ─── Read Tools ───────────────────────────────────────────────────────────────

@server.tool()
@handle_http_errors("search_drive_files", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def search_drive_files(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    query: str = Field(..., description="The search query string. Supports Google Drive search operators (e.g., \"name contains 'report'\", \"mimeType='application/vnd.google-apps.document'\", \"'folder_id' in parents\")."),
    page_size: int = Field(10, description="The maximum number of files to return. Defaults to 10."),
    drive_id: Optional[str] = Field(None, description="ID of the shared drive to search. If None, behavior depends on `corpora` and `include_items_from_all_drives`."),
    include_items_from_all_drives: bool = Field(True, description="Whether shared drive items should be included in results. Defaults to True. This is effective when not specifying a `drive_id`."),
    corpora: Optional[str] = Field(None, description="Bodies of items to query. Options: 'user' (My Drive only), 'domain' (domain-wide), 'drive' (specific shared drive), 'allDrives' (all accessible). If 'drive_id' is specified and 'corpora' is None, it defaults to 'drive'. Prefer 'user' or 'drive' over 'allDrives' for efficiency."),
    page_token: Optional[str] = Field(None, description="Token for fetching the next page of results, returned as 'next_page_token' from a previous search."),
) -> str:
    """
    **ONLY use when the user explicitly mentions Google Drive/Workspace** (e.g., "search my Drive").
    DO NOT use for generic file searches.

    Searches for files and folders within a user's Google Drive, including shared drives.

    Returns:
        str: A formatted list of found files/folders with their details (ID, name, type, size, modified time, link).
    """
    logger.info(f"[search_drive_files] Invoked. Email: '{user_google_email}', Query: '{query}'")

    # Check if the query looks like a structured Drive query or free text
    is_structured_query = any(pattern.search(query) for pattern in DRIVE_QUERY_PATTERNS)

    if is_structured_query:
        final_query = query
        logger.info(f"[search_drive_files] Using structured query as-is: '{final_query}'")
    else:
        # For free text queries, wrap in fullText contains and exclude trashed
        escaped_query = query.replace("'", "\\'")
        final_query = f"fullText contains '{escaped_query}' and trashed=false"
        logger.info(f"[search_drive_files] Reformatting free text query '{query}' to '{final_query}'")

    list_params = _build_drive_list_params(
        query=final_query,
        page_size=page_size,
        drive_id=drive_id,
        include_items_from_all_drives=include_items_from_all_drives,
        corpora=corpora,
        page_token=page_token,
    )

    results = await asyncio.to_thread(
        service.files().list(**list_params).execute
    )
    files = results.get('files', [])
    next_page_token = results.get('nextPageToken')

    mapped = [_map_file(f) for f in files]
    data = {"files": mapped, "count": len(mapped)}
    if next_page_token:
        data["next_page_token"] = next_page_token
    return success_response(data)


@server.tool()
@handle_http_errors("get_drive_file_content", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def get_drive_file_content(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    file_id: str = Field(..., description="The Google Drive file ID. Use the FULL ID exactly from search_drive_files, list_drive_items, or create_drive_file - do NOT truncate or modify it."),
) -> str:
    """
    **ONLY use when the user explicitly mentions reading from Google Drive/Workspace.**
    DO NOT use for generic file reading requests.

    Retrieves the content of a specific Google Drive file by ID, supporting files in shared drives.

    • Native Google Docs, Sheets, Slides → exported as text / CSV.
    • Office files (.docx, .xlsx, .pptx) → unzipped & parsed with std-lib to
      extract readable text.
    • Any other file → downloaded; tries UTF-8 decode, else notes binary.

    Returns:
        str: The file content as plain text with metadata header.
    """
    logger.info(f"[get_drive_file_content] Invoked. File ID: '{file_id}'")

    file_metadata = await asyncio.to_thread(
        service.files().get(
            fileId=file_id, fields="id, name, mimeType, webViewLink", supportsAllDrives=True
        ).execute
    )
    mime_type = file_metadata.get("mimeType", "")
    file_name = file_metadata.get("name", "Unknown File")
    export_mime_type = {
        "application/vnd.google-apps.document": "text/plain",
        "application/vnd.google-apps.spreadsheet": "text/csv",
        "application/vnd.google-apps.presentation": "text/plain",
    }.get(mime_type)

    request_obj = (
        service.files().export_media(fileId=file_id, mimeType=export_mime_type)
        if export_mime_type
        else service.files().get_media(fileId=file_id)
    )
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request_obj)
    done = False
    while not done:
        status, done = await asyncio.to_thread(downloader.next_chunk)

    file_content_bytes = fh.getvalue()

    # Attempt Office XML extraction only for actual Office XML files
    office_mime_types = {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }

    if mime_type in office_mime_types:
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
            "id": file_id,
            "name": file_name,
            "type": mime_type,
            "link": file_metadata.get("webViewLink"),
        },
        "content": body_text,
    })


@server.tool()
@handle_http_errors("get_drive_file_metadata", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def get_drive_file_metadata(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    file_id: str = Field(..., description="The Google Drive file ID."),
) -> str:
    """
    **ONLY use when the user explicitly mentions Google Drive/Workspace.**

    Retrieves detailed metadata for a Google Drive file including owners, permissions,
    sharing status, size, and parent folders.

    Returns:
        str: Detailed file metadata as JSON.
    """
    logger.info(f"[get_drive_file_metadata] Invoked. File ID: '{file_id}'")

    file_metadata = await asyncio.to_thread(
        service.files().get(
            fileId=file_id,
            fields="id, name, mimeType, webViewLink, iconLink, modifiedTime, createdTime, size, description, starred, trashed, parents, owners, sharingUser, shared, permissions(id, role, type, emailAddress, displayName), capabilities",
            supportsAllDrives=True,
        ).execute
    )

    return success_response({"file": _map_file_detailed(file_metadata)})


@server.tool()
@handle_http_errors("list_drive_items", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def list_drive_items(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    folder_id: str = Field('root', description="The ID of the Google Drive folder. Use 'root' for the root of My Drive. For a shared drive, this can be the shared drive's ID to list its root, or a folder ID within that shared drive. Use the FULL ID exactly from search_drive_files or list_drive_items - do NOT truncate or modify it."),
    page_size: int = Field(100, description="The maximum number of items to return. Defaults to 100."),
    drive_id: Optional[str] = Field(None, description="ID of the shared drive. If provided, the listing is scoped to this drive. If not provided, lists items from user's 'My Drive' and accessible shared drives (if include_items_from_all_drives is True)."),
    include_items_from_all_drives: bool = Field(True, description="Whether items from all accessible shared drives should be included if drive_id is not set. Defaults to True."),
    corpora: Optional[str] = Field(None, description="Corpus to query. Options: 'user' (My Drive only), 'drive' (specific shared drive), 'allDrives' (all accessible). If drive_id is set and corpora is None, 'drive' is used. If None and no drive_id, API defaults apply."),
    page_token: Optional[str] = Field(None, description="Token for fetching the next page of results, returned as 'next_page_token' from a previous list call."),
) -> str:
    """
    **ONLY use when the user explicitly mentions listing Google Drive/Workspace folders.**
    DO NOT use for generic folder listing requests.

    Lists files and folders, supporting shared drives.
    If `drive_id` is specified, lists items within that shared drive. `folder_id` is then relative to that drive (or use drive_id as folder_id for root).
    If `drive_id` is not specified, lists items from user's "My Drive" and accessible shared drives (if `include_items_from_all_drives` is True).

    Returns:
        str: A formatted list of files/folders in the specified folder.
    """
    logger.info(f"[list_drive_items] Invoked. Email: '{user_google_email}', Folder ID: '{folder_id}'")

    final_query = f"'{folder_id}' in parents and trashed=false"

    list_params = _build_drive_list_params(
        query=final_query,
        page_size=page_size,
        drive_id=drive_id,
        include_items_from_all_drives=include_items_from_all_drives,
        corpora=corpora,
        page_token=page_token,
    )

    results = await asyncio.to_thread(
        service.files().list(**list_params).execute
    )
    files = results.get('files', [])
    next_page_token = results.get('nextPageToken')

    mapped = [_map_file(f) for f in files]
    data = {"items": mapped, "count": len(mapped)}
    if next_page_token:
        data["next_page_token"] = next_page_token
    return success_response(data)


@server.tool()
@handle_http_errors("list_shared_drives", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def list_shared_drives(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    page_size: int = Field(100, description="Maximum number of shared drives to return. Defaults to 100."),
    page_token: Optional[str] = Field(None, description="Token for fetching the next page of results."),
) -> str:
    """
    **ONLY use when the user explicitly mentions Google Drive shared drives.**

    Lists all shared drives accessible to the user. Use the returned drive IDs
    with search_drive_files or list_drive_items to browse shared drive contents.

    Returns:
        str: List of shared drives with their IDs and names.
    """
    logger.info(f"[list_shared_drives] Invoked. Email: '{user_google_email}'")

    params: Dict[str, Any] = {"pageSize": page_size}
    if page_token:
        params["pageToken"] = page_token

    results = await asyncio.to_thread(
        service.drives().list(**params).execute
    )
    drives = results.get("drives", [])
    next_token = results.get("nextPageToken")

    mapped = [
        {
            "id": d.get("id"),
            "name": d.get("name"),
            "kind": d.get("kind"),
        }
        for d in drives
    ]
    data = {"drives": mapped, "count": len(mapped)}
    if next_token:
        data["next_page_token"] = next_token
    return success_response(data)


# ─── Write Tools ──────────────────────────────────────────────────────────────

@server.tool()
@handle_http_errors("create_drive_file", service_type="drive")
@require_google_service("drive", "drive_file")
async def create_drive_file(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    file_name: str = Field(..., description="The name for the new file or folder."),
    content: Optional[str] = Field(None, description="The content to write to the file. Either 'content' or 'fileUrl' must be provided. Not required when creating a folder."),
    folder_id: str = Field('root', description="The ID of the parent folder. Use 'root' for the root of My Drive. For shared drives, this must be a folder ID within the shared drive. Use the FULL ID exactly from search_drive_files or list_drive_items - do NOT truncate or modify it."),
    mime_type: str = Field('text/plain', description="The MIME type of the file. Examples: 'text/plain', 'text/html', 'application/json', 'image/png'. Use 'application/vnd.google-apps.folder' to create a folder. Defaults to 'text/plain'. If fileUrl is provided, the MIME type may be automatically detected from the Content-Type header."),
    fileUrl: Optional[str] = Field(None, description="Public URL to fetch the file content from. Either 'content' or 'fileUrl' must be provided. The file will be downloaded from this URL and uploaded to Google Drive."),
) -> str:
    """
    **ONLY use when the user explicitly mentions saving to Google Drive/Workspace** (e.g., "save to my Drive").
    For generic requests like "create a file", provide content directly in chat instead.

    Creates a new file or folder in Google Drive, supporting creation within shared drives.
    Accepts either direct content or a fileUrl to fetch the content from.
    To create a folder, set mime_type to 'application/vnd.google-apps.folder'.

    Returns:
        str: Confirmation message of the successful file creation with file link.
    """
    logger.info(f"[create_drive_file] Invoked. Email: '{user_google_email}', File Name: {file_name}, Folder ID: {folder_id}, fileUrl: {fileUrl}")

    is_folder = mime_type == "application/vnd.google-apps.folder"

    if not is_folder and not content and not fileUrl:
        raise Exception("You must provide either 'content' or 'fileUrl' (unless creating a folder).")

    file_metadata: Dict[str, Any] = {
        'name': file_name,
        'parents': [folder_id],
        'mimeType': mime_type,
    }

    if is_folder:
        created_file = await asyncio.to_thread(
            service.files().create(
                body=file_metadata,
                fields='id, name, webViewLink',
                supportsAllDrives=True,
            ).execute
        )
    else:
        file_data = None
        if fileUrl:
            logger.info(f"[create_drive_file] Fetching file from URL: {fileUrl}")
            async with httpx.AsyncClient() as client:
                resp = await client.get(fileUrl)
                if resp.status_code != 200:
                    raise Exception(f"Failed to fetch file from URL: {fileUrl} (status {resp.status_code})")
                file_data = await resp.aread()
                content_type = resp.headers.get("Content-Type")
                if content_type and content_type != "application/octet-stream":
                    mime_type = content_type
                    logger.info(f"[create_drive_file] Using MIME type from Content-Type header: {mime_type}")
        elif content:
            file_data = content.encode('utf-8')

        media = io.BytesIO(file_data)

        created_file = await asyncio.to_thread(
            service.files().create(
                body=file_metadata,
                media_body=MediaIoBaseUpload(media, mimetype=mime_type, resumable=True),
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute
        )

    logger.info(f"Successfully created file. Link: {created_file.get('webViewLink')}")
    return success_response({
        "file": {
            "id": created_file.get("id"),
            "name": created_file.get("name"),
            "link": created_file.get("webViewLink"),
        }
    })


@server.tool()
@handle_http_errors("update_drive_file", service_type="drive")
@require_google_service("drive", "drive_file")
async def update_drive_file(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    file_id: str = Field(..., description="The Google Drive file ID to update."),
    new_name: Optional[str] = Field(None, description="New name for the file. Leave empty to keep current name."),
    new_content: Optional[str] = Field(None, description="New text content for the file. Leave empty to keep current content."),
    description: Optional[str] = Field(None, description="New description for the file. Leave empty to keep current description."),
    mime_type: Optional[str] = Field(None, description="MIME type for the new content. Required if new_content is provided. Examples: 'text/plain', 'text/html', 'application/json'."),
) -> str:
    """
    **ONLY use when the user explicitly mentions updating a file on Google Drive/Workspace.**

    Updates an existing Google Drive file's metadata (name, description) and/or content.
    At least one of new_name, new_content, or description must be provided.

    Returns:
        str: Updated file details with link.
    """
    logger.info(f"[update_drive_file] Invoked. File ID: '{file_id}'")

    if not new_name and not new_content and description is None:
        raise Exception("At least one of 'new_name', 'new_content', or 'description' must be provided.")

    file_metadata: Dict[str, Any] = {}
    if new_name:
        file_metadata["name"] = new_name
    if description is not None:
        file_metadata["description"] = description

    if new_content:
        resolved_mime = mime_type or "text/plain"
        media = io.BytesIO(new_content.encode("utf-8"))
        updated_file = await asyncio.to_thread(
            service.files().update(
                fileId=file_id,
                body=file_metadata if file_metadata else None,
                media_body=MediaIoBaseUpload(media, mimetype=resolved_mime, resumable=True),
                fields="id, name, webViewLink, modifiedTime",
                supportsAllDrives=True,
            ).execute
        )
    else:
        updated_file = await asyncio.to_thread(
            service.files().update(
                fileId=file_id,
                body=file_metadata,
                fields="id, name, webViewLink, modifiedTime",
                supportsAllDrives=True,
            ).execute
        )

    return success_response({
        "file": {
            "id": updated_file.get("id"),
            "name": updated_file.get("name"),
            "link": updated_file.get("webViewLink"),
            "modified": updated_file.get("modifiedTime"),
        }
    })


@server.tool()
@handle_http_errors("copy_drive_file", service_type="drive")
@require_google_service("drive", "drive_file")
async def copy_drive_file(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    file_id: str = Field(..., description="The Google Drive file ID to copy."),
    new_name: Optional[str] = Field(None, description="Name for the copy. If not provided, defaults to 'Copy of <original name>'."),
    folder_id: Optional[str] = Field(None, description="The ID of the destination folder. If not provided, the copy is placed in the same folder as the original."),
) -> str:
    """
    **ONLY use when the user explicitly mentions copying a file on Google Drive/Workspace.**

    Creates a copy of an existing Google Drive file, optionally with a new name and/or in a different folder.

    Returns:
        str: Details of the newly copied file.
    """
    logger.info(f"[copy_drive_file] Invoked. File ID: '{file_id}'")

    body: Dict[str, Any] = {}
    if new_name:
        body["name"] = new_name
    if folder_id:
        body["parents"] = [folder_id]

    copied_file = await asyncio.to_thread(
        service.files().copy(
            fileId=file_id,
            body=body,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        ).execute
    )

    return success_response({
        "file": {
            "id": copied_file.get("id"),
            "name": copied_file.get("name"),
            "link": copied_file.get("webViewLink"),
        }
    })


@server.tool()
@handle_http_errors("move_drive_file", service_type="drive")
@require_google_service("drive", "drive_full")
async def move_drive_file(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    file_id: str = Field(..., description="The Google Drive file ID to move."),
    destination_folder_id: str = Field(..., description="The ID of the destination folder."),
) -> str:
    """
    **ONLY use when the user explicitly mentions moving a file on Google Drive/Workspace.**

    Moves a file from its current folder(s) to a new destination folder.

    Returns:
        str: Updated file details after the move.
    """
    logger.info(f"[move_drive_file] Invoked. File ID: '{file_id}', Destination: '{destination_folder_id}'")

    # Get current parents to remove them
    file_metadata = await asyncio.to_thread(
        service.files().get(
            fileId=file_id, fields="parents", supportsAllDrives=True
        ).execute
    )
    current_parents = ",".join(file_metadata.get("parents", []))

    moved_file = await asyncio.to_thread(
        service.files().update(
            fileId=file_id,
            addParents=destination_folder_id,
            removeParents=current_parents,
            fields="id, name, webViewLink, parents",
            supportsAllDrives=True,
        ).execute
    )

    return success_response({
        "file": {
            "id": moved_file.get("id"),
            "name": moved_file.get("name"),
            "link": moved_file.get("webViewLink"),
            "parents": moved_file.get("parents"),
        }
    })


@server.tool()
@handle_http_errors("delete_drive_file", service_type="drive")
@require_google_service("drive", "drive_full")
async def delete_drive_file(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    file_id: str = Field(..., description="The Google Drive file ID to delete or trash."),
    permanent: bool = Field(False, description="If True, permanently deletes the file (cannot be recovered). If False (default), moves the file to trash."),
) -> str:
    """
    **ONLY use when the user explicitly mentions deleting/trashing a file on Google Drive/Workspace.**

    Deletes or trashes a Google Drive file. By default, moves to trash (recoverable).
    Set permanent=True to permanently delete (irreversible).

    Returns:
        str: Confirmation of the deletion or trash action.
    """
    logger.info(f"[delete_drive_file] Invoked. File ID: '{file_id}', Permanent: {permanent}")

    if permanent:
        await asyncio.to_thread(
            service.files().delete(
                fileId=file_id, supportsAllDrives=True
            ).execute
        )
        return success_response({"status": "permanently_deleted", "file_id": file_id})
    else:
        updated_file = await asyncio.to_thread(
            service.files().update(
                fileId=file_id,
                body={"trashed": True},
                fields="id, name, trashed",
                supportsAllDrives=True,
            ).execute
        )
        return success_response({
            "status": "trashed",
            "file": {
                "id": updated_file.get("id"),
                "name": updated_file.get("name"),
                "trashed": updated_file.get("trashed"),
            }
        })


@server.tool()
@handle_http_errors("share_drive_file", service_type="drive")
@require_google_service("drive", "drive_full")
async def share_drive_file(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    file_id: str = Field(..., description="The Google Drive file ID to share."),
    role: Literal["reader", "writer", "commenter"] = Field(..., description="The role to grant. Options: 'reader' (view only), 'writer' (can edit), 'commenter' (can comment)."),
    type: Literal["user", "group", "domain", "anyone"] = Field(..., description="The type of grantee. 'user' or 'group' requires email_address. 'domain' requires domain. 'anyone' makes the file accessible via link."),
    email_address: Optional[str] = Field(None, description="Email address of the user or group to share with. Required when type is 'user' or 'group'."),
    domain: Optional[str] = Field(None, description="Domain to share with (e.g., 'example.com'). Required when type is 'domain'."),
    send_notification: bool = Field(True, description="Whether to send a notification email to the grantee. Defaults to True."),
) -> str:
    """
    **ONLY use when the user explicitly mentions sharing a file on Google Drive/Workspace.**

    Shares a Google Drive file with a user, group, domain, or anyone (link sharing).

    Returns:
        str: Details of the created permission.
    """
    logger.info(f"[share_drive_file] Invoked. File ID: '{file_id}', Role: '{role}', Type: '{type}'")

    if type in ("user", "group") and not email_address:
        raise Exception(f"'email_address' is required when type is '{type}'.")
    if type == "domain" and not domain:
        raise Exception("'domain' is required when type is 'domain'.")

    permission_body: Dict[str, Any] = {"role": role, "type": type}
    if email_address:
        permission_body["emailAddress"] = email_address
    if domain:
        permission_body["domain"] = domain

    created_permission = await asyncio.to_thread(
        service.permissions().create(
            fileId=file_id,
            body=permission_body,
            sendNotificationEmail=send_notification,
            fields="id, role, type, emailAddress, displayName",
            supportsAllDrives=True,
        ).execute
    )

    return success_response({
        "permission": {
            "id": created_permission.get("id"),
            "role": created_permission.get("role"),
            "type": created_permission.get("type"),
            "emailAddress": created_permission.get("emailAddress"),
            "displayName": created_permission.get("displayName"),
        }
    })
