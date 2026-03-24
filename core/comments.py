"""
Core Comments Module

This module provides reusable comment management functions for Google Workspace applications.
All Google Workspace apps (Docs, Sheets, Slides) use the Drive API for comment operations.
"""

import logging
import asyncio

from pydantic import Field

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors
from core.response import success_response

logger = logging.getLogger(__name__)


def _map_comment(raw):
    """Map a raw comment to a clean shape."""
    replies_raw = raw.get('replies', [])
    replies = [{
        "id": r.get("id"),
        "content": r.get("content"),
        "author": r.get("author", {}).get("displayName"),
        "created": r.get("createdTime"),
    } for r in replies_raw]

    return {
        "id": raw.get("id"),
        "content": raw.get("content"),
        "author": raw.get("author", {}).get("displayName"),
        "created": raw.get("createdTime"),
        "resolved": raw.get("resolved", False),
        "replies": replies if replies else None,
        "reply_count": len(replies_raw),
    }


def create_comment_tools(app_name: str, file_id_param: str):
    """
    Factory function to create comment management tools for a specific Google Workspace app.

    Returns:
        Dict containing the four comment management functions with unique names
    """

    # Create unique function names based on the app type
    read_func_name = f"read_{app_name}_comments"
    create_func_name = f"create_{app_name}_comment"
    reply_func_name = f"reply_to_{app_name}_comment"
    resolve_func_name = f"resolve_{app_name}_comment"

    # Create functions without decorators first, then apply decorators with proper names
    if file_id_param == "document_id":
        @require_google_service("drive", "drive_read")
        @handle_http_errors(read_func_name, service_type="drive")
        async def read_comments(service, user_google_email: str = Field(..., description="The user's Google email address."), document_id: str = Field(..., description="The ID of the Google Document to read comments from. Obtain this from search_docs or list_docs_in_folder results.")) -> str:
            """Read all comments from a Google Document."""
            return await _read_comments_impl(service, app_name, document_id)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(create_func_name, service_type="drive")
        async def create_comment(service, user_google_email: str = Field(..., description="The user's Google email address."), document_id: str = Field(..., description="The ID of the Google Document to add a comment to. Obtain this from search_docs or list_docs_in_folder results."), comment_content: str = Field(..., description="The text content of the comment to create.")) -> str:
            """Create a new comment on a Google Document."""
            return await _create_comment_impl(service, app_name, document_id, comment_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(reply_func_name, service_type="drive")
        async def reply_to_comment(service, user_google_email: str = Field(..., description="The user's Google email address."), document_id: str = Field(..., description="The ID of the Google Document containing the comment. Obtain this from search_docs or list_docs_in_folder results."), comment_id: str = Field(..., description="The ID of the comment to reply to. Obtain this from read_doc_comments results."), reply_content: str = Field(..., description="The text content of the reply.")) -> str:
            """Reply to a specific comment in a Google Document."""
            return await _reply_to_comment_impl(service, app_name, document_id, comment_id, reply_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(resolve_func_name, service_type="drive")
        async def resolve_comment(service, user_google_email: str = Field(..., description="The user's Google email address."), document_id: str = Field(..., description="The ID of the Google Document containing the comment. Obtain this from search_docs or list_docs_in_folder results."), comment_id: str = Field(..., description="The ID of the comment to resolve. Obtain this from read_doc_comments results.")) -> str:
            """Resolve a comment in a Google Document."""
            return await _resolve_comment_impl(service, app_name, document_id, comment_id)

    elif file_id_param == "spreadsheet_id":
        @require_google_service("drive", "drive_read")
        @handle_http_errors(read_func_name, service_type="drive")
        async def read_comments(service, user_google_email: str = Field(..., description="The user's Google email address."), spreadsheet_id: str = Field(..., description="The ID of the Google Spreadsheet to read comments from. Obtain this from list_spreadsheets results.")) -> str:
            """Read all comments from a Google Spreadsheet."""
            return await _read_comments_impl(service, app_name, spreadsheet_id)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(create_func_name, service_type="drive")
        async def create_comment(service, user_google_email: str = Field(..., description="The user's Google email address."), spreadsheet_id: str = Field(..., description="The ID of the Google Spreadsheet to add a comment to. Obtain this from list_spreadsheets results."), comment_content: str = Field(..., description="The text content of the comment to create.")) -> str:
            """Create a new comment on a Google Spreadsheet."""
            return await _create_comment_impl(service, app_name, spreadsheet_id, comment_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(reply_func_name, service_type="drive")
        async def reply_to_comment(service, user_google_email: str = Field(..., description="The user's Google email address."), spreadsheet_id: str = Field(..., description="The ID of the Google Spreadsheet containing the comment. Obtain this from list_spreadsheets results."), comment_id: str = Field(..., description="The ID of the comment to reply to. Obtain this from read_sheet_comments results."), reply_content: str = Field(..., description="The text content of the reply.")) -> str:
            """Reply to a specific comment in a Google Spreadsheet."""
            return await _reply_to_comment_impl(service, app_name, spreadsheet_id, comment_id, reply_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(resolve_func_name, service_type="drive")
        async def resolve_comment(service, user_google_email: str = Field(..., description="The user's Google email address."), spreadsheet_id: str = Field(..., description="The ID of the Google Spreadsheet containing the comment. Obtain this from list_spreadsheets results."), comment_id: str = Field(..., description="The ID of the comment to resolve. Obtain this from read_sheet_comments results.")) -> str:
            """Resolve a comment in a Google Spreadsheet."""
            return await _resolve_comment_impl(service, app_name, spreadsheet_id, comment_id)

    elif file_id_param == "presentation_id":
        @require_google_service("drive", "drive_read")
        @handle_http_errors(read_func_name, service_type="drive")
        async def read_comments(service, user_google_email: str = Field(..., description="The user's Google email address."), presentation_id: str = Field(..., description="The ID of the Google Presentation to read comments from. Obtain this from the presentation's edit URL or from presentation creation results.")) -> str:
            """Read all comments from a Google Presentation. Returns JSON with comments array. Each comment has: id, content, author, created, resolved, replies[] (each with id, content, author, created), reply_count."""
            return await _read_comments_impl(service, app_name, presentation_id)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(create_func_name, service_type="drive")
        async def create_comment(service, user_google_email: str = Field(..., description="The user's Google email address."), presentation_id: str = Field(..., description="The ID of the Google Presentation to add a comment to. Obtain this from the presentation's edit URL or from presentation creation results."), comment_content: str = Field(..., description="The text content of the comment to create.")) -> str:
            """Create a new comment on a Google Presentation. Returns JSON with the created comment: id, content, author, created."""
            return await _create_comment_impl(service, app_name, presentation_id, comment_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(reply_func_name, service_type="drive")
        async def reply_to_comment(service, user_google_email: str = Field(..., description="The user's Google email address."), presentation_id: str = Field(..., description="The ID of the Google Presentation containing the comment. Obtain this from the presentation's edit URL or from presentation creation results."), comment_id: str = Field(..., description="The ID of the comment to reply to. Obtain this from read_presentation_comments results."), reply_content: str = Field(..., description="The text content of the reply.")) -> str:
            """Reply to a specific comment in a Google Presentation. Returns JSON with the reply: id, content, author, created."""
            return await _reply_to_comment_impl(service, app_name, presentation_id, comment_id, reply_content)

        @require_google_service("drive", "drive_file")
        @handle_http_errors(resolve_func_name, service_type="drive")
        async def resolve_comment(service, user_google_email: str = Field(..., description="The user's Google email address."), presentation_id: str = Field(..., description="The ID of the Google Presentation containing the comment. Obtain this from the presentation's edit URL or from presentation creation results."), comment_id: str = Field(..., description="The ID of the comment to resolve. Obtain this from read_presentation_comments results.")) -> str:
            """Resolve a comment in a Google Presentation. Returns JSON with resolved=true, comment_id, and reply_id."""
            return await _resolve_comment_impl(service, app_name, presentation_id, comment_id)

    # Set the proper function names and register with server
    read_comments.__name__ = read_func_name
    create_comment.__name__ = create_func_name
    reply_to_comment.__name__ = reply_func_name
    resolve_comment.__name__ = resolve_func_name

    # Register tools with the server using the proper names
    server.tool()(read_comments)
    server.tool()(create_comment)
    server.tool()(reply_to_comment)
    server.tool()(resolve_comment)

    return {
        'read_comments': read_comments,
        'create_comment': create_comment,
        'reply_to_comment': reply_to_comment,
        'resolve_comment': resolve_comment
    }


async def _read_comments_impl(service, app_name: str, file_id: str) -> str:
    """Implementation for reading comments from any Google Workspace file."""
    logger.info(f"[read_{app_name}_comments] Reading comments for {app_name} {file_id}")

    response = await asyncio.to_thread(
        service.comments().list(
            fileId=file_id,
            fields="comments(id,content,author,createdTime,modifiedTime,resolved,replies(content,author,id,createdTime,modifiedTime))"
        ).execute
    )

    comments = response.get('comments', [])
    mapped = [_map_comment(c) for c in comments]

    return success_response({
        "comments": mapped,
        "count": len(mapped),
    })


async def _create_comment_impl(service, app_name: str, file_id: str, comment_content: str) -> str:
    """Implementation for creating a comment on any Google Workspace file."""
    logger.info(f"[create_{app_name}_comment] Creating comment in {app_name} {file_id}")

    body = {"content": comment_content}

    comment = await asyncio.to_thread(
        service.comments().create(
            fileId=file_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime"
        ).execute
    )

    return success_response({
        "comment": {
            "id": comment.get("id"),
            "content": comment.get("content"),
            "author": comment.get("author", {}).get("displayName"),
            "created": comment.get("createdTime"),
        },
    })


async def _reply_to_comment_impl(service, app_name: str, file_id: str, comment_id: str, reply_content: str) -> str:
    """Implementation for replying to a comment on any Google Workspace file."""
    logger.info(f"[reply_to_{app_name}_comment] Replying to comment {comment_id} in {app_name} {file_id}")

    body = {'content': reply_content}

    reply = await asyncio.to_thread(
        service.replies().create(
            fileId=file_id,
            commentId=comment_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime"
        ).execute
    )

    return success_response({
        "reply": {
            "id": reply.get("id"),
            "content": reply.get("content"),
            "author": reply.get("author", {}).get("displayName"),
            "created": reply.get("createdTime"),
        },
    })


async def _resolve_comment_impl(service, app_name: str, file_id: str, comment_id: str) -> str:
    """Implementation for resolving a comment on any Google Workspace file."""
    logger.info(f"[resolve_{app_name}_comment] Resolving comment {comment_id} in {app_name} {file_id}")

    body = {
        "content": "This comment has been resolved.",
        "action": "resolve"
    }

    reply = await asyncio.to_thread(
        service.replies().create(
            fileId=file_id,
            commentId=comment_id,
            body=body,
            fields="id,content,author,createdTime,modifiedTime"
        ).execute
    )

    return success_response({
        "resolved": True,
        "comment_id": comment_id,
        "reply_id": reply.get("id"),
    })
