"""
Google Chat MCP Tools

This module provides MCP tools for interacting with Google Chat API.
"""
import logging
import asyncio
from typing import Optional, Dict, Any

from googleapiclient.errors import HttpError
from pydantic import Field

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors
from core.response import success_response

logger = logging.getLogger(__name__)


def _map_space(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw Chat API space to a clean response shape."""
    return {
        "id": raw.get("name"),
        "name": raw.get("displayName"),
        "type": raw.get("spaceType"),
    }


def _map_chat_message(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw Chat API message to a clean response shape."""
    return {
        "id": raw.get("name"),
        "sender": raw.get("sender", {}).get("displayName"),
        "text": raw.get("text"),
        "created": raw.get("createTime"),
    }


@server.tool()
@handle_http_errors("list_spaces", service_type="chat")
@require_google_service("chat", "chat_read")
async def list_spaces(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    page_size: int = Field(100, description="Maximum number of spaces to return. Defaults to 100."),
    space_type: str = Field("all", description="Type of spaces to list. Options: 'all' (all spaces), 'room' (chat rooms only), 'dm' (direct messages only). Defaults to 'all'."),
) -> str:
    """
    Lists Google Chat spaces (rooms and direct messages) accessible to the user.

    Returns:
        str: A formatted list of Google Chat spaces accessible to the user.
    """
    logger.info(f"[list_spaces] Email={user_google_email}, Type={space_type}")

    filter_param = None
    if space_type == "room":
        filter_param = "spaceType = SPACE"
    elif space_type == "dm":
        filter_param = "spaceType = DIRECT_MESSAGE"

    request_params = {"pageSize": page_size}
    if filter_param:
        request_params["filter"] = filter_param

    response = await asyncio.to_thread(
        service.spaces().list(**request_params).execute
    )

    spaces = response.get('spaces', [])
    mapped = [_map_space(s) for s in spaces]
    return success_response({"spaces": mapped, "count": len(mapped)})


@server.tool()
@handle_http_errors("get_messages", service_type="chat")
@require_google_service("chat", "chat_read")
async def get_messages(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    space_id: str = Field(..., description="The ID of the Google Chat space to retrieve messages from. Use the FULL ID exactly from list_spaces - do NOT truncate or modify it."),
    page_size: int = Field(50, description="Maximum number of messages to return. Defaults to 50."),
    order_by: str = Field("createTime desc", description="Order in which to return messages. Options: 'createTime desc' (newest first), 'createTime asc' (oldest first). Defaults to 'createTime desc'."),
) -> str:
    """
    Retrieves messages from a Google Chat space.
    Returns:
        str: Formatted messages from the specified space.
    """
    logger.info(f"[get_messages] Space ID: '{space_id}' for user '{user_google_email}'")

    space_info = await asyncio.to_thread(
        service.spaces().get(name=space_id).execute
    )
    space_name = space_info.get('displayName')

    response = await asyncio.to_thread(
        service.spaces().messages().list(
            parent=space_id,
            pageSize=page_size,
            orderBy=order_by
        ).execute
    )

    messages = response.get('messages', [])
    mapped = [_map_chat_message(m) for m in messages]
    return success_response({
        "space": {"id": space_id, "name": space_name},
        "messages": mapped,
        "count": len(mapped),
    })


@server.tool()
@handle_http_errors("send_message", service_type="chat")
@require_google_service("chat", "chat_write")
async def send_message(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    space_id: str = Field(..., description="The ID of the Google Chat space to send the message to. Use the FULL ID exactly from list_spaces - do NOT truncate or modify it."),
    message_text: str = Field(..., description="The text content of the message to send."),
    thread_key: Optional[str] = Field(None, description="Thread key for replying to a specific thread. If provided, the message will be sent as a reply in that thread. If not provided, sends a new message."),
) -> str:
    """
    Sends a message to a Google Chat space.
    Returns:
        str: Confirmation message with sent message details.
    """
    logger.info(f"[send_message] Email: '{user_google_email}', Space: '{space_id}'")

    request_params = {
        'parent': space_id,
        'body': {'text': message_text}
    }
    if thread_key:
        request_params['threadKey'] = thread_key

    message = await asyncio.to_thread(
        service.spaces().messages().create(**request_params).execute
    )

    logger.info(f"Successfully sent message to space '{space_id}' by {user_google_email}")
    return success_response({
        "message_id": message.get("name"),
        "created": message.get("createTime"),
    })


@server.tool()
@handle_http_errors("search_messages", service_type="chat")
@require_google_service("chat", "chat_read")
async def search_messages(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    query: str = Field(..., description="The search query string to match against message text content."),
    space_id: Optional[str] = Field(None, description="The ID of a specific Google Chat space to search within. If not provided, searches across all accessible spaces (limited to first 10 spaces). Use the FULL ID exactly from list_spaces - do NOT truncate or modify it."),
    page_size: int = Field(25, description="Maximum number of messages to return per space. Defaults to 25."),
) -> str:
    """
    Searches for messages in Google Chat spaces by text content.

    Returns:
        str: A formatted list of messages matching the search query.
    """
    logger.info(f"[search_messages] Email={user_google_email}, Query='{query}'")

    if space_id:
        response = await asyncio.to_thread(
            service.spaces().messages().list(
                parent=space_id,
                pageSize=page_size,
                filter=f'text:"{query}"'
            ).execute
        )
        messages = response.get('messages', [])
    else:
        spaces_response = await asyncio.to_thread(
            service.spaces().list(pageSize=100).execute
        )
        spaces = spaces_response.get('spaces', [])

        messages = []
        for space in spaces[:10]:
            try:
                space_messages = await asyncio.to_thread(
                    service.spaces().messages().list(
                        parent=space.get('name'),
                        pageSize=5,
                        filter=f'text:"{query}"'
                    ).execute
                )
                space_msgs = space_messages.get('messages', [])
                for msg in space_msgs:
                    msg['_space_name'] = space.get('displayName')
                messages.extend(space_msgs)
            except HttpError:
                continue  # Skip spaces we can't access

    mapped = []
    for msg in messages:
        m = _map_chat_message(msg)
        if '_space_name' in msg:
            m['space_name'] = msg['_space_name']
        mapped.append(m)

    return success_response({"messages": mapped, "count": len(mapped)})
