"""
Google Chat MCP Tools

This module provides MCP tools for interacting with Google Chat API.
"""
import logging
import asyncio
from typing import Optional, Dict, Any, List

from pydantic import Field
from googleapiclient.errors import HttpError

from auth.service_decorator import require_google_service, require_multiple_services
from core.server import server
from core.utils import handle_http_errors
from core.response import success_response

logger = logging.getLogger(__name__)

# In-memory cache for user ID → display name (bounded to avoid unbounded growth)
_SENDER_CACHE_MAX_SIZE = 256
_sender_name_cache: Dict[str, str] = {}


def _cache_sender(user_id: str, name: str) -> None:
    """Store a resolved sender name, evicting oldest entries if cache is full."""
    if len(_sender_name_cache) >= _SENDER_CACHE_MAX_SIZE:
        to_remove = list(_sender_name_cache.keys())[: _SENDER_CACHE_MAX_SIZE // 2]
        for k in to_remove:
            del _sender_name_cache[k]
    _sender_name_cache[user_id] = name


async def _resolve_sender(people_service, sender_obj: Dict[str, Any]) -> str:
    """Resolve a Chat message sender to a display name.

    Fast path: use displayName if the API already provided it.
    Slow path: look up the user via People API people.get and cache the result.
    Falls back to email, then raw user_id when nothing else is available.
    """
    display_name = sender_obj.get("displayName")
    if display_name:
        return display_name

    user_id = sender_obj.get("name", "")
    if not user_id:
        return "Unknown Sender"

    if user_id in _sender_name_cache:
        return _sender_name_cache[user_id]

    people_resource = user_id.replace("users/", "people/", 1)
    if people_service:
        try:
            person = await asyncio.to_thread(
                people_service.people()
                .get(resourceName=people_resource, personFields="names,emailAddresses")
                .execute
            )
            names = person.get("names", [])
            if names:
                resolved = names[0].get("displayName", user_id)
                _cache_sender(user_id, resolved)
                return resolved
            emails = person.get("emailAddresses", [])
            if emails:
                resolved = emails[0].get("value", user_id)
                _cache_sender(user_id, resolved)
                return resolved
        except HttpError as e:
            logger.debug(f"People API lookup failed for {user_id}: {e}")
        except Exception as e:
            logger.debug(f"Unexpected error resolving {user_id}: {e}")

    _cache_sender(user_id, user_id)
    return user_id


async def _resolve_senders_batch(people_service, senders: List[Dict[str, Any]]) -> Dict[str, str]:
    """Dedupe sender_id list + resolve serially (httplib2 is not thread-safe). Returns {sender_id: name}."""
    unique: Dict[str, Dict[str, Any]] = {}
    for s in senders:
        sid = s.get("name")
        if sid and sid not in unique:
            unique[sid] = s
    if not unique:
        return {}
    result_map: Dict[str, str] = {}
    for sid, sender_obj in unique.items():
        result_map[sid] = await _resolve_sender(people_service, sender_obj)
    return result_map


def _map_space(raw: Dict[str, Any], compact: bool = False) -> Dict[str, Any]:
    """Map a raw Chat API space to a clean response shape."""
    result = {
        "id": raw.get("name"),
        "name": raw.get("displayName"),
        "type": raw.get("spaceType"),
        "member_count": raw.get("membershipCount"),
    }
    if not compact:
        details = raw.get("spaceDetails", {})
        result["description"] = details.get("description")
        result["guidelines"] = details.get("guidelines")
        result["threaded"] = raw.get("threaded")
        result["created"] = raw.get("createTime")
        result["space_uri"] = raw.get("spaceUri")
    return result


def _map_chat_message(
    raw: Dict[str, Any],
    compact: bool = False,
    sender_name_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Map a raw Chat API message to a clean response shape."""
    sender = raw.get("sender", {})
    sender_id = sender.get("name")
    resolved_name = (sender_name_map or {}).get(sender_id) if sender_id else None
    result = {
        "id": raw.get("name"),
        "sender": sender.get("displayName") or resolved_name,
        "sender_type": sender.get("type"),
        "text": raw.get("text"),
        "thread_name": raw.get("thread", {}).get("name"),
        "created": raw.get("createTime"),
    }
    if not compact:
        result["sender_id"] = sender.get("name")
        result["formatted_text"] = raw.get("formattedText")
        result["updated"] = raw.get("lastUpdateTime")
        attachments = raw.get("attachment", [])
        if attachments:
            result["attachments"] = [
                {
                    "name": a.get("name"),
                    "content_name": a.get("contentName"),
                    "content_type": a.get("contentType"),
                }
                for a in attachments
            ]
    return result


def _map_member(
    raw: Dict[str, Any],
    sender_name_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Map a raw Chat API membership to a clean response shape."""
    member = raw.get("member", {})
    member_id = member.get("name")
    resolved_name = (sender_name_map or {}).get(member_id) if member_id else None
    return {
        "id": raw.get("name"),
        "name": member.get("displayName") or resolved_name,
        "email": member.get("email"),
        "type": member.get("type"),
        "role": raw.get("role"),
        "joined": raw.get("createTime"),
    }


@server.tool()
@handle_http_errors("list_spaces", service_type="chat", is_read_only=True)
@require_google_service("chat", "chat_read")
async def list_spaces(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    page_size: int = Field(100, description="Maximum number of spaces to return. Defaults to 100, maximum is 1000."),
    space_type: str = Field("all", description="Type of spaces to list. Options: 'all' (all spaces), 'room' (chat rooms only), 'dm' (direct messages only). Defaults to 'all'."),
    page_token: Optional[str] = Field(None, description="Token for retrieving the next page of results. Use the 'next_page_token' from the previous response to get more results."),
) -> str:
    """
    Lists Google Chat spaces (rooms and direct messages) accessible to the user.

    Returns:
        str: A formatted list of Google Chat spaces accessible to the user.
    """
    logger.info(f"[list_spaces] Email={user_google_email}, Type={space_type}")

    filter_param = None
    if space_type == "room":
        filter_param = 'spaceType = "SPACE"'
    elif space_type == "dm":
        filter_param = 'spaceType = "DIRECT_MESSAGE"'

    request_params = {"pageSize": page_size}
    if filter_param:
        request_params["filter"] = filter_param
    if page_token:
        request_params["pageToken"] = page_token

    response = await asyncio.to_thread(
        service.spaces().list(**request_params).execute
    )

    spaces = response.get('spaces', [])
    next_page_token = response.get('nextPageToken')

    mapped = [_map_space(s, compact=True) for s in spaces]
    data = {"spaces": mapped, "count": len(mapped)}
    if next_page_token:
        data["next_page_token"] = next_page_token

    return success_response(data)


@server.tool()
@handle_http_errors("get_space", service_type="chat", is_read_only=True)
@require_google_service("chat", "chat_read")
async def get_space(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    space_id: str = Field(..., description="The ID of the Google Chat space. Use the FULL ID exactly from list_spaces (e.g. 'spaces/AAAA1234') - do NOT truncate or modify it."),
) -> str:
    """
    Gets details of a specific Google Chat space.

    Returns:
        str: Space details including name, type, description, member count, and guidelines.
    """
    logger.info(f"[get_space] Space ID: '{space_id}' for user '{user_google_email}'")

    space = await asyncio.to_thread(
        service.spaces().get(name=space_id).execute
    )

    return success_response(_map_space(space, compact=False))


@server.tool()
@handle_http_errors("get_messages", service_type="chat", is_read_only=True)
@require_multiple_services([
    {"service_type": "chat", "scopes": "chat_read", "param_name": "chat_service"},
    {"service_type": "people", "scopes": "contacts_read", "param_name": "people_service"},
])
async def get_messages(
    chat_service,
    people_service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    space_id: str = Field(..., description="The ID of the Google Chat space to retrieve messages from. Use the FULL ID exactly from list_spaces - do NOT truncate or modify it."),
    page_size: int = Field(25, description="Maximum number of messages to return. Defaults to 25, maximum is 1000."),
    order_by: str = Field("createTime desc", description="Order in which to return messages. Options: 'createTime desc' (newest first), 'createTime asc' (oldest first). Defaults to 'createTime desc'."),
    page_token: Optional[str] = Field(None, description="Token for retrieving the next page of results. Use the 'next_page_token' from the previous response to get more results."),
    show_deleted: Optional[bool] = Field(None, description="Whether to include deleted messages in the results. Defaults to false."),
) -> str:
    """
    Retrieves messages from a Google Chat space.

    Returns:
        str: Formatted messages from the specified space.
    """
    logger.info(f"[get_messages] Space ID: '{space_id}' for user '{user_google_email}'")

    request_params = {
        "parent": space_id,
        "pageSize": page_size,
        "orderBy": order_by,
    }
    if page_token:
        request_params["pageToken"] = page_token
    if show_deleted is not None:
        request_params["showDeleted"] = show_deleted

    response = await asyncio.to_thread(
        chat_service.spaces().messages().list(**request_params).execute
    )

    messages = response.get('messages', [])
    next_page_token = response.get('nextPageToken')

    senders = [m.get("sender", {}) for m in messages if m.get("sender", {}).get("name")]
    sender_name_map = await _resolve_senders_batch(people_service, senders)

    mapped = [_map_chat_message(m, compact=True, sender_name_map=sender_name_map) for m in messages]
    data = {"messages": mapped, "count": len(mapped)}
    if next_page_token:
        data["next_page_token"] = next_page_token

    return success_response(data)


@server.tool()
@handle_http_errors("get_message", service_type="chat", is_read_only=True)
@require_multiple_services([
    {"service_type": "chat", "scopes": "chat_read", "param_name": "chat_service"},
    {"service_type": "people", "scopes": "contacts_read", "param_name": "people_service"},
])
async def get_message(
    chat_service,
    people_service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    message_id: str = Field(..., description="The full resource name of the message (e.g. 'spaces/AAAA1234/messages/msg1'). Use the FULL ID exactly from get_messages - do NOT truncate or modify it."),
) -> str:
    """
    Gets details of a specific message in a Google Chat space.

    Returns:
        str: Full message details including sender, text, thread, attachments, and timestamps.
    """
    logger.info(f"[get_message] Message ID: '{message_id}' for user '{user_google_email}'")

    message = await asyncio.to_thread(
        chat_service.spaces().messages().get(name=message_id).execute
    )

    sender_obj = message.get("sender", {})
    sender_name_map = {}
    if sender_obj.get("name"):
        resolved = await _resolve_sender(people_service, sender_obj)
        sender_name_map[sender_obj["name"]] = resolved

    return success_response(_map_chat_message(message, compact=False, sender_name_map=sender_name_map))


@server.tool()
@handle_http_errors("send_message", service_type="chat")
@require_google_service("chat", "chat_write")
async def send_message(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    space_id: str = Field(..., description="The ID of the Google Chat space to send the message to. Use the FULL ID exactly from list_spaces - do NOT truncate or modify it."),
    message_text: str = Field(..., description="The text content of the message to send."),
    thread_key: Optional[str] = Field(None, description="Thread key for replying to or creating a specific thread. If the thread exists, the message is sent as a reply. If not, a new thread is created with this key."),
    message_id: Optional[str] = Field(None, description="A custom ID for the message. Must start with 'client-' and contain only lowercase letters, numbers, and hyphens. Must be unique within the space."),
) -> str:
    """
    Sends a message to a Google Chat space. Supports threading via thread_key.

    Returns:
        str: Confirmation message with sent message details.
    """
    logger.info(f"[send_message] Email: '{user_google_email}', Space: '{space_id}'")

    body = {'text': message_text}

    request_params = {'parent': space_id, 'body': body}

    if thread_key:
        body['thread'] = {'threadKey': thread_key}
        request_params['messageReplyOption'] = 'REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD'

    if message_id:
        request_params['messageId'] = message_id

    message = await asyncio.to_thread(
        service.spaces().messages().create(**request_params).execute
    )

    logger.info(f"Successfully sent message to space '{space_id}' by {user_google_email}")
    return success_response({
        "message_id": message.get("name"),
        "thread_name": message.get("thread", {}).get("name"),
        "created": message.get("createTime"),
    })


@server.tool()
@handle_http_errors("update_message", service_type="chat")
@require_google_service("chat", "chat_write")
async def update_message(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    message_id: str = Field(..., description="The full resource name of the message to update (e.g. 'spaces/AAAA1234/messages/msg1'). Use the FULL ID exactly - do NOT truncate or modify it."),
    message_text: str = Field(..., description="The new text content for the message."),
) -> str:
    """
    Updates the text of an existing message in a Google Chat space. You can only update messages sent by the authenticated user.

    Returns:
        str: Confirmation with updated message details.
    """
    logger.info(f"[update_message] Message ID: '{message_id}' for user '{user_google_email}'")

    message = await asyncio.to_thread(
        service.spaces().messages().patch(
            name=message_id,
            updateMask='text',
            body={'text': message_text}
        ).execute
    )

    logger.info(f"Successfully updated message '{message_id}' by {user_google_email}")
    return success_response({
        "message_id": message.get("name"),
        "updated": message.get("lastUpdateTime"),
    })


@server.tool()
@handle_http_errors("delete_message", service_type="chat")
@require_google_service("chat", "chat_write")
async def delete_message(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    message_id: str = Field(..., description="The full resource name of the message to delete (e.g. 'spaces/AAAA1234/messages/msg1'). Use the FULL ID exactly - do NOT truncate or modify it."),
) -> str:
    """
    Deletes a message from a Google Chat space. You can only delete messages sent by the authenticated user.

    Returns:
        str: Confirmation that the message was deleted.
    """
    logger.info(f"[delete_message] Message ID: '{message_id}' for user '{user_google_email}'")

    await asyncio.to_thread(
        service.spaces().messages().delete(name=message_id).execute
    )

    logger.info(f"Deleted message '{message_id}' by {user_google_email}")
    return success_response({"deleted": True, "message_id": message_id})


@server.tool()
@handle_http_errors("search_messages", service_type="chat", is_read_only=True)
@require_multiple_services([
    {"service_type": "chat", "scopes": "chat_read", "param_name": "chat_service"},
    {"service_type": "people", "scopes": "contacts_read", "param_name": "people_service"},
])
async def search_messages(
    chat_service,
    people_service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    space_id: str = Field(..., description="The ID of the Google Chat space to search within. Use the FULL ID exactly from list_spaces - do NOT truncate or modify it."),
    query: Optional[str] = Field(None, description="Text to search for in message content. Messages are fetched using API-supported filters, then filtered client-side by this text query."),
    thread_name: Optional[str] = Field(None, description="Filter messages by thread. Use the full thread resource name (e.g. 'spaces/AAAA1234/threads/thread1') from get_messages results."),
    create_time_after: Optional[str] = Field(None, description="Only return messages created after this time. RFC-3339 format (e.g. '2026-03-01T00:00:00Z')."),
    create_time_before: Optional[str] = Field(None, description="Only return messages created before this time. RFC-3339 format (e.g. '2026-03-12T23:59:59Z')."),
    page_size: int = Field(25, description="Maximum number of messages to return. Defaults to 25, maximum is 1000."),
    page_token: Optional[str] = Field(None, description="Token for retrieving the next page of results."),
    order_by: str = Field("createTime desc", description="Order in which to return messages. Options: 'createTime desc' (newest first), 'createTime asc' (oldest first). Defaults to 'createTime desc'."),
) -> str:
    """
    Searches for messages in a Google Chat space. Supports filtering by thread, date range,
    and text content. The API supports filtering by createTime and thread.name natively;
    text matching is performed client-side after fetching.

    Returns:
        str: A formatted list of messages matching the search criteria.
    """
    logger.info(f"[search_messages] Email={user_google_email}, Space={space_id}, Query='{query}'")

    # Build API-supported filter expression
    filter_parts = []
    if thread_name:
        filter_parts.append(f'thread.name = "{thread_name}"')
    if create_time_after:
        filter_parts.append(f'createTime > "{create_time_after}"')
    if create_time_before:
        filter_parts.append(f'createTime < "{create_time_before}"')

    request_params = {
        "parent": space_id,
        "pageSize": page_size,
        "orderBy": order_by,
    }
    if filter_parts:
        request_params["filter"] = " AND ".join(filter_parts)
    if page_token:
        request_params["pageToken"] = page_token

    response = await asyncio.to_thread(
        chat_service.spaces().messages().list(**request_params).execute
    )

    messages = response.get('messages', [])
    next_page_token = response.get('nextPageToken')

    # Client-side text filtering if query is provided
    if query:
        query_lower = query.lower()
        messages = [
            m for m in messages
            if m.get("text") and query_lower in m["text"].lower()
        ]

    senders = [m.get("sender", {}) for m in messages if m.get("sender", {}).get("name")]
    sender_name_map = await _resolve_senders_batch(people_service, senders)

    mapped = [_map_chat_message(m, compact=True, sender_name_map=sender_name_map) for m in messages]
    data = {"messages": mapped, "count": len(mapped)}
    if next_page_token:
        data["next_page_token"] = next_page_token

    return success_response(data)


@server.tool()
@handle_http_errors("list_members", service_type="chat", is_read_only=True)
@require_multiple_services([
    {"service_type": "chat", "scopes": "chat_memberships_read", "param_name": "chat_service"},
    {"service_type": "people", "scopes": "contacts_read", "param_name": "people_service"},
])
async def list_members(
    chat_service,
    people_service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    space_id: str = Field(..., description="The ID of the Google Chat space. Use the FULL ID exactly from list_spaces - do NOT truncate or modify it."),
    page_size: int = Field(100, description="Maximum number of members to return. Defaults to 100, maximum is 1000."),
    filter: Optional[str] = Field(None, description="Filter by role or type. Examples: \"role = 'ROLE_MANAGER'\", \"member.type = 'HUMAN'\", \"member.type = 'HUMAN' AND role = 'ROLE_MANAGER'\"."),
    page_token: Optional[str] = Field(None, description="Token for retrieving the next page of results."),
    show_groups: Optional[bool] = Field(None, description="Whether to include Google Group memberships. Defaults to false."),
    show_invited: Optional[bool] = Field(None, description="Whether to include invited members. Defaults to false."),
) -> str:
    """
    Lists members of a Google Chat space.

    Under user OAuth, Chat API only returns {id, type, role} per member. Names are enriched
    via People API for users in the caller's contact graph (self, personal contacts, domain
    contacts, other contacts). Members outside that graph fall back to raw user IDs.

    Returns:
        str: List of space members with resolved names where available, emails, roles, and types.
    """
    logger.info(f"[list_members] Space ID: '{space_id}' for user '{user_google_email}'")

    request_params = {"parent": space_id, "pageSize": page_size}
    if filter:
        request_params["filter"] = filter
    if page_token:
        request_params["pageToken"] = page_token
    if show_groups is not None:
        request_params["showGroups"] = show_groups
    if show_invited is not None:
        request_params["showInvited"] = show_invited

    response = await asyncio.to_thread(
        chat_service.spaces().members().list(**request_params).execute
    )

    members = response.get('memberships', [])
    next_page_token = response.get('nextPageToken')

    member_objs = [
        m.get("member", {}) for m in members
        if m.get("member", {}).get("type") == "HUMAN" and m.get("member", {}).get("name")
    ]
    sender_name_map = await _resolve_senders_batch(people_service, member_objs)

    mapped = [_map_member(m, sender_name_map=sender_name_map) for m in members]
    data = {"members": mapped, "count": len(mapped)}
    if next_page_token:
        data["next_page_token"] = next_page_token

    return success_response(data)


@server.tool()
@handle_http_errors("find_direct_message", service_type="chat", is_read_only=True)
@require_google_service("chat", "chat_spaces")
async def find_direct_message(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    target_user: str = Field(..., description="The resource name of the user to find a DM with (e.g. 'users/1234567890'). Obtain this from list_members results (the 'sender_id' field in messages or 'id' prefix in member entries)."),
) -> str:
    """
    Finds an existing direct message space with a specific user.

    Returns:
        str: The DM space details if found, including the space ID for sending messages.
    """
    logger.info(f"[find_direct_message] Target: '{target_user}' for user '{user_google_email}'")

    space = await asyncio.to_thread(
        service.spaces().findDirectMessage(name=target_user).execute
    )

    return success_response(_map_space(space, compact=False))
