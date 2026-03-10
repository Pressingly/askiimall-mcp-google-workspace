"""
Shared response formatting utilities for MCP tool responses.

All tools should use success_response() and error_response() to wrap their
return values in a consistent JSON envelope.
"""

import json
from typing import Any


def success_response(data: Any) -> str:
    """Wrap successful tool output in standard envelope.

    Args:
        data: The response payload (dict, list, or primitive).

    Returns:
        JSON string: {"success": true, "data": <data>}
    """
    return json.dumps({"success": True, "data": data}, default=str)


def error_response(code: int, message: str, retryable: bool = False) -> str:
    """Wrap error output in standard envelope.

    Args:
        code: HTTP status code from the Google API error.
        message: Human-readable error description.
        retryable: Whether the caller should retry the request.

    Returns:
        JSON string: {"success": false, "error": {"code": ..., "message": ..., "retryable": ...}}
    """
    return json.dumps({
        "success": False,
        "error": {"code": code, "message": message, "retryable": retryable}
    })


# Google API fields that should be stripped from responses
NOISE_KEYS = {
    "kind", "etag", "iCalUID", "sequence", "self", "resource",
    "additionalGuests", "endTimeUnspecified", "privateCopy",
    "anyoneCanAddSelf", "guestsCanInviteOthers", "guestsCanModify",
    "guestsCanSeeOtherGuests",
}

# Standard field renames for LLM clarity
FIELD_RENAMES = {
    "summary": "title",
    "htmlLink": "link",
    "displayName": "name",
    "responseStatus": "response",
    "fileUrl": "url",
    "mimeType": "type",
}
