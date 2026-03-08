"""
Google Slides MCP Tools

This module provides MCP tools for interacting with Google Slides API.
"""

import logging
import asyncio
from typing import List, Dict, Any

from pydantic import Field

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors
from core.response import success_response
from core.comments import create_comment_tools

logger = logging.getLogger(__name__)


def _map_slide(slide: Dict[str, Any], index: int) -> Dict[str, Any]:
    """Map a raw slide to a clean shape."""
    return {
        "index": index,
        "id": slide.get("objectId"),
        "element_count": len(slide.get("pageElements", [])),
    }


def _map_page_element(element: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw page element to a clean shape."""
    mapped = {"id": element.get("objectId")}
    if "shape" in element:
        mapped["type"] = "shape"
        mapped["shape_type"] = element["shape"].get("shapeType")
    elif "table" in element:
        mapped["type"] = "table"
        mapped["rows"] = element["table"].get("rows")
        mapped["columns"] = element["table"].get("columns")
    elif "line" in element:
        mapped["type"] = "line"
        mapped["line_type"] = element["line"].get("lineType")
    elif "image" in element:
        mapped["type"] = "image"
    else:
        mapped["type"] = "unknown"
    return mapped


@server.tool()
@handle_http_errors("create_presentation", service_type="slides")
@require_google_service("slides", "slides")
async def create_presentation(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    title: str = Field("Untitled Presentation", description="The title for the new presentation. Defaults to 'Untitled Presentation'."),
) -> str:
    """
    Create a new Google Slides presentation.

    Returns:
        str: Details about the created presentation including ID and URL.
    """
    logger.info(f"[create_presentation] Invoked. Email: '{user_google_email}', Title: '{title}'")

    body = {
        'title': title
    }

    result = await asyncio.to_thread(
        service.presentations().create(body=body).execute
    )

    presentation_id = result.get('presentationId')
    logger.info(f"Presentation created successfully for {user_google_email}")
    return success_response({
        "id": presentation_id,
        "title": title,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
        "slide_count": len(result.get('slides', [])),
    })


@server.tool()
@handle_http_errors("get_presentation", is_read_only=True, service_type="slides")
@require_google_service("slides", "slides_read")
async def get_presentation(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The ID of the presentation to retrieve. Obtain from the presentation's edit URL (https://docs.google.com/presentation/d/{presentation_id}/edit) or from create_presentation. Use the FULL ID - do NOT truncate or modify it."),
) -> str:
    """
    Get details about a Google Slides presentation.

    Returns:
        str: Details about the presentation including title, slides count, and metadata.
    """
    logger.info(f"[get_presentation] Invoked. Email: '{user_google_email}', ID: '{presentation_id}'")

    result = await asyncio.to_thread(
        service.presentations().get(presentationId=presentation_id).execute
    )

    slides = result.get('slides', [])
    page_size = result.get('pageSize', {})
    width = page_size.get('width', {})
    height = page_size.get('height', {})

    logger.info(f"Presentation retrieved successfully for {user_google_email}")
    return success_response({
        "id": presentation_id,
        "title": result.get('title'),
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
        "slide_count": len(slides),
        "page_size": {
            "width": width.get('magnitude'),
            "height": height.get('magnitude'),
            "unit": width.get('unit'),
        } if width else None,
        "slides": [_map_slide(s, i) for i, s in enumerate(slides, 1)],
    })


@server.tool()
@handle_http_errors("batch_update_presentation", service_type="slides")
@require_google_service("slides", "slides")
async def batch_update_presentation(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The ID of the presentation to update. Obtain from the presentation's edit URL or from create_presentation. Use the FULL ID - do NOT truncate or modify it."),
    requests: List[Dict[str, Any]] = Field(..., description="List of update requests to apply. Each request is a dictionary containing the operation type and parameters. Supported operations include creating slides, shapes, tables, inserting text, etc."),
) -> str:
    """
    Apply batch updates to a Google Slides presentation.

    Returns:
        str: Details about the batch update operation results.
    """
    logger.info(f"[batch_update_presentation] Invoked. Email: '{user_google_email}', ID: '{presentation_id}', Requests: {len(requests)}")

    body = {
        'requests': requests
    }

    result = await asyncio.to_thread(
        service.presentations().batchUpdate(
            presentationId=presentation_id,
            body=body
        ).execute
    )

    replies = result.get('replies', [])
    reply_details = []
    for reply in replies:
        if 'createSlide' in reply:
            reply_details.append({"type": "createSlide", "id": reply['createSlide'].get('objectId')})
        elif 'createShape' in reply:
            reply_details.append({"type": "createShape", "id": reply['createShape'].get('objectId')})
        elif 'createTable' in reply:
            reply_details.append({"type": "createTable", "id": reply['createTable'].get('objectId')})
        else:
            reply_details.append({"type": "other"})

    logger.info(f"Batch update completed successfully for {user_google_email}")
    return success_response({
        "id": presentation_id,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
        "requests_applied": len(requests),
        "replies": reply_details,
    })


@server.tool()
@handle_http_errors("get_page", is_read_only=True, service_type="slides")
@require_google_service("slides", "slides_read")
async def get_page(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The ID of the presentation. Obtain from the presentation's edit URL or from create_presentation. Use the FULL ID - do NOT truncate or modify it."),
    page_object_id: str = Field(..., description="The object ID of the page/slide to retrieve. Use the FULL ID exactly from get_presentation - do NOT truncate or modify it."),
) -> str:
    """
    Get details about a specific page (slide) in a presentation.

    Returns:
        str: Details about the specific page including elements and layout.
    """
    logger.info(f"[get_page] Invoked. Email: '{user_google_email}', Presentation: '{presentation_id}', Page: '{page_object_id}'")

    result = await asyncio.to_thread(
        service.presentations().pages().get(
            presentationId=presentation_id,
            pageObjectId=page_object_id
        ).execute
    )

    page_elements = result.get('pageElements', [])

    logger.info(f"Page retrieved successfully for {user_google_email}")
    return success_response({
        "presentation_id": presentation_id,
        "page_id": page_object_id,
        "page_type": result.get('pageType'),
        "element_count": len(page_elements),
        "elements": [_map_page_element(e) for e in page_elements],
    })


@server.tool()
@handle_http_errors("get_page_thumbnail", is_read_only=True, service_type="slides")
@require_google_service("slides", "slides_read")
async def get_page_thumbnail(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The ID of the presentation. Obtain from the presentation's edit URL or from create_presentation. Use the FULL ID - do NOT truncate or modify it."),
    page_object_id: str = Field(..., description="The object ID of the page/slide. Use the FULL ID exactly from get_presentation - do NOT truncate or modify it."),
    thumbnail_size: str = Field("MEDIUM", description="Size of thumbnail. Options: 'LARGE' (largest size), 'MEDIUM' (medium size), 'SMALL' (smallest size). Defaults to 'MEDIUM'."),
) -> str:
    """
    Generate a thumbnail URL for a specific page (slide) in a presentation.

    Returns:
        str: URL to the generated thumbnail image.
    """
    logger.info(f"[get_page_thumbnail] Invoked. Email: '{user_google_email}', Presentation: '{presentation_id}', Page: '{page_object_id}', Size: '{thumbnail_size}'")

    result = await asyncio.to_thread(
        service.presentations().pages().getThumbnail(
            presentationId=presentation_id,
            pageObjectId=page_object_id,
            thumbnailProperties_thumbnailSize=thumbnail_size,
            thumbnailProperties_mimeType='PNG'
        ).execute
    )

    logger.info(f"Thumbnail generated successfully for {user_google_email}")
    return success_response({
        "presentation_id": presentation_id,
        "page_id": page_object_id,
        "size": thumbnail_size,
        "thumbnail_url": result.get('contentUrl'),
    })


# Create comment management tools for slides
_comment_tools = create_comment_tools("presentation", "presentation_id")
read_presentation_comments = _comment_tools['read_comments']
create_presentation_comment = _comment_tools['create_comment']
reply_to_presentation_comment = _comment_tools['reply_to_comment']
resolve_presentation_comment = _comment_tools['resolve_comment']

# Aliases for backwards compatibility and intuitive naming
read_slide_comments = read_presentation_comments
create_slide_comment = create_presentation_comment
reply_to_slide_comment = reply_to_presentation_comment
resolve_slide_comment = resolve_presentation_comment
