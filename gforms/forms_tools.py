"""
Google Forms MCP Tools

This module provides MCP tools for interacting with Google Forms API.
"""

import logging
import asyncio
from typing import Optional, Dict, Any, List

from pydantic import Field

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors
from core.response import success_response

logger = logging.getLogger(__name__)


def _map_question(item: Dict[str, Any], index: int) -> Dict[str, Any]:
    """Map a raw form item to a clean question shape."""
    question_item = item.get("questionItem", {})
    question = question_item.get("question", {})
    return {
        "index": index,
        "title": item.get("title"),
        "required": question.get("required", False),
    }


def _map_form_response(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw form response to a clean shape."""
    answers_raw = raw.get("answers", {})
    answers = []
    for question_id, answer_data in answers_raw.items():
        text_answers = answer_data.get("textAnswers", {}).get("answers", [])
        values = [ans.get("value") for ans in text_answers if ans.get("value")]
        answers.append({
            "question_id": question_id,
            "values": values if values else None,
        })
    return {
        "response_id": raw.get("responseId"),
        "created": raw.get("createTime"),
        "last_submitted": raw.get("lastSubmittedTime"),
        "answer_count": len(answers_raw),
        "answers": answers,
    }


@server.tool()
@handle_http_errors("create_form", service_type="forms")
@require_google_service("forms", "forms")
async def create_form(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    title: str = Field(..., description="The title of the form."),
    description: Optional[str] = Field(None, description="The description of the form. This appears at the top of the form to provide context or instructions to respondents."),
    document_title: Optional[str] = Field(None, description="The document title shown in the browser tab. If not provided, uses the form title."),
) -> str:
    """
    Create a new form using the title given in the provided form message in the request.

    Returns:
        str: Confirmation message with form ID and edit URL.
    """
    logger.info(f"[create_form] Invoked. Email: '{user_google_email}', Title: {title}")

    form_body: Dict[str, Any] = {
        "info": {
            "title": title
        }
    }

    if description:
        form_body["info"]["description"] = description

    if document_title:
        form_body["info"]["document_title"] = document_title

    created_form = await asyncio.to_thread(
        service.forms().create(body=form_body).execute
    )

    form_id = created_form.get("formId")
    return success_response({
        "form_id": form_id,
        "title": created_form.get("info", {}).get("title"),
        "edit_url": f"https://docs.google.com/forms/d/{form_id}/edit",
        "responder_url": created_form.get("responderUri"),
    })


@server.tool()
@handle_http_errors("get_form", is_read_only=True, service_type="forms")
@require_google_service("forms", "forms")
async def get_form(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form to retrieve. Obtain from the form's edit URL (https://docs.google.com/forms/d/{form_id}/edit) or from create_form. Use the FULL ID - do NOT truncate or modify it."),
) -> str:
    """
    Get a form.

    Returns:
        str: Form details including title, description, questions, and URLs.
    """
    logger.info(f"[get_form] Invoked. Email: '{user_google_email}', Form ID: {form_id}")

    form = await asyncio.to_thread(
        service.forms().get(formId=form_id).execute
    )

    form_info = form.get("info", {})
    items = form.get("items", [])
    questions = [_map_question(item, i) for i, item in enumerate(items, 1)]

    return success_response({
        "form_id": form_id,
        "title": form_info.get("title"),
        "description": form_info.get("description"),
        "document_title": form_info.get("documentTitle"),
        "edit_url": f"https://docs.google.com/forms/d/{form_id}/edit",
        "responder_url": form.get("responderUri"),
        "questions": questions,
        "question_count": len(items),
    })


@server.tool()
@handle_http_errors("set_publish_settings", service_type="forms")
@require_google_service("forms", "forms")
async def set_publish_settings(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form to update publish settings for. Obtain from the form's edit URL or from create_form. Use the FULL ID - do NOT truncate or modify it."),
    publish_as_template: bool = Field(False, description="Whether to publish the form as a template. If True, the form can be used as a template by others. Defaults to False."),
    require_authentication: bool = Field(False, description="Whether to require authentication to view or submit the form. If True, only authenticated users can access the form. Defaults to False."),
) -> str:
    """
    Updates the publish settings of a form.

    Returns:
        str: Confirmation message of the successful publish settings update.
    """
    logger.info(f"[set_publish_settings] Invoked. Email: '{user_google_email}', Form ID: {form_id}")

    settings_body = {
        "publishAsTemplate": publish_as_template,
        "requireAuthentication": require_authentication
    }

    await asyncio.to_thread(
        service.forms().setPublishSettings(formId=form_id, body=settings_body).execute
    )

    logger.info(f"Publish settings updated successfully for {user_google_email}. Form ID: {form_id}")
    return success_response({
        "form_id": form_id,
        "publish_as_template": publish_as_template,
        "require_authentication": require_authentication,
    })


@server.tool()
@handle_http_errors("get_form_response", is_read_only=True, service_type="forms")
@require_google_service("forms", "forms")
async def get_form_response(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form. Obtain from the form's edit URL or from create_form. Use the FULL ID - do NOT truncate or modify it."),
    response_id: str = Field(..., description="The ID of the response to retrieve. Use the FULL ID exactly from list_form_responses - do NOT truncate or modify it."),
) -> str:
    """
    Get one response from the form.

    Returns:
        str: Response details including answers and metadata.
    """
    logger.info(f"[get_form_response] Invoked. Email: '{user_google_email}', Form ID: {form_id}, Response ID: {response_id}")

    response = await asyncio.to_thread(
        service.forms().responses().get(formId=form_id, responseId=response_id).execute
    )

    mapped = _map_form_response(response)
    mapped["form_id"] = form_id
    return success_response(mapped)


@server.tool()
@handle_http_errors("list_form_responses", is_read_only=True, service_type="forms")
@require_google_service("forms", "forms")
async def list_form_responses(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form. Obtain from the form's edit URL or from create_form. Use the FULL ID - do NOT truncate or modify it."),
    page_size: int = Field(10, description="Maximum number of responses to return per page. Defaults to 10."),
    page_token: Optional[str] = Field(None, description="Token for retrieving the next page of results. Use the 'next_page_token' from the previous response to get more results."),
) -> str:
    """
    List a form's responses.

    Returns:
        str: List of responses with basic details and pagination info.
    """
    logger.info(f"[list_form_responses] Invoked. Email: '{user_google_email}', Form ID: {form_id}")

    params = {
        "formId": form_id,
        "pageSize": page_size
    }
    if page_token:
        params["pageToken"] = page_token

    responses_result = await asyncio.to_thread(
        service.forms().responses().list(**params).execute
    )

    responses = responses_result.get("responses", [])
    next_page_token = responses_result.get("nextPageToken")

    mapped = []
    for r in responses:
        mapped.append({
            "response_id": r.get("responseId"),
            "created": r.get("createTime"),
            "last_submitted": r.get("lastSubmittedTime"),
            "answer_count": len(r.get("answers", {})),
        })

    return success_response({
        "form_id": form_id,
        "responses": mapped,
        "count": len(mapped),
        "next_page_token": next_page_token,
    })
