"""
Google Forms MCP Tools

This module provides MCP tools for interacting with Google Forms API.
"""

import logging
import asyncio
from typing import Optional, Dict, Any

from pydantic import Field

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors
from core.response import success_response

logger = logging.getLogger(__name__)


def _question_type(question: Dict[str, Any]) -> Optional[str]:
    """Derive a friendly question type from a raw question object."""
    if "choiceQuestion" in question:
        return question["choiceQuestion"].get("type")
    if "textQuestion" in question:
        return "PARAGRAPH" if question["textQuestion"].get("paragraph") else "SHORT_TEXT"
    if "scaleQuestion" in question:
        return "SCALE"
    if "dateQuestion" in question:
        return "DATE"
    if "timeQuestion" in question:
        return "TIME"
    return None


def _map_question(item: Dict[str, Any], index: int) -> Dict[str, Any]:
    """Map a raw form item to a clean question shape."""
    question_item = item.get("questionItem", {})
    question = question_item.get("question", {})
    options = [
        opt.get("value")
        for opt in question.get("choiceQuestion", {}).get("options", [])
    ]
    return {
        "index": index,
        "item_id": item.get("itemId"),
        "question_id": question.get("questionId"),
        "title": item.get("title"),
        "type": _question_type(question),
        "required": question.get("required", False),
        "options": options or None,
    }


async def _batch_update(service, form_id: str, requests: list) -> Dict[str, Any]:
    """Execute a batchUpdate call on the Forms API."""
    return await asyncio.to_thread(
        service.forms().batchUpdate(formId=form_id, body={"requests": requests}).execute
    )


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

    # The Forms API only allows info.title and info.documentTitle at creation;
    # description (and everything else) must be applied afterwards via batchUpdate.
    info: Dict[str, Any] = {"title": title}
    if document_title:
        info["documentTitle"] = document_title

    created_form = await asyncio.to_thread(
        service.forms().create(body={"info": info}).execute
    )

    form_id = created_form.get("formId")

    if description:
        await _batch_update(service, form_id, [{
            "updateFormInfo": {
                "info": {"description": description},
                "updateMask": "description",
            }
        }])

    return success_response({
        "form_id": form_id,
        "title": created_form.get("info", {}).get("title"),
        "description": description,
        "edit_url": f"https://docs.google.com/forms/d/{form_id}/edit",
        "responder_url": created_form.get("responderUri"),
    })


@server.tool()
@handle_http_errors("get_form", is_read_only=True, service_type="forms")
@require_google_service("forms", "forms_read")
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
@handle_http_errors("add_question", service_type="forms")
@require_google_service("forms", "forms")
async def add_question(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form to add the question to. Obtain from the form's edit URL or from create_form. Use the FULL ID - do NOT truncate or modify it."),
    title: str = Field(..., description="The question text shown to respondents."),
    question_type: str = Field(..., description="The type of question. One of: RADIO (multiple choice, pick one), CHECKBOX (pick many), DROP_DOWN, SHORT_TEXT, PARAGRAPH (long text), SCALE (linear scale), DATE, TIME."),
    options: Optional[list[str]] = Field(None, description="The answer choices, required for RADIO, CHECKBOX, and DROP_DOWN questions. Ignored for other types."),
    required: bool = Field(False, description="Whether an answer is required. Defaults to False."),
    index: Optional[int] = Field(None, description="Zero-based position to insert the question at. If omitted, the question is appended to the end of the form."),
    description: Optional[str] = Field(None, description="Optional help text shown below the question title."),
    scale_low: int = Field(1, description="For SCALE questions, the lowest value of the scale. Defaults to 1."),
    scale_high: int = Field(5, description="For SCALE questions, the highest value of the scale. Defaults to 5."),
) -> str:
    """
    Add a question (item) to an existing form via batchUpdate.

    Returns:
        str: Details of the created question including its question_id and item_id.
    """
    logger.info(f"[add_question] Invoked. Email: '{user_google_email}', Form ID: {form_id}, Type: {question_type}")

    qtype = question_type.upper()
    question: Dict[str, Any] = {"required": required}

    if qtype in ("RADIO", "CHECKBOX", "DROP_DOWN"):
        if not options:
            raise ValueError(f"'options' is required for {qtype} questions.")
        question["choiceQuestion"] = {
            "type": qtype,
            "options": [{"value": opt} for opt in options],
        }
    elif qtype in ("SHORT_TEXT", "PARAGRAPH"):
        question["textQuestion"] = {"paragraph": qtype == "PARAGRAPH"}
    elif qtype == "SCALE":
        question["scaleQuestion"] = {"low": scale_low, "high": scale_high}
    elif qtype == "DATE":
        question["dateQuestion"] = {}
    elif qtype == "TIME":
        question["timeQuestion"] = {}
    else:
        raise ValueError(
            f"Unsupported question_type '{question_type}'. Use one of: "
            "RADIO, CHECKBOX, DROP_DOWN, SHORT_TEXT, PARAGRAPH, SCALE, DATE, TIME."
        )

    item: Dict[str, Any] = {"title": title, "questionItem": {"question": question}}
    if description:
        item["description"] = description

    # Determine insert location. Default to appending after the last existing item.
    if index is None:
        form = await asyncio.to_thread(service.forms().get(formId=form_id).execute)
        index = len(form.get("items", []))

    result = await _batch_update(service, form_id, [{
        "createItem": {"item": item, "location": {"index": index}}
    }])

    created = result.get("replies", [{}])[0].get("createItem", {})
    return success_response({
        "form_id": form_id,
        "item_id": created.get("itemId"),
        "question_id": (created.get("questionId") or [None])[0],
        "title": title,
        "type": qtype,
        "index": index,
    })


@server.tool()
@handle_http_errors("get_form_response", is_read_only=True, service_type="forms")
@require_google_service("forms", "forms_responses_read")
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
@require_google_service("forms", "forms_responses_read")
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
