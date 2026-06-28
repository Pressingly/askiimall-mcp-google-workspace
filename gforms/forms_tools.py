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
    is_published: bool = Field(True, description="Whether the form is published (live and reachable at its responder URL). Defaults to True."),
    is_accepting_responses: bool = Field(True, description="Whether the published form accepts new responses. Defaults to True."),
) -> str:
    """
    Updates the publish state of a form (published / accepting responses).

    Returns:
        str: Confirmation message of the successful publish settings update.
    """
    logger.info(f"[set_publish_settings] Invoked. Email: '{user_google_email}', Form ID: {form_id}")

    # Forms API v1 SetPublishSettingsRequest: publishSettings.publishState + updateMask.
    settings_body = {
        "publishSettings": {
            "publishState": {
                "isPublished": is_published,
                "isAcceptingResponses": is_accepting_responses,
            }
        },
        "updateMask": "publishState.isPublished,publishState.isAcceptingResponses",
    }

    await asyncio.to_thread(
        service.forms().setPublishSettings(formId=form_id, body=settings_body).execute
    )

    logger.info(f"Publish settings updated successfully for {user_google_email}. Form ID: {form_id}")
    return success_response({
        "form_id": form_id,
        "is_published": is_published,
        "is_accepting_responses": is_accepting_responses,
    })


@server.tool()
@handle_http_errors("add_question", service_type="forms")
@require_google_service("forms", "forms")
async def add_question(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form to add the question to. Obtain from the form's edit URL or from create_form. Use the FULL ID - do NOT truncate or modify it."),
    title: str = Field(..., description="The question text (or section/page-break title) shown to respondents."),
    question_type: str = Field(..., description="The type of item. One of: RADIO, CHECKBOX, DROP_DOWN, SHORT_TEXT, PARAGRAPH, SCALE, DATE, TIME, RADIO_GRID, CHECKBOX_GRID, PAGE_BREAK (start a new section)."),
    options: Optional[list[str]] = Field(None, description="The answer choices, required for RADIO, CHECKBOX, and DROP_DOWN questions."),
    required: bool = Field(False, description="Whether an answer is required. Defaults to False."),
    index: Optional[int] = Field(None, description="Zero-based position to insert at. If omitted, appended to the end of the form."),
    description: Optional[str] = Field(None, description="Optional help text shown below the title."),
    scale_low: int = Field(1, description="For SCALE questions, the lowest value of the scale. Defaults to 1."),
    scale_high: int = Field(5, description="For SCALE questions, the highest value of the scale. Defaults to 5."),
    grid_rows: Optional[list[str]] = Field(None, description="Row labels for RADIO_GRID / CHECKBOX_GRID questions."),
    grid_columns: Optional[list[str]] = Field(None, description="Column choices for RADIO_GRID / CHECKBOX_GRID questions."),
) -> str:
    """
    Add an item (question, grid, or page break) to an existing form via batchUpdate.

    Returns:
        str: Details of the created item including its question_id and item_id.
    """
    logger.info(f"[add_question] Invoked. Email: '{user_google_email}', Form ID: {form_id}, Type: {question_type}")

    qtype = question_type.upper()
    item: Dict[str, Any] = {"title": title}

    if qtype in ("RADIO", "CHECKBOX", "DROP_DOWN"):
        if not options:
            raise ValueError(f"'options' is required for {qtype} questions.")
        item["questionItem"] = {"question": {
            "required": required,
            "choiceQuestion": {"type": qtype, "options": [{"value": opt} for opt in options]},
        }}
    elif qtype in ("SHORT_TEXT", "PARAGRAPH"):
        item["questionItem"] = {"question": {
            "required": required, "textQuestion": {"paragraph": qtype == "PARAGRAPH"},
        }}
    elif qtype == "SCALE":
        item["questionItem"] = {"question": {
            "required": required, "scaleQuestion": {"low": scale_low, "high": scale_high},
        }}
    elif qtype == "DATE":
        item["questionItem"] = {"question": {"required": required, "dateQuestion": {}}}
    elif qtype == "TIME":
        item["questionItem"] = {"question": {"required": required, "timeQuestion": {}}}
    elif qtype in ("RADIO_GRID", "CHECKBOX_GRID"):
        if not grid_rows or not grid_columns:
            raise ValueError("grid_rows and grid_columns are required for grid questions.")
        col_type = "RADIO" if qtype == "RADIO_GRID" else "CHECKBOX"
        item["questionGroupItem"] = {
            "questions": [{"required": required, "rowQuestion": {"title": r}} for r in grid_rows],
            "grid": {"columns": {"type": col_type, "options": [{"value": c} for c in grid_columns]}},
        }
    elif qtype == "PAGE_BREAK":
        item["pageBreakItem"] = {}
    else:
        raise ValueError(
            f"Unsupported question_type '{question_type}'. Use one of: "
            "RADIO, CHECKBOX, DROP_DOWN, SHORT_TEXT, PARAGRAPH, SCALE, DATE, TIME, "
            "RADIO_GRID, CHECKBOX_GRID, PAGE_BREAK."
        )

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
@handle_http_errors("update_form_info", service_type="forms")
@require_google_service("forms", "forms")
async def update_form_info(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form. Use the FULL ID - do NOT truncate."),
    title: Optional[str] = Field(None, description="New form title (shown at the top of the form)."),
    description: Optional[str] = Field(None, description="New form description."),
    document_title: Optional[str] = Field(None, description="New browser-tab document title."),
) -> str:
    """
    Update a form's title, description, and/or document title.
    """
    logger.info(f"[update_form_info] Invoked. Email: '{user_google_email}', Form ID: {form_id}")

    info: Dict[str, Any] = {}
    masks = []
    if title is not None:
        info["title"] = title; masks.append("title")
    if description is not None:
        info["description"] = description; masks.append("description")
    if document_title is not None:
        info["documentTitle"] = document_title; masks.append("documentTitle")
    if not masks:
        raise ValueError("Provide at least one of title, description, document_title.")

    await _batch_update(service, form_id, [{
        "updateFormInfo": {"info": info, "updateMask": ",".join(masks)}
    }])
    return success_response({"form_id": form_id, "updated": masks})


@server.tool()
@handle_http_errors("update_question", service_type="forms")
@require_google_service("forms", "forms")
async def update_question(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form. Use the FULL ID - do NOT truncate."),
    index: int = Field(..., description="Zero-based position of the item to update (from get_form question index minus 1, or the order shown)."),
    title: Optional[str] = Field(None, description="New question title."),
    required: Optional[bool] = Field(None, description="New required flag."),
    options: Optional[list[str]] = Field(None, description="New answer choices (for RADIO/CHECKBOX/DROP_DOWN questions)."),
    description: Optional[str] = Field(None, description="New help text."),
) -> str:
    """
    Update an existing question's title, required flag, choices, or help text.
    """
    logger.info(f"[update_question] Invoked. Email: '{user_google_email}', Form ID: {form_id}, Index: {index}")

    item: Dict[str, Any] = {}
    masks = []
    if title is not None:
        item["title"] = title; masks.append("title")
    if description is not None:
        item["description"] = description; masks.append("description")
    question: Dict[str, Any] = {}
    if required is not None:
        question["required"] = required; masks.append("questionItem.question.required")
    if options is not None:
        question["choiceQuestion"] = {"options": [{"value": o} for o in options]}
        masks.append("questionItem.question.choiceQuestion.options")
    if question:
        item["questionItem"] = {"question": question}
    if not masks:
        raise ValueError("Provide at least one field to update (title, required, options, description).")

    await _batch_update(service, form_id, [{
        "updateItem": {"item": item, "location": {"index": index}, "updateMask": ",".join(masks)}
    }])
    return success_response({"form_id": form_id, "index": index, "updated": masks})


@server.tool()
@handle_http_errors("delete_question", service_type="forms")
@require_google_service("forms", "forms")
async def delete_question(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form. Use the FULL ID - do NOT truncate."),
    index: int = Field(..., description="Zero-based position of the item to delete."),
) -> str:
    """
    Delete an item (question / grid / page break) from a form by position.
    """
    logger.info(f"[delete_question] Invoked. Email: '{user_google_email}', Form ID: {form_id}, Index: {index}")
    await _batch_update(service, form_id, [{"deleteItem": {"location": {"index": index}}}])
    return success_response({"form_id": form_id, "deleted_index": index})


@server.tool()
@handle_http_errors("move_question", service_type="forms")
@require_google_service("forms", "forms")
async def move_question(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form. Use the FULL ID - do NOT truncate."),
    from_index: int = Field(..., description="Current zero-based position of the item."),
    to_index: int = Field(..., description="New zero-based position for the item."),
) -> str:
    """
    Reorder an item by moving it from one position to another.
    """
    logger.info(f"[move_question] Invoked. Email: '{user_google_email}', Form ID: {form_id}, {from_index}->{to_index}")
    await _batch_update(service, form_id, [{
        "moveItem": {"originalLocation": {"index": from_index}, "newLocation": {"index": to_index}}
    }])
    return success_response({"form_id": form_id, "from_index": from_index, "to_index": to_index})


@server.tool()
@handle_http_errors("set_quiz_settings", service_type="forms")
@require_google_service("forms", "forms")
async def set_quiz_settings(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    form_id: str = Field(..., description="The ID of the form. Use the FULL ID - do NOT truncate."),
    is_quiz: bool = Field(True, description="Whether the form is a quiz (enables grading). Defaults to True."),
) -> str:
    """
    Turn quiz mode on or off for a form (enables per-question grading in the UI).
    """
    logger.info(f"[set_quiz_settings] Invoked. Email: '{user_google_email}', Form ID: {form_id}, is_quiz={is_quiz}")
    await _batch_update(service, form_id, [{
        "updateSettings": {
            "settings": {"quizSettings": {"isQuiz": is_quiz}},
            "updateMask": "quizSettings.isQuiz",
        }
    }])
    return success_response({"form_id": form_id, "is_quiz": is_quiz})


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
