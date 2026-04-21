"""
Google Calendar MCP Tools

This module provides MCP tools for interacting with Google Calendar API.
"""

import datetime
import logging
import asyncio
import re
import uuid
import json
from typing import List, Optional, Dict, Any, Union

from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from pydantic import Field

from auth.service_decorator import require_google_service
from core.utils import handle_http_errors
from core.response import success_response

from core.server import server


# Configure module logger
logger = logging.getLogger(__name__)

# Google Calendar API field projections
CALENDAR_FIELDS = {
    "list": "items(id,summary,primary,description,timeZone,backgroundColor,foregroundColor,colorId,accessRole,selected,summaryOverride,defaultReminders,conferenceProperties),nextPageToken",
    "list_events": "items(id,summary,start,end,htmlLink,description,location,status,attendees(email,displayName,responseStatus,self),conferenceData(entryPoints),recurrence,recurringEventId,visibility,transparency,colorId),nextPageToken",
    "get_event": "id,summary,start,end,htmlLink,description,location,status,creator,organizer,attendees(email,displayName,responseStatus,self,comment,optional,additionalGuests,organizer),conferenceData(entryPoints),reminders,attachments(fileUrl,title,mimeType),created,updated,recurrence,recurringEventId,visibility,transparency,colorId",
}


def _extract_meet_link(event: Dict[str, Any]) -> Optional[str]:
    """Extract Google Meet link from event conference data."""
    conference_data = event.get("conferenceData")
    if not conference_data:
        return None
    for entry_point in conference_data.get("entryPoints", []):
        if entry_point.get("entryPointType") == "video":
            return entry_point.get("uri")
    return None


def _map_calendar(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw Calendar API calendar object to a clean response shape."""
    result = {
        "id": raw["id"],
        "title": raw.get("summary"),
        "primary": raw.get("primary", False),
        "description": raw.get("description"),
        "timezone": raw.get("timeZone"),
        "backgroundColor": raw.get("backgroundColor"),
        "foregroundColor": raw.get("foregroundColor"),
        "colorId": raw.get("colorId"),
        "accessRole": raw.get("accessRole"),
        "selected": raw.get("selected"),
        "summaryOverride": raw.get("summaryOverride"),
    }
    if raw.get("defaultReminders"):
        result["defaultReminders"] = raw["defaultReminders"]
    if raw.get("conferenceProperties"):
        result["conferenceProperties"] = raw["conferenceProperties"]
    return result


def _get_my_response_status(raw: Dict[str, Any]) -> Optional[str]:
    """Extract the authenticated user's response status from attendees."""
    for attendee in raw.get("attendees", []):
        if attendee.get("self"):
            return attendee.get("responseStatus")
    return None


def _map_event(raw: Dict[str, Any], compact: bool = False) -> Dict[str, Any]:
    """Map a raw Calendar API event object to a clean response shape.

    Args:
        raw: Raw event dict from Google Calendar API.
        compact: If True, return a compact shape for list views.
    """
    start = raw.get("start", {})
    end = raw.get("end", {})
    all_day = "date" in start and "dateTime" not in start
    result = {
        "id": raw.get("id"),
        "title": raw.get("summary"),
        "start": start.get("dateTime") or start.get("date"),
        "end": end.get("dateTime") or end.get("date"),
        "link": raw.get("htmlLink"),
        "status": raw.get("status"),
        "allDay": all_day,
        "myResponseStatus": _get_my_response_status(raw),
        "recurrence": raw.get("recurrence"),
        "recurringEventId": raw.get("recurringEventId"),
        "visibility": raw.get("visibility"),
        "transparency": raw.get("transparency"),
        "colorId": raw.get("colorId"),
    }
    if compact:
        result["attendee_count"] = len(raw.get("attendees", []))
    else:
        result["description"] = raw.get("description")
        result["location"] = raw.get("location")
        result["attendees"] = [
            {
                "email": a.get("email"),
                "name": a.get("displayName"),
                "response": a.get("responseStatus"),
                "comment": a.get("comment"),
                "optional": a.get("optional"),
                "organizer": a.get("organizer"),
                "self": a.get("self"),
                "additionalGuests": a.get("additionalGuests"),
            }
            for a in raw.get("attendees", [])
        ]
        result["meet_link"] = _extract_meet_link(raw)
        result["reminders"] = raw.get("reminders")
        result["attachments"] = [
            {"url": att.get("fileUrl"), "title": att.get("title"), "type": att.get("mimeType")}
            for att in raw.get("attachments", [])
        ]
        result["creator"] = raw.get("creator", {}).get("email")
        result["organizer"] = raw.get("organizer", {}).get("email")
        result["created"] = raw.get("created")
        result["updated"] = raw.get("updated")
    return result


def _parse_reminders_json(reminders_input: Optional[Union[str, List[Dict[str, Any]]]], function_name: str) -> List[Dict[str, Any]]:
    """
    Parse reminders from JSON string or list object and validate them.
    
    Args:
        reminders_input: JSON string containing reminder objects or list of reminder objects
        function_name: Name of calling function for logging
        
    Returns:
        List of validated reminder objects
    """
    if not reminders_input:
        return []
    
    # Handle both string (JSON) and list inputs
    if isinstance(reminders_input, str):
        try:
            reminders = json.loads(reminders_input)
            if not isinstance(reminders, list):
                logger.warning(f"[{function_name}] Reminders must be a JSON array, got {type(reminders).__name__}")
                return []
        except json.JSONDecodeError as e:
            logger.warning(f"[{function_name}] Invalid JSON for reminders: {e}")
            return []
    elif isinstance(reminders_input, list):
        reminders = reminders_input
    else:
        logger.warning(f"[{function_name}] Reminders must be a JSON string or list, got {type(reminders_input).__name__}")
        return []
    
    # Validate reminders
    if len(reminders) > 5:
        logger.warning(f"[{function_name}] More than 5 reminders provided, truncating to first 5")
        reminders = reminders[:5]
    
    validated_reminders = []
    for reminder in reminders:
        if not isinstance(reminder, dict) or "method" not in reminder or "minutes" not in reminder:
            logger.warning(f"[{function_name}] Invalid reminder format: {reminder}, skipping")
            continue
        
        method = reminder["method"].lower()
        if method not in ["popup", "email"]:
            logger.warning(f"[{function_name}] Invalid reminder method '{method}', must be 'popup' or 'email', skipping")
            continue
        
        minutes = reminder["minutes"]
        if not isinstance(minutes, int) or minutes < 0 or minutes > 40320:
            logger.warning(f"[{function_name}] Invalid reminder minutes '{minutes}', must be integer 0-40320, skipping")
            continue
        
        validated_reminders.append({
            "method": method,
            "minutes": minutes
        })
    
    return validated_reminders


def _preserve_existing_fields(event_body: Dict[str, Any], existing_event: Dict[str, Any], field_mappings: Dict[str, Any]) -> None:
    """
    Helper function to preserve existing event fields when not explicitly provided.

    Args:
        event_body: The event body being built for the API call
        existing_event: The existing event data from the API
        field_mappings: Dict mapping field names to their new values (None means preserve existing)
    """
    for field_name, new_value in field_mappings.items():
        if new_value is None and field_name in existing_event:
            event_body[field_name] = existing_event[field_name]
            logger.info(f"[modify_event] Preserving existing {field_name}")
        elif new_value is not None:
            event_body[field_name] = new_value


# Helper function to ensure time strings for API calls are correctly formatted
def _correct_time_format_for_api(
    time_str: Optional[str], param_name: str
) -> Optional[str]:
    if not time_str:
        return None

    logger.info(
        f"_correct_time_format_for_api: Processing {param_name} with value '{time_str}'"
    )

    # Handle date-only format (YYYY-MM-DD)
    if len(time_str) == 10 and time_str.count("-") == 2:
        try:
            # Validate it's a proper date
            datetime.datetime.strptime(time_str, "%Y-%m-%d")
            # For date-only, append T00:00:00Z to make it RFC3339 compliant
            formatted = f"{time_str}T00:00:00Z"
            logger.info(
                f"Formatting date-only {param_name} '{time_str}' to RFC3339: '{formatted}'"
            )
            return formatted
        except ValueError:
            logger.warning(
                f"{param_name} '{time_str}' looks like a date but is not valid YYYY-MM-DD. Using as is."
            )
            return time_str

    # Specifically address YYYY-MM-DDTHH:MM:SS by appending 'Z'
    if (
        len(time_str) == 19
        and time_str[10] == "T"
        and time_str.count(":") == 2
        and not (
            time_str.endswith("Z") or ("+" in time_str[10:]) or ("-" in time_str[10:])
        )
    ):
        try:
            # Validate the format before appending 'Z'
            datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
            logger.info(
                f"Formatting {param_name} '{time_str}' by appending 'Z' for UTC."
            )
            return time_str + "Z"
        except ValueError:
            logger.warning(
                f"{param_name} '{time_str}' looks like it needs 'Z' but is not valid YYYY-MM-DDTHH:MM:SS. Using as is."
            )
            return time_str

    # If it already has timezone info or doesn't match our patterns, return as is
    logger.info(f"{param_name} '{time_str}' doesn't need formatting, using as is.")
    return time_str


@server.tool()
@handle_http_errors("list_calendars", is_read_only=True, service_type="calendar")
@require_google_service("calendar", "calendar_read")
async def list_calendars(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    page_token: Optional[str] = Field(None, description="Token for retrieving the next page of results. Use the next_page_token from a previous response."),
) -> str:
    """
    Retrieves a list of calendars accessible to the authenticated user.
    Supports pagination for accounts with many calendars.

    Returns:
        str: A formatted list of the user's calendars (summary, ID, primary status).
    """
    logger.info(f"[list_calendars] Invoked. Email: '{user_google_email}'")

    request_params = {"fields": CALENDAR_FIELDS["list"]}
    if page_token:
        request_params["pageToken"] = page_token

    calendar_list_response = await asyncio.to_thread(
        lambda: service.calendarList().list(**request_params).execute()
    )
    items = calendar_list_response.get("items", [])
    next_page_token = calendar_list_response.get("nextPageToken")

    calendars = [_map_calendar(cal) for cal in items]
    logger.info(f"Successfully listed {len(calendars)} calendars for {user_google_email}.")
    response_data = {"calendars": calendars, "count": len(calendars)}
    if next_page_token:
        response_data["next_page_token"] = next_page_token
    return success_response(response_data)


@server.tool()
@handle_http_errors("get_events", is_read_only=True, service_type="calendar")
@require_google_service("calendar", "calendar_read")
async def get_events(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    calendar_id: str = Field("primary", description="The ID of the calendar to query. Use 'primary' for the user's primary calendar. Use the FULL ID exactly from list_calendars - do NOT truncate or modify it."),
    time_min: Optional[str] = Field(None, description="The start of the time range (inclusive) in RFC3339 format. Examples: '2024-05-12T10:00:00Z' (with time) or '2024-05-12' (date only). If omitted, defaults to the current time."),
    time_max: Optional[str] = Field(None, description="The end of the time range (exclusive) in RFC3339 format. Examples: '2024-05-13T10:00:00Z' (with time) or '2024-05-13' (date only). If omitted, events starting from time_min onwards are considered (up to max_results)."),
    max_results: int = Field(50, description="The maximum number of events to return per page. Defaults to 50, max 250."),
    query: Optional[str] = Field(None, description="A keyword to search for within event fields (summary, description, location)."),
    timezone: Optional[str] = Field(None, description="IANA timezone for interpreting times in the request and response (e.g., 'America/New_York')."),
    condense_event_details: bool = Field(True, description="If true (default), returns condensed event format. If false, returns full event details including attendees and attachments."),
    page_token: Optional[str] = Field(None, description="Token for pagination. Use the next_page_token from a previous response to get the next set of results."),
) -> str:
    """
    Retrieves a list of events from a specified Google Calendar within a given time range.
    You can also search for events by keyword by supplying the optional "query" param.
    Supports pagination for busy calendars or long time ranges.

    Returns:
        str: A formatted list of events (summary, start and end times, link) within the specified range.
    """
    logger.info(
        f"[get_events] Raw time parameters - time_min: '{time_min}', time_max: '{time_max}', query: '{query}'"
    )

    # Clamp max_results to valid range
    max_results = min(max(1, max_results), 250)

    # Ensure time_min and time_max are correctly formatted for the API
    formatted_time_min = _correct_time_format_for_api(time_min, "time_min")
    effective_time_min = formatted_time_min or (
        datetime.datetime.utcnow().isoformat() + "Z"
    )
    if time_min is None:
        logger.info(
            f"time_min not provided, defaulting to current UTC time: {effective_time_min}"
        )
    else:
        logger.info(
            f"time_min processing: original='{time_min}', formatted='{formatted_time_min}', effective='{effective_time_min}'"
        )

    effective_time_max = _correct_time_format_for_api(time_max, "time_max")
    if time_max:
        logger.info(
            f"time_max processing: original='{time_max}', formatted='{effective_time_max}'"
        )

    logger.info(
        f"[get_events] Final API parameters - calendarId: '{calendar_id}', timeMin: '{effective_time_min}', timeMax: '{effective_time_max}', maxResults: {max_results}, query: '{query}'"
    )

    # Choose field projection based on detail level
    fields = CALENDAR_FIELDS["list_events"] if condense_event_details else CALENDAR_FIELDS["get_event"].replace(
        "id,", "items(id,"
    ).replace("colorId", "colorId),nextPageToken")

    # Build the request parameters dynamically
    request_params = {
        "calendarId": calendar_id,
        "timeMin": effective_time_min,
        "timeMax": effective_time_max,
        "maxResults": max_results,
        "singleEvents": True,
        "orderBy": "startTime",
        "fields": CALENDAR_FIELDS["list_events"],
    }

    if query:
        request_params["q"] = query
    if timezone:
        request_params["timeZone"] = timezone
    if page_token:
        request_params["pageToken"] = page_token

    events_result = await asyncio.to_thread(
        lambda: service.events()
        .list(**request_params)
        .execute()
    )
    items = events_result.get("items", [])
    next_page_token = events_result.get("nextPageToken")

    compact = condense_event_details
    events = [_map_event(item, compact=compact) for item in items]
    logger.info(f"Successfully retrieved {len(events)} events for {user_google_email}.")
    response_data = {"events": events, "count": len(events)}
    if next_page_token:
        response_data["next_page_token"] = next_page_token
    return success_response(response_data)


@server.tool()
@handle_http_errors("create_event", service_type="calendar")
@require_google_service("calendar", "calendar_events")
async def create_event(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    summary: str = Field(..., description="Event title or summary."),
    start_time: str = Field(..., description="Start time in RFC3339 format. Examples: '2023-10-27T10:00:00-07:00' (with time) or '2023-10-27' (all-day event)."),
    end_time: str = Field(..., description="End time in RFC3339 format. Examples: '2023-10-27T11:00:00-07:00' (with time) or '2023-10-28' (all-day event)."),
    calendar_id: str = Field("primary", description="Calendar ID. Use 'primary' for the user's primary calendar. Use the FULL ID exactly from list_calendars - do NOT truncate or modify it."),
    description: Optional[str] = Field(None, description="Event description or notes."),
    location: Optional[str] = Field(None, description="Event location (e.g., 'Conference Room A', '123 Main St, City, State')."),
    attendees: Optional[List[str]] = Field(None, description="List of attendee email addresses to invite to the event."),
    timezone: Optional[str] = Field(None, description="Timezone for the event (e.g., 'America/New_York', 'Europe/London', 'Asia/Tokyo'). Only applies to timed events (not all-day)."),
    attachments: Optional[List[str]] = Field(None, description="List of Google Drive file URLs or file IDs to attach to the event. Can be full URLs (https://drive.google.com/...) or just file IDs."),
    add_google_meet: bool = Field(False, description="Whether to add a Google Meet video conference link to the event. Defaults to False."),
    reminders: Optional[Union[str, List[Dict[str, Any]]]] = Field(None, description="JSON string or list of reminder objects. Each reminder should have 'method' (\"popup\" or \"email\") and 'minutes' (0-40320). Maximum 5 reminders. Example: '[{\"method\": \"popup\", \"minutes\": 15}]' or [{\"method\": \"popup\", \"minutes\": 15}]."),
    use_default_reminders: bool = Field(True, description="Whether to use the calendar's default reminders. If False and reminders are provided, uses custom reminders. Defaults to True."),
    recurrence: Optional[List[str]] = Field(None, description="List of RRULE strings for recurring events (e.g., ['RRULE:FREQ=WEEKLY;BYDAY=MO,WE,FR'])."),
    color_id: Optional[str] = Field(None, description="Event color ID ('1'-'11'): 1=Lavender, 2=Sage, 3=Grape, 4=Flamingo, 5=Banana, 6=Tangerine, 7=Peacock, 8=Graphite, 9=Blueberry, 10=Basil, 11=Tomato."),
    send_updates: Optional[str] = Field(None, description="Who receives notification of this event creation: 'all' (all attendees), 'externalOnly' (only external attendees), or 'none'."),
) -> str:
    """
    Creates a new event.

    Returns:
        str: Confirmation message of the successful event creation with event link.
    """
    logger.info(
        f"[create_event] Invoked. Email: '{user_google_email}', Summary: {summary}"
    )
    logger.info(f"[create_event] Incoming attachments param: {attachments}")
    # If attachments value is a string, split by comma and strip whitespace
    if attachments and isinstance(attachments, str):
        attachments = [a.strip() for a in attachments.split(',') if a.strip()]
        logger.info(f"[create_event] Parsed attachments list from string: {attachments}")
    event_body: Dict[str, Any] = {
        "summary": summary,
        "start": (
            {"date": start_time}
            if "T" not in start_time
            else {"dateTime": start_time}
        ),
        "end": (
            {"date": end_time} if "T" not in end_time else {"dateTime": end_time}
        ),
    }
    if location:
        event_body["location"] = location
    if description:
        event_body["description"] = description
    if timezone:
        if "dateTime" in event_body["start"]:
            event_body["start"]["timeZone"] = timezone
        if "dateTime" in event_body["end"]:
            event_body["end"]["timeZone"] = timezone
    if attendees:
        event_body["attendees"] = [{"email": email} for email in attendees]

    # Handle reminders
    if reminders is not None or not use_default_reminders:
        # If custom reminders are provided, automatically disable default reminders
        effective_use_default = use_default_reminders and reminders is None
        
        reminder_data = {
            "useDefault": effective_use_default
        }
        if reminders is not None:
            validated_reminders = _parse_reminders_json(reminders, "create_event")
            if validated_reminders:
                reminder_data["overrides"] = validated_reminders
                logger.info(f"[create_event] Added {len(validated_reminders)} custom reminders")
                if use_default_reminders:
                    logger.info("[create_event] Custom reminders provided - disabling default reminders")
        
        event_body["reminders"] = reminder_data

    if recurrence:
        event_body["recurrence"] = recurrence
    if color_id:
        event_body["colorId"] = color_id

    if add_google_meet:
        request_id = str(uuid.uuid4())
        event_body["conferenceData"] = {
            "createRequest": {
                "requestId": request_id,
                "conferenceSolutionKey": {
                    "type": "hangoutsMeet"
                }
            }
        }
        logger.info(f"[create_event] Adding Google Meet conference with request ID: {request_id}")

    # Build insert kwargs
    insert_kwargs = {
        "calendarId": calendar_id,
        "body": event_body,
        "conferenceDataVersion": 1 if add_google_meet else 0,
    }
    if send_updates:
        insert_kwargs["sendUpdates"] = send_updates

    if attachments:
        # Accept both file URLs and file IDs. If a URL, extract the fileId.
        event_body["attachments"] = []
        drive_service = None
        try:
            drive_service = service._http and build("drive", "v3", http=service._http)
        except Exception as e:
            logger.warning(f"Could not build Drive service for MIME type lookup: {e}")
        for att in attachments:
            file_id = None
            if att.startswith("https://"):
                # Match /d/<id>, /file/d/<id>, ?id=<id>
                match = re.search(r"(?:/d/|/file/d/|id=)([\w-]+)", att)
                file_id = match.group(1) if match else None
                logger.info(f"[create_event] Extracted file_id '{file_id}' from attachment URL '{att}'")
            else:
                file_id = att
                logger.info(f"[create_event] Using direct file_id '{file_id}' for attachment")
            if file_id:
                file_url = f"https://drive.google.com/open?id={file_id}"
                mime_type = "application/vnd.google-apps.drive-sdk"
                title = "Drive Attachment"
                # Try to get the actual MIME type and filename from Drive
                if drive_service:
                    try:
                        file_metadata = await asyncio.to_thread(
                            lambda: drive_service.files().get(fileId=file_id, fields="mimeType,name").execute()
                        )
                        mime_type = file_metadata.get("mimeType", mime_type)
                        filename = file_metadata.get("name")
                        if filename:
                            title = filename
                            logger.info(f"[create_event] Using filename '{filename}' as attachment title")
                        else:
                            logger.info("[create_event] No filename found, using generic title")
                    except Exception as e:
                        logger.warning(f"Could not fetch metadata for file {file_id}: {e}")
                event_body["attachments"].append({
                    "fileUrl": file_url,
                    "title": title,
                    "mimeType": mime_type,
                })
        insert_kwargs["supportsAttachments"] = True
        created_event = await asyncio.to_thread(
            lambda: service.events().insert(**insert_kwargs).execute()
        )
    else:
        created_event = await asyncio.to_thread(
            lambda: service.events().insert(**insert_kwargs).execute()
        )
    mapped = _map_event(created_event, compact=False)
    logger.info(
        f"Event created successfully for {user_google_email}. ID: {mapped.get('id')}, Link: {mapped.get('link')}"
    )
    return success_response({"event": mapped})


@server.tool()
@handle_http_errors("modify_event", service_type="calendar")
@require_google_service("calendar", "calendar_events")
async def modify_event(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    event_id: str = Field(..., description="The ID of the event to modify. Use the FULL ID exactly from get_events, get_event, or create_event - do NOT truncate or modify it."),
    calendar_id: str = Field("primary", description="Calendar ID. Use 'primary' for the user's primary calendar. Calendar IDs can be obtained using list_calendars."),
    summary: Optional[str] = Field(None, description="New event title. If not provided, the existing title is preserved."),
    start_time: Optional[str] = Field(None, description="New start time in RFC3339 format. Examples: '2023-10-27T10:00:00-07:00' (with time) or '2023-10-27' (all-day). If not provided, the existing start time is preserved."),
    end_time: Optional[str] = Field(None, description="New end time in RFC3339 format. Examples: '2023-10-27T11:00:00-07:00' (with time) or '2023-10-28' (all-day). If not provided, the existing end time is preserved."),
    description: Optional[str] = Field(None, description="New event description. If not provided, the existing description is preserved."),
    location: Optional[str] = Field(None, description="New event location. If not provided, the existing location is preserved."),
    attendees: Optional[List[str]] = Field(None, description="New list of attendee email addresses. If not provided, the existing attendees are preserved."),
    timezone: Optional[str] = Field(None, description="New timezone (e.g., 'America/New_York', 'Europe/London'). Only applies to timed events (not all-day)."),
    add_google_meet: Optional[bool] = Field(None, description="Whether to add or remove Google Meet video conference. If True, adds Google Meet; if False, removes it; if None, leaves unchanged."),
    reminders: Optional[Union[str, List[Dict[str, Any]]]] = Field(None, description="JSON string or list of reminder objects to replace existing reminders. Each reminder should have 'method' (\"popup\" or \"email\") and 'minutes' (0-40320). Maximum 5 reminders. Example: '[{\"method\": \"popup\", \"minutes\": 15}]'."),
    use_default_reminders: Optional[bool] = Field(None, description="Whether to use calendar's default reminders. If specified, overrides current reminder settings. If None, preserves existing reminder settings."),
    color_id: Optional[str] = Field(None, description="Event color ID ('1'-'11'): 1=Lavender, 2=Sage, 3=Grape, 4=Flamingo, 5=Banana, 6=Tangerine, 7=Peacock, 8=Graphite, 9=Blueberry, 10=Basil, 11=Tomato."),
    send_updates: Optional[str] = Field(None, description="Who receives notification of this event update: 'all' (all attendees), 'externalOnly' (only external attendees), or 'none'."),
) -> str:
    """
    Modifies an existing event.

    Returns:
        str: Confirmation message of the successful event modification with event link.
    """
    logger.info(
        f"[modify_event] Invoked. Email: '{user_google_email}', Event ID: {event_id}"
    )

    # Build the event body with only the fields that are provided
    event_body: Dict[str, Any] = {}
    if summary is not None:
        event_body["summary"] = summary
    if start_time is not None:
        event_body["start"] = (
            {"date": start_time}
            if "T" not in start_time
            else {"dateTime": start_time}
        )
        if timezone is not None and "dateTime" in event_body["start"]:
            event_body["start"]["timeZone"] = timezone
    if end_time is not None:
        event_body["end"] = (
            {"date": end_time} if "T" not in end_time else {"dateTime": end_time}
        )
        if timezone is not None and "dateTime" in event_body["end"]:
            event_body["end"]["timeZone"] = timezone
    if description is not None:
        event_body["description"] = description
    if location is not None:
        event_body["location"] = location
    if attendees is not None:
        event_body["attendees"] = [{"email": email} for email in attendees]
    if color_id is not None:
        event_body["colorId"] = color_id

    # Handle reminders
    if reminders is not None or use_default_reminders is not None:
        reminder_data = {}
        if use_default_reminders is not None:
            reminder_data["useDefault"] = use_default_reminders
        else:
            # Preserve existing event's useDefault value if not explicitly specified
            try:
                existing_event = service.events().get(calendarId=calendar_id, eventId=event_id).execute()
                reminder_data["useDefault"] = existing_event.get("reminders", {}).get("useDefault", True)
            except Exception as e:
                logger.warning(f"[modify_event] Could not fetch existing event for reminders: {e}")
                reminder_data["useDefault"] = True  # Fallback to True if unable to fetch
        
        # If custom reminders are provided, automatically disable default reminders
        if reminders is not None:
            if reminder_data.get("useDefault", False):
                reminder_data["useDefault"] = False
                logger.info("[modify_event] Custom reminders provided - disabling default reminders")
            
            validated_reminders = _parse_reminders_json(reminders, "modify_event")
            if reminders and not validated_reminders:
                logger.warning("[modify_event] Reminders provided but failed validation. No custom reminders will be set.")
            elif validated_reminders:
                reminder_data["overrides"] = validated_reminders
                logger.info(f"[modify_event] Updated reminders with {len(validated_reminders)} custom reminders")
        
        event_body["reminders"] = reminder_data

    if (
        timezone is not None
        and "start" not in event_body
        and "end" not in event_body
    ):
        # If timezone is provided but start/end times are not, we need to fetch the existing event
        # to apply the timezone correctly. This is a simplification; a full implementation
        # might handle this more robustly or require start/end with timezone.
        # For now, we'll log a warning and skip applying timezone if start/end are missing.
        logger.warning(
            "[modify_event] Timezone provided but start_time and end_time are missing. Timezone will not be applied unless start/end times are also provided."
        )

    if not event_body:
        message = "No fields provided to modify the event."
        logger.warning(f"[modify_event] {message}")
        raise Exception(message)

    # Log the event ID for debugging
    logger.info(
        f"[modify_event] Attempting to update event with ID: '{event_id}' in calendar '{calendar_id}'"
    )

    # Get the existing event to preserve fields that aren't being updated
    try:
        existing_event = await asyncio.to_thread(
            lambda: service.events().get(calendarId=calendar_id, eventId=event_id).execute()
        )
        logger.info(
            "[modify_event] Successfully retrieved existing event before update"
        )

        # Preserve existing fields if not provided in the update
        # Only preserve fields that weren't already set in event_body
        _preserve_existing_fields(event_body, existing_event, {
            "summary": summary,
            "description": description,
            "location": location,
            "attendees": event_body.get("attendees"),
            "start": event_body.get("start"),
            "end": event_body.get("end"),
            "colorId": color_id,
        })

        # Handle Google Meet conference data
        if add_google_meet is not None:
            if add_google_meet:
                # Add Google Meet
                request_id = str(uuid.uuid4())
                event_body["conferenceData"] = {
                    "createRequest": {
                        "requestId": request_id,
                        "conferenceSolutionKey": {
                            "type": "hangoutsMeet"
                        }
                    }
                }
                logger.info(f"[modify_event] Adding Google Meet conference with request ID: {request_id}")
            else:
                # Remove Google Meet by setting conferenceData to empty
                event_body["conferenceData"] = {}
                logger.info("[modify_event] Removing Google Meet conference")
        elif 'conferenceData' in existing_event:
            # Preserve existing conference data if not specified
            event_body["conferenceData"] = existing_event["conferenceData"]
            logger.info("[modify_event] Preserving existing conference data")

    except HttpError as get_error:
        if get_error.resp.status == 404:
            logger.error(
                f"[modify_event] Event not found during pre-update verification: {get_error}"
            )
            message = f"Event not found during verification. The event with ID '{event_id}' could not be found in calendar '{calendar_id}'. This may be due to incorrect ID format or the event no longer exists."
            raise Exception(message)
        else:
            logger.warning(
                f"[modify_event] Error during pre-update verification, but proceeding with update: {get_error}"
            )

    # Proceed with the update
    update_kwargs = {
        "calendarId": calendar_id,
        "eventId": event_id,
        "body": event_body,
        "conferenceDataVersion": 1,
    }
    if send_updates:
        update_kwargs["sendUpdates"] = send_updates

    updated_event = await asyncio.to_thread(
        lambda: service.events()
        .update(**update_kwargs)
        .execute()
    )

    mapped = _map_event(updated_event, compact=False)
    logger.info(
        f"Event modified successfully for {user_google_email}. ID: {mapped.get('id')}, Link: {mapped.get('link')}"
    )
    return success_response({"event": mapped})


@server.tool()
@handle_http_errors("delete_event", service_type="calendar")
@require_google_service("calendar", "calendar_events")
async def delete_event(
    service, 
    user_google_email: str = Field(..., description="The user's Google email address."),
    event_id: str = Field(..., description="The ID of the event to delete. Use the FULL ID exactly from get_events, get_event, or create_event - do NOT truncate or modify it."),
    calendar_id: str = Field("primary", description="Calendar ID. Use 'primary' for the user's primary calendar. Calendar IDs can be obtained using list_calendars."),
    send_updates: Optional[str] = Field(None, description="Who receives notification of this event deletion: 'all' (all attendees), 'externalOnly' (only external attendees), or 'none'."),
) -> str:
    """
    Deletes an existing event.

    Returns:
        str: Confirmation message of the successful event deletion.
    """
    logger.info(
        f"[delete_event] Invoked. Email: '{user_google_email}', Event ID: {event_id}"
    )

    # Log the event ID for debugging
    logger.info(
        f"[delete_event] Attempting to delete event with ID: '{event_id}' in calendar '{calendar_id}'"
    )

    # Try to get the event first to verify it exists
    try:
        await asyncio.to_thread(
            lambda: service.events().get(calendarId=calendar_id, eventId=event_id).execute()
        )
        logger.info(
            "[delete_event] Successfully verified event exists before deletion"
        )
    except HttpError as get_error:
        if get_error.resp.status == 404:
            logger.error(
                f"[delete_event] Event not found during pre-delete verification: {get_error}"
            )
            message = f"Event not found during verification. The event with ID '{event_id}' could not be found in calendar '{calendar_id}'. This may be due to incorrect ID format or the event no longer exists."
            raise Exception(message)
        else:
            logger.warning(
                f"[delete_event] Error during pre-delete verification, but proceeding with deletion: {get_error}"
            )

    # Proceed with the deletion
    delete_kwargs = {"calendarId": calendar_id, "eventId": event_id}
    if send_updates:
        delete_kwargs["sendUpdates"] = send_updates
    await asyncio.to_thread(
        lambda: service.events().delete(**delete_kwargs).execute()
    )

    logger.info(f"Event deleted successfully for {user_google_email}. ID: {event_id}")
    return success_response({"deleted": True, "event_id": event_id})


@server.tool()
@handle_http_errors("get_event", is_read_only=True, service_type="calendar")
@require_google_service("calendar", "calendar_read")
async def get_event(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    event_id: str = Field(..., description="The ID of the event to retrieve. Use the FULL ID exactly from get_events or create_event - do NOT truncate or modify it."),
    calendar_id: str = Field("primary", description="The ID of the calendar to query. Use 'primary' for the user's primary calendar. Calendar IDs can be obtained using list_calendars.")
) -> str:
    """
    Retrieves the details of a single event by its ID from a specified Google Calendar.

    Returns:
        str: A formatted string with the event's details.
    """
    logger.info(f"[get_event] Invoked. Email: '{user_google_email}', Event ID: {event_id}")
    event = await asyncio.to_thread(
        lambda: service.events().get(
            calendarId=calendar_id, eventId=event_id,
            fields=CALENDAR_FIELDS["get_event"]
        ).execute()
    )
    mapped = _map_event(event, compact=False)
    logger.info(f"[get_event] Successfully retrieved event {event_id} for {user_google_email}.")
    return success_response(mapped)


# ---------------------------------------------------------------------------
# Helper functions for FreeBusy-based tools
# ---------------------------------------------------------------------------

def _merge_busy_intervals(busy_list: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Merge overlapping busy intervals into a minimal set.

    Args:
        busy_list: List of {"start": ..., "end": ...} ISO 8601 strings.

    Returns:
        Sorted, merged list of busy intervals.
    """
    if not busy_list:
        return []
    parsed = sorted(
        busy_list,
        key=lambda b: b["start"],
    )
    merged: List[Dict[str, str]] = [parsed[0]]
    for interval in parsed[1:]:
        if interval["start"] <= merged[-1]["end"]:
            if interval["end"] > merged[-1]["end"]:
                merged[-1]["end"] = interval["end"]
        else:
            merged.append(interval)
    return merged


def _find_free_slots(
    busy_intervals: List[Dict[str, str]],
    time_min: str,
    time_max: str,
    min_duration_minutes: int = 30,
) -> List[Dict[str, Any]]:
    """Compute free slots as the complement of busy intervals within the given range.

    Args:
        busy_intervals: Merged list of busy intervals (ISO 8601 strings).
        time_min: Start of search range (ISO 8601).
        time_max: End of search range (ISO 8601).
        min_duration_minutes: Minimum slot duration in minutes to include.

    Returns:
        List of free slot dicts with start, end, and duration_minutes.
    """
    from datetime import timezone as tz

    def parse_dt(s: str) -> datetime.datetime:
        # Handle both 'Z' suffix and +00:00 offset
        s = s.replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(s)

    range_start = parse_dt(time_min)
    range_end = parse_dt(time_max)
    min_delta = datetime.timedelta(minutes=min_duration_minutes)

    free_slots: List[Dict[str, Any]] = []
    cursor = range_start

    for interval in busy_intervals:
        busy_start = parse_dt(interval["start"])
        busy_end = parse_dt(interval["end"])

        if busy_start > cursor:
            gap = busy_start - cursor
            if gap >= min_delta:
                free_slots.append({
                    "start": cursor.isoformat(),
                    "end": busy_start.isoformat(),
                    "duration_minutes": int(gap.total_seconds() / 60),
                })
        if busy_end > cursor:
            cursor = busy_end

    # Remaining time after last busy interval
    if cursor < range_end:
        gap = range_end - cursor
        if gap >= min_delta:
            free_slots.append({
                "start": cursor.isoformat(),
                "end": range_end.isoformat(),
                "duration_minutes": int(gap.total_seconds() / 60),
            })

    return free_slots


# ---------------------------------------------------------------------------
# New tool: respond_to_event
# ---------------------------------------------------------------------------

@server.tool()
@handle_http_errors("respond_to_event", service_type="calendar")
@require_google_service("calendar", "calendar_events")
async def respond_to_event(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    event_id: str = Field(..., description="The ID of the event to respond to."),
    response: str = Field(..., description="Your attendance decision: 'accepted', 'declined', or 'tentative'."),
    calendar_id: str = Field("primary", description="The calendar containing the event."),
    comment: Optional[str] = Field(None, description="Optional message to send to the organizer with your response."),
    send_updates: Optional[str] = Field("all", description="Who receives notification: 'all' (default), 'externalOnly', or 'none'."),
) -> str:
    """
    Responds to a calendar event invitation (accept, decline, or tentative).

    Returns:
        str: Confirmation with your updated response status and event details.
    """
    logger.info(f"[respond_to_event] Invoked. Email: '{user_google_email}', Event ID: {event_id}, Response: {response}")

    valid_responses = {"accepted", "declined", "tentative"}
    if response not in valid_responses:
        raise Exception(f"Invalid response '{response}'. Must be one of: {', '.join(valid_responses)}")

    # Fetch the event to get the attendees list
    event = await asyncio.to_thread(
        lambda: service.events().get(calendarId=calendar_id, eventId=event_id).execute()
    )

    attendees = event.get("attendees", [])
    user_found = False
    for attendee in attendees:
        if attendee.get("self"):
            attendee["responseStatus"] = response
            if comment:
                attendee["comment"] = comment
            user_found = True
            break

    if not user_found:
        raise Exception(
            f"You ({user_google_email}) are not listed as an attendee of this event. "
            "You can only respond to events you've been invited to."
        )

    # Patch the event with updated attendees
    patch_kwargs = {
        "calendarId": calendar_id,
        "eventId": event_id,
        "body": {"attendees": attendees},
    }
    if send_updates:
        patch_kwargs["sendUpdates"] = send_updates

    updated_event = await asyncio.to_thread(
        lambda: service.events().patch(**patch_kwargs).execute()
    )

    mapped = _map_event(updated_event, compact=False)
    logger.info(f"[respond_to_event] Successfully responded '{response}' to event {event_id}")
    return success_response({"response": response, "comment": comment, "event": mapped})


# ---------------------------------------------------------------------------
# New tool: find_my_free_time
# ---------------------------------------------------------------------------

@server.tool()
@handle_http_errors("find_my_free_time", is_read_only=True, service_type="calendar")
@require_google_service("calendar", "calendar_read")
async def find_my_free_time(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    calendar_ids: List[str] = Field(..., description="List of calendar IDs to check for availability (e.g., ['primary'])."),
    time_min: str = Field(..., description="Start of time range to check in RFC3339 format (e.g., '2024-05-12T00:00:00Z')."),
    time_max: str = Field(..., description="End of time range to check in RFC3339 format (e.g., '2024-05-12T23:59:59Z')."),
    timezone: Optional[str] = Field(None, description="IANA timezone for interpreting times and displaying results (e.g., 'America/New_York')."),
    min_duration: int = Field(30, description="Minimum free slot duration in minutes to include. Defaults to 30."),
) -> str:
    """
    Finds free time slots in your personal calendar(s) where no events are scheduled.

    Analyzes your calendar(s) to find gaps between events, helping you identify
    available time for focused work, personal tasks, or new meetings.

    Returns:
        str: List of free time slots with start, end, and duration.
    """
    logger.info(f"[find_my_free_time] Invoked. Email: '{user_google_email}', Calendars: {calendar_ids}")

    formatted_time_min = _correct_time_format_for_api(time_min, "time_min") or time_min
    formatted_time_max = _correct_time_format_for_api(time_max, "time_max") or time_max

    # Build FreeBusy query
    freebusy_body: Dict[str, Any] = {
        "timeMin": formatted_time_min,
        "timeMax": formatted_time_max,
        "items": [{"id": cal_id} for cal_id in calendar_ids],
    }
    if timezone:
        freebusy_body["timeZone"] = timezone

    freebusy_response = await asyncio.to_thread(
        lambda: service.freebusy().query(body=freebusy_body).execute()
    )

    # Collect all busy intervals from all requested calendars
    all_busy: List[Dict[str, str]] = []
    calendars_data = freebusy_response.get("calendars", {})
    for cal_id in calendar_ids:
        cal_info = calendars_data.get(cal_id, {})
        all_busy.extend(cal_info.get("busy", []))

    # Merge overlapping busy intervals and compute free slots
    merged_busy = _merge_busy_intervals(all_busy)
    free_slots = _find_free_slots(merged_busy, formatted_time_min, formatted_time_max, min_duration)

    logger.info(f"[find_my_free_time] Found {len(free_slots)} free slots for {user_google_email}")
    return success_response({
        "timeRange": {"start": formatted_time_min, "end": formatted_time_max, "timeZone": timezone},
        "freeSlots": free_slots,
        "totalFreeSlots": len(free_slots),
    })


# ---------------------------------------------------------------------------
# New tool: find_meeting_times
# ---------------------------------------------------------------------------

@server.tool()
@handle_http_errors("find_meeting_times", is_read_only=True, service_type="calendar")
@require_google_service("calendar", "calendar_read")
async def find_meeting_times(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    attendees: List[str] = Field(..., description="List of email addresses to check availability for. The authenticated user is automatically included."),
    duration: int = Field(..., description="Required meeting duration in minutes (e.g., 30, 60, 90)."),
    time_min: str = Field(..., description="Start of search range in RFC3339 format (e.g., '2024-05-12T00:00:00Z')."),
    time_max: str = Field(..., description="End of search range in RFC3339 format (e.g., '2024-05-19T23:59:59Z')."),
    timezone: Optional[str] = Field(None, description="IANA timezone for interpreting times and displaying results (e.g., 'America/New_York')."),
    start_hour: int = Field(9, description="Earliest hour to start meetings (0-23). Default: 9."),
    end_hour: int = Field(17, description="Latest hour to end meetings (0-23). Default: 17."),
    exclude_weekends: bool = Field(True, description="Skip Saturday and Sunday. Default: True."),
    max_results: int = Field(5, description="Maximum number of available slots to return. Default: 5."),
) -> str:
    """
    Finds optimal meeting times when all specified attendees are available.

    Uses Google's FreeBusy API to check multiple calendars simultaneously and
    identify time slots where all attendees can meet. Results respect business
    hours and exclude weekends by default.

    Returns:
        str: Available meeting time slots with details.
    """
    logger.info(f"[find_meeting_times] Invoked. Email: '{user_google_email}', Attendees: {attendees}, Duration: {duration}min")

    formatted_time_min = _correct_time_format_for_api(time_min, "time_min") or time_min
    formatted_time_max = _correct_time_format_for_api(time_max, "time_max") or time_max

    # Ensure the authenticated user is included
    all_attendees = list(set(attendees + [user_google_email]))

    # Build FreeBusy query
    freebusy_body: Dict[str, Any] = {
        "timeMin": formatted_time_min,
        "timeMax": formatted_time_max,
        "items": [{"id": email} for email in all_attendees],
    }
    if timezone:
        freebusy_body["timeZone"] = timezone

    freebusy_response = await asyncio.to_thread(
        lambda: service.freebusy().query(body=freebusy_body).execute()
    )

    # Collect all busy intervals from all attendees
    all_busy: List[Dict[str, str]] = []
    calendars_data = freebusy_response.get("calendars", {})
    for email in all_attendees:
        cal_info = calendars_data.get(email, {})
        all_busy.extend(cal_info.get("busy", []))

    # Merge all busy intervals
    merged_busy = _merge_busy_intervals(all_busy)

    # Compute free slots across the entire range
    free_slots = _find_free_slots(merged_busy, formatted_time_min, formatted_time_max, duration)

    # Resolve the display/working timezone once. All wall-clock snapping
    # (start_hour, end_hour, weekday) must happen in this tz, otherwise
    # `.replace(hour=...)` on a UTC datetime sets the UTC hour rather than
    # the intended local hour — producing wrong slots or an infinite loop.
    if timezone:
        try:
            import zoneinfo
            tz = zoneinfo.ZoneInfo(timezone)
        except Exception:
            tz = datetime.timezone.utc
    else:
        tz = datetime.timezone.utc

    def _snap_to_start_hour(dt_utc: datetime.datetime) -> datetime.datetime:
        """Snap a UTC datetime to `start_hour` on the same local day."""
        local = dt_utc.astimezone(tz).replace(
            hour=start_hour, minute=0, second=0, microsecond=0
        )
        return local.astimezone(datetime.timezone.utc)

    def _advance_local_days(dt_utc: datetime.datetime, days: int) -> datetime.datetime:
        """Advance a UTC datetime by N local days, snapping to local start_hour."""
        local = dt_utc.astimezone(tz) + datetime.timedelta(days=days)
        local = local.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        return local.astimezone(datetime.timezone.utc)

    # Filter by business hours and weekday preferences
    meeting_duration = datetime.timedelta(minutes=duration)
    available_slots: List[Dict[str, Any]] = []

    for slot in free_slots:
        slot_start_str = slot["start"].replace("Z", "+00:00")
        slot_end_str = slot["end"].replace("Z", "+00:00")
        slot_start = datetime.datetime.fromisoformat(slot_start_str)
        slot_end = datetime.datetime.fromisoformat(slot_end_str)

        # Generate candidate meeting slots within this free window
        candidate = slot_start
        while candidate + meeting_duration <= slot_end and len(available_slots) < max_results:
            candidate_local = candidate.astimezone(tz)
            prev_candidate = candidate

            # Check weekday
            if exclude_weekends and candidate_local.weekday() >= 5:
                days_until_monday = 7 - candidate_local.weekday()
                candidate = _advance_local_days(candidate, days_until_monday)
                if candidate <= prev_candidate:
                    break
                continue

            # Check business hours
            if candidate_local.hour < start_hour:
                candidate = _snap_to_start_hour(candidate)
                if candidate <= prev_candidate:
                    break
                continue
            if candidate_local.hour >= end_hour:
                candidate = _advance_local_days(candidate, 1)
                if candidate <= prev_candidate:
                    break
                continue

            # Check the meeting would end within business hours
            candidate_end = candidate + meeting_duration
            candidate_end_local = candidate_end.astimezone(tz)

            if candidate_end_local.hour > end_hour or (
                candidate_end_local.hour == end_hour and candidate_end_local.minute > 0
            ):
                candidate = _advance_local_days(candidate, 1)
                if candidate <= prev_candidate:
                    break
                continue

            available_slots.append({
                "start": candidate.isoformat(),
                "end": candidate_end.isoformat(),
                "duration_minutes": duration,
            })

            # Move to next 15-minute increment
            candidate = candidate + datetime.timedelta(minutes=15)

        if len(available_slots) >= max_results:
            break

    logger.info(f"[find_meeting_times] Found {len(available_slots)} available slots for {len(all_attendees)} attendees")
    return success_response({
        "availableSlots": available_slots,
        "totalSlots": len(available_slots),
        "attendees": all_attendees,
        "duration_minutes": duration,
        "preferences": {
            "startHour": start_hour,
            "endHour": end_hour,
            "excludeWeekends": exclude_weekends,
        },
        "timeRange": {"start": formatted_time_min, "end": formatted_time_max, "timeZone": timezone},
    })
