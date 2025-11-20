"""
Google Tasks MCP Tools

This module provides MCP tools for interacting with Google Tasks API.
"""

import logging
import asyncio
from typing import Optional

from googleapiclient.errors import HttpError
from pydantic import Field

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors

logger = logging.getLogger(__name__)


@server.tool()
@require_google_service("tasks", "tasks_read")
@handle_http_errors("list_task_lists", service_type="tasks")
async def list_task_lists(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    max_results: Optional[int] = Field(None, description="Maximum number of task lists to return. Defaults to 1000, maximum is 1000."),
    page_token: Optional[str] = Field(None, description="Token for retrieving the next page of results. Use the 'next_page_token' from the previous response to get more results."),
) -> str:
    """
    List all task lists for the user.

    Returns:
        str: List of task lists with their IDs, titles, and details.
    """
    logger.info(f"[list_task_lists] Invoked. Email: '{user_google_email}'")

    try:
        params = {}
        if max_results is not None:
            params["maxResults"] = max_results
        if page_token:
            params["pageToken"] = page_token

        result = await asyncio.to_thread(
            service.tasklists().list(**params).execute
        )

        task_lists = result.get("items", [])
        next_page_token = result.get("nextPageToken")

        if not task_lists:
            return f"No task lists found for {user_google_email}."

        response = f"Task Lists for {user_google_email}:\n"
        for task_list in task_lists:
            response += f"- {task_list['title']} (ID: {task_list['id']})\n"
            response += f"  Updated: {task_list.get('updated', 'N/A')}\n"

        if next_page_token:
            response += f"\nNext page token: {next_page_token}"

        logger.info(f"Found {len(task_lists)} task lists for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks_read")
@handle_http_errors("get_task_list", service_type="tasks")
async def get_task_list(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    task_list_id: str = Field(..., description="The ID of the task list to retrieve. Obtain this from list_task_lists results."),
) -> str:
    """
    Get details of a specific task list.

    Returns:
        str: Task list details including title, ID, and last updated time.
    """
    logger.info(f"[get_task_list] Invoked. Email: '{user_google_email}', Task List ID: {task_list_id}")

    try:
        task_list = await asyncio.to_thread(
            service.tasklists().get(tasklist=task_list_id).execute
        )

        response = f"""Task List Details for {user_google_email}:
- Title: {task_list['title']}
- ID: {task_list['id']}
- Updated: {task_list.get('updated', 'N/A')}
- Self Link: {task_list.get('selfLink', 'N/A')}"""

        logger.info(f"Retrieved task list '{task_list['title']}' for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks")
@handle_http_errors("create_task_list", service_type="tasks")
async def create_task_list(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    title: str = Field(..., description="The title of the new task list."),
) -> str:
    """
    Create a new task list.

    Returns:
        str: Confirmation message with the new task list ID and details.
    """
    logger.info(f"[create_task_list] Invoked. Email: '{user_google_email}', Title: '{title}'")

    try:
        body = {
            "title": title
        }

        result = await asyncio.to_thread(
            service.tasklists().insert(body=body).execute
        )

        response = f"""Task List Created for {user_google_email}:
- Title: {result['title']}
- ID: {result['id']}
- Created: {result.get('updated', 'N/A')}
- Self Link: {result.get('selfLink', 'N/A')}"""

        logger.info(f"Created task list '{title}' with ID {result['id']} for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks")
@handle_http_errors("update_task_list", service_type="tasks")
async def update_task_list(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    task_list_id: str = Field(..., description="The ID of the task list to update. Obtain this from list_task_lists results."),
    title: str = Field(..., description="The new title for the task list."),
) -> str:
    """
    Update an existing task list.

    Returns:
        str: Confirmation message with updated task list details.
    """
    logger.info(f"[update_task_list] Invoked. Email: '{user_google_email}', Task List ID: {task_list_id}, New Title: '{title}'")

    try:
        body = {
            "id": task_list_id,
            "title": title
        }

        result = await asyncio.to_thread(
            service.tasklists().update(tasklist=task_list_id, body=body).execute
        )

        response = f"""Task List Updated for {user_google_email}:
- Title: {result['title']}
- ID: {result['id']}
- Updated: {result.get('updated', 'N/A')}"""

        logger.info(f"Updated task list {task_list_id} with new title '{title}' for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks")
@handle_http_errors("delete_task_list", service_type="tasks")
async def delete_task_list(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    task_list_id: str = Field(..., description="The ID of the task list to delete. Obtain this from list_task_lists results. WARNING: This will also delete all tasks in the list."),
) -> str:
    """
    Delete a task list. Note: This will also delete all tasks in the list.

    Returns:
        str: Confirmation message.
    """
    logger.info(f"[delete_task_list] Invoked. Email: '{user_google_email}', Task List ID: {task_list_id}")

    try:
        await asyncio.to_thread(
            service.tasklists().delete(tasklist=task_list_id).execute
        )

        response = f"Task list {task_list_id} has been deleted for {user_google_email}. All tasks in this list have also been deleted."

        logger.info(f"Deleted task list {task_list_id} for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks_read")
@handle_http_errors("list_tasks", service_type="tasks")
async def list_tasks(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    task_list_id: str = Field(..., description="The ID of the task list to retrieve tasks from. Obtain this from list_task_lists results."),
    max_results: Optional[int] = Field(None, description="Maximum number of tasks to return. Defaults to 20, maximum is 100."),
    page_token: Optional[str] = Field(None, description="Token for retrieving the next page of results. Use the 'next_page_token' from the previous response to get more results."),
    show_completed: Optional[bool] = Field(None, description="Whether to include completed tasks. If True, includes completed tasks. If False, excludes them. If None, uses default (True)."),
    show_deleted: Optional[bool] = Field(None, description="Whether to include deleted tasks. If True, includes deleted tasks. If False, excludes them. If None, uses default (False)."),
    show_hidden: Optional[bool] = Field(None, description="Whether to include hidden tasks. If True, includes hidden tasks. If False, excludes them. If None, uses default (False)."),
    show_assigned: Optional[bool] = Field(None, description="Whether to include assigned tasks. If True, includes assigned tasks. If False, excludes them. If None, uses default (False)."),
    completed_max: Optional[str] = Field(None, description="Upper bound for completion date in RFC 3339 timestamp format (e.g., '2024-12-31T23:59:59Z'). Only tasks completed before this date will be returned."),
    completed_min: Optional[str] = Field(None, description="Lower bound for completion date in RFC 3339 timestamp format (e.g., '2024-01-01T00:00:00Z'). Only tasks completed after this date will be returned."),
    due_max: Optional[str] = Field(None, description="Upper bound for due date in RFC 3339 timestamp format (e.g., '2024-12-31T23:59:59Z'). Only tasks with due dates before this date will be returned."),
    due_min: Optional[str] = Field(None, description="Lower bound for due date in RFC 3339 timestamp format (e.g., '2024-01-01T00:00:00Z'). Only tasks with due dates after this date will be returned."),
    updated_min: Optional[str] = Field(None, description="Lower bound for last modification time in RFC 3339 timestamp format (e.g., '2024-01-01T00:00:00Z'). Only tasks modified after this time will be returned."),
) -> str:
    """
    List all tasks in a specific task list.

    Returns:
        str: List of tasks with their details.
    """
    logger.info(f"[list_tasks] Invoked. Email: '{user_google_email}', Task List ID: {task_list_id}")

    try:
        params = {"tasklist": task_list_id}
        if max_results is not None:
            params["maxResults"] = max_results
        if page_token:
            params["pageToken"] = page_token
        if show_completed is not None:
            params["showCompleted"] = show_completed
        if show_deleted is not None:
            params["showDeleted"] = show_deleted
        if show_hidden is not None:
            params["showHidden"] = show_hidden
        if show_assigned is not None:
            params["showAssigned"] = show_assigned
        if completed_max:
            params["completedMax"] = completed_max
        if completed_min:
            params["completedMin"] = completed_min
        if due_max:
            params["dueMax"] = due_max
        if due_min:
            params["dueMin"] = due_min
        if updated_min:
            params["updatedMin"] = updated_min

        result = await asyncio.to_thread(
            service.tasks().list(**params).execute
        )

        tasks = result.get("items", [])
        next_page_token = result.get("nextPageToken")

        if not tasks:
            return f"No tasks found in task list {task_list_id} for {user_google_email}."

        response = f"Tasks in list {task_list_id} for {user_google_email}:\n"
        for task in tasks:
            response += f"- {task.get('title', 'Untitled')} (ID: {task['id']})\n"
            response += f"  Status: {task.get('status', 'N/A')}\n"
            if task.get('due'):
                response += f"  Due: {task['due']}\n"
            if task.get('notes'):
                response += f"  Notes: {task['notes'][:100]}{'...' if len(task['notes']) > 100 else ''}\n"
            if task.get('completed'):
                response += f"  Completed: {task['completed']}\n"
            response += f"  Updated: {task.get('updated', 'N/A')}\n"
            response += "\n"

        if next_page_token:
            response += f"Next page token: {next_page_token}"

        logger.info(f"Found {len(tasks)} tasks in list {task_list_id} for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks_read")
@handle_http_errors("get_task", service_type="tasks")
async def get_task(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    task_list_id: str = Field(..., description="The ID of the task list containing the task. Obtain this from list_task_lists results."),
    task_id: str = Field(..., description="The ID of the task to retrieve. Obtain this from list_tasks results."),
) -> str:
    """
    Get details of a specific task.

    Returns:
        str: Task details including title, notes, status, due date, etc.
    """
    logger.info(f"[get_task] Invoked. Email: '{user_google_email}', Task List ID: {task_list_id}, Task ID: {task_id}")

    try:
        task = await asyncio.to_thread(
            service.tasks().get(tasklist=task_list_id, task=task_id).execute
        )

        response = f"""Task Details for {user_google_email}:
- Title: {task.get('title', 'Untitled')}
- ID: {task['id']}
- Status: {task.get('status', 'N/A')}
- Updated: {task.get('updated', 'N/A')}"""

        if task.get('due'):
            response += f"\n- Due Date: {task['due']}"
        if task.get('completed'):
            response += f"\n- Completed: {task['completed']}"
        if task.get('notes'):
            response += f"\n- Notes: {task['notes']}"
        if task.get('parent'):
            response += f"\n- Parent Task ID: {task['parent']}"
        if task.get('position'):
            response += f"\n- Position: {task['position']}"
        if task.get('selfLink'):
            response += f"\n- Self Link: {task['selfLink']}"
        if task.get('webViewLink'):
            response += f"\n- Web View Link: {task['webViewLink']}"

        logger.info(f"Retrieved task '{task.get('title', 'Untitled')}' for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks")
@handle_http_errors("create_task", service_type="tasks")
async def create_task(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    task_list_id: str = Field(..., description="The ID of the task list to create the task in. Obtain this from list_task_lists results."),
    title: str = Field(..., description="The title of the task."),
    notes: Optional[str] = Field(None, description="Notes or description for the task."),
    due: Optional[str] = Field(None, description="Due date in RFC 3339 timestamp format (e.g., '2024-12-31T23:59:59Z' or '2024-12-31')."),
    parent: Optional[str] = Field(None, description="Parent task ID to make this task a subtask. Obtain the parent task ID from list_tasks results."),
    previous: Optional[str] = Field(None, description="Previous sibling task ID for positioning. The new task will be inserted after this task. Obtain the previous task ID from list_tasks results."),
) -> str:
    """
    Create a new task in a task list.

    Returns:
        str: Confirmation message with the new task ID and details.
    """
    logger.info(f"[create_task] Invoked. Email: '{user_google_email}', Task List ID: {task_list_id}, Title: '{title}'")

    try:
        body = {
            "title": title
        }
        if notes:
            body["notes"] = notes
        if due:
            body["due"] = due

        params = {"tasklist": task_list_id, "body": body}
        if parent:
            params["parent"] = parent
        if previous:
            params["previous"] = previous

        result = await asyncio.to_thread(
            service.tasks().insert(**params).execute
        )

        response = f"""Task Created for {user_google_email}:
- Title: {result['title']}
- ID: {result['id']}
- Status: {result.get('status', 'N/A')}
- Updated: {result.get('updated', 'N/A')}"""

        if result.get('due'):
            response += f"\n- Due Date: {result['due']}"
        if result.get('notes'):
            response += f"\n- Notes: {result['notes']}"
        if result.get('webViewLink'):
            response += f"\n- Web View Link: {result['webViewLink']}"

        logger.info(f"Created task '{title}' with ID {result['id']} for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks")
@handle_http_errors("update_task", service_type="tasks")
async def update_task(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    task_list_id: str = Field(..., description="The ID of the task list containing the task. Obtain this from list_task_lists results."),
    task_id: str = Field(..., description="The ID of the task to update. Obtain this from list_tasks results."),
    title: Optional[str] = Field(None, description="New title for the task. If not provided, the existing title is preserved."),
    notes: Optional[str] = Field(None, description="New notes or description for the task. If not provided, the existing notes are preserved."),
    status: Optional[str] = Field(None, description="New status for the task. Options: 'needsAction' (task is not completed), 'completed' (task is completed). If not provided, the existing status is preserved."),
    due: Optional[str] = Field(None, description="New due date in RFC 3339 timestamp format (e.g., '2024-12-31T23:59:59Z' or '2024-12-31'). If not provided, the existing due date is preserved."),
) -> str:
    """
    Update an existing task.

    Returns:
        str: Confirmation message with updated task details.
    """
    logger.info(f"[update_task] Invoked. Email: '{user_google_email}', Task List ID: {task_list_id}, Task ID: {task_id}")

    try:
        # First get the current task to build the update body
        current_task = await asyncio.to_thread(
            service.tasks().get(tasklist=task_list_id, task=task_id).execute
        )

        body = {
            "id": task_id,
            "title": title if title is not None else current_task.get("title", ""),
            "status": status if status is not None else current_task.get("status", "needsAction")
        }

        if notes is not None:
            body["notes"] = notes
        elif current_task.get("notes"):
            body["notes"] = current_task["notes"]

        if due is not None:
            body["due"] = due
        elif current_task.get("due"):
            body["due"] = current_task["due"]

        result = await asyncio.to_thread(
            service.tasks().update(tasklist=task_list_id, task=task_id, body=body).execute
        )

        response = f"""Task Updated for {user_google_email}:
- Title: {result['title']}
- ID: {result['id']}
- Status: {result.get('status', 'N/A')}
- Updated: {result.get('updated', 'N/A')}"""

        if result.get('due'):
            response += f"\n- Due Date: {result['due']}"
        if result.get('notes'):
            response += f"\n- Notes: {result['notes']}"
        if result.get('completed'):
            response += f"\n- Completed: {result['completed']}"

        logger.info(f"Updated task {task_id} for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks")
@handle_http_errors("delete_task", service_type="tasks")
async def delete_task(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    task_list_id: str = Field(..., description="The ID of the task list containing the task. Obtain this from list_task_lists results."),
    task_id: str = Field(..., description="The ID of the task to delete. Obtain this from list_tasks results."),
) -> str:
    """
    Delete a task from a task list.

    Returns:
        str: Confirmation message.
    """
    logger.info(f"[delete_task] Invoked. Email: '{user_google_email}', Task List ID: {task_list_id}, Task ID: {task_id}")

    try:
        await asyncio.to_thread(
            service.tasks().delete(tasklist=task_list_id, task=task_id).execute
        )

        response = f"Task {task_id} has been deleted from task list {task_list_id} for {user_google_email}."

        logger.info(f"Deleted task {task_id} for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks")
@handle_http_errors("move_task", service_type="tasks")
async def move_task(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    task_list_id: str = Field(..., description="The ID of the current task list containing the task. Obtain this from list_task_lists results."),
    task_id: str = Field(..., description="The ID of the task to move. Obtain this from list_tasks results."),
    parent: Optional[str] = Field(None, description="New parent task ID to make this task a subtask. Obtain the parent task ID from list_tasks results. If not provided, the task remains at the top level."),
    previous: Optional[str] = Field(None, description="Previous sibling task ID for positioning. The task will be moved to appear after this task. Obtain the previous task ID from list_tasks results. If not provided, the task is moved to the end."),
    destination_task_list: Optional[str] = Field(None, description="Destination task list ID for moving the task to a different list. Obtain this from list_task_lists results. If not provided, the task stays in the current list."),
) -> str:
    """
    Move a task to a different position or parent within the same list, or to a different list.

    Returns:
        str: Confirmation message with updated task details.
    """
    logger.info(f"[move_task] Invoked. Email: '{user_google_email}', Task List ID: {task_list_id}, Task ID: {task_id}")

    try:
        params = {
            "tasklist": task_list_id,
            "task": task_id
        }
        if parent:
            params["parent"] = parent
        if previous:
            params["previous"] = previous
        if destination_task_list:
            params["destinationTasklist"] = destination_task_list

        result = await asyncio.to_thread(
            service.tasks().move(**params).execute
        )

        response = f"""Task Moved for {user_google_email}:
- Title: {result['title']}
- ID: {result['id']}
- Status: {result.get('status', 'N/A')}
- Updated: {result.get('updated', 'N/A')}"""

        if result.get('parent'):
            response += f"\n- Parent Task ID: {result['parent']}"
        if result.get('position'):
            response += f"\n- Position: {result['position']}"

        move_details = []
        if destination_task_list:
            move_details.append(f"moved to task list {destination_task_list}")
        if parent:
            move_details.append(f"made a subtask of {parent}")
        if previous:
            move_details.append(f"positioned after {previous}")

        if move_details:
            response += f"\n- Move Details: {', '.join(move_details)}"

        logger.info(f"Moved task {task_id} for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)


@server.tool()
@require_google_service("tasks", "tasks")
@handle_http_errors("clear_completed_tasks", service_type="tasks")
async def clear_completed_tasks(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    task_list_id: str = Field(..., description="The ID of the task list to clear completed tasks from. Obtain this from list_task_lists results. WARNING: All completed tasks will be marked as hidden."),
) -> str:
    """
    Clear all completed tasks from a task list. The tasks will be marked as hidden.

    Returns:
        str: Confirmation message.
    """
    logger.info(f"[clear_completed_tasks] Invoked. Email: '{user_google_email}', Task List ID: {task_list_id}")

    try:
        await asyncio.to_thread(
            service.tasks().clear(tasklist=task_list_id).execute
        )

        response = f"All completed tasks have been cleared from task list {task_list_id} for {user_google_email}. The tasks are now hidden and won't appear in default task list views."

        logger.info(f"Cleared completed tasks from list {task_list_id} for {user_google_email}")
        return response

    except HttpError as error:
        message = f"API error: {error}. You might need to re-authenticate. LLM: Try 'start_google_auth' with the user's email ({user_google_email}) and service_name='Google Tasks'."
        logger.error(message, exc_info=True)
        raise Exception(message)
    except Exception as e:
        message = f"Unexpected error: {e}."
        logger.exception(message)
        raise Exception(message)