import codecs
import ctypes
import ctypes.util
import gc
import io
import logging
import os
import tempfile
import time
import zipfile
import xml.etree.ElementTree as ET
import ssl
import asyncio
import functools

from typing import IO, Callable, List, Optional, Union

import httpx
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from mcp.server.fastmcp.exceptions import ToolError
from .api_enablement import get_api_enablement_message
from auth.google_auth import GoogleAuthenticationError

logger = logging.getLogger(__name__)

# Downloads are streamed to a temp file (deleted on close) instead of RAM, so the
# ceiling is bounded by the container's ephemeral storage, not its memory limit.
MAX_DOWNLOAD_BYTES = int(os.getenv("WORKSPACE_MCP_MAX_DOWNLOAD_MB", "500")) * 1024 * 1024
# googleapiclient defaults to 100MB chunks, i.e. 100MB held in memory per next_chunk().
TRANSFER_CHUNK_BYTES = 4 * 1024 * 1024

OFFICE_MIME_TYPES = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}
# Never decodable as text: report them without downloading the bytes at all.
BINARY_MIME_PREFIXES = ("image/", "video/", "audio/")

_libc = ctypes.CDLL(ctypes.util.find_library("c")) if ctypes.util.find_library("c") else None
_malloc_trim = getattr(_libc, "malloc_trim", None)  # glibc only (python:*-slim); absent on macOS/musl


class TransientNetworkError(Exception):
    """Custom exception for transient network errors after retries."""

    pass


class FileTooLargeError(Exception):
    """Raised when a download exceeds MAX_DOWNLOAD_BYTES."""

    def __init__(self, size: int, limit: int = MAX_DOWNLOAD_BYTES):
        super().__init__(
            f"File is too large to read ({size / 2**20:.1f}MB+, limit {limit / 2**20:.0f}MB). "
            "Share the file link with the user instead of reading its content."
        )


def _rss_mb() -> int:
    """Current resident memory of this process in MB (0 where /proc is unavailable)."""
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") // 2**20
    except (OSError, ValueError, IndexError):
        return 0


def release_memory() -> None:
    """Return freed heap to the OS after a large file was processed.

    Freed memory otherwise stays mapped in glibc's heap and counts against the
    container limit even though Python no longer uses it.
    """
    gc.collect()
    if _malloc_trim is not None:
        _malloc_trim(0)


async def download_drive_media(request_obj, fh: IO[bytes], max_bytes: int = MAX_DOWNLOAD_BYTES) -> int:
    """Stream a Drive get_media/export_media request into fh in small chunks. Returns the byte count."""
    downloader = MediaIoBaseDownload(fh, request_obj, chunksize=TRANSFER_CHUNK_BYTES)
    done = False
    while not done:
        status, done = await asyncio.to_thread(downloader.next_chunk)
        # total_size comes from Content-Range, so oversized files fail after the first chunk.
        size = max(fh.tell(), (status.total_size or 0) if status else 0)
        if size > max_bytes:
            raise FileTooLargeError(size, max_bytes)
    return fh.tell()


async def fetch_url_to_file(url: str, fh: IO[bytes], max_bytes: int = MAX_DOWNLOAD_BYTES) -> Optional[str]:
    """Stream a URL body into fh. Returns the response Content-Type."""
    async with httpx.AsyncClient() as client:
        async with client.stream("GET", url) as resp:
            if resp.status_code != 200:
                raise Exception(f"Failed to fetch file from URL: {url} (status {resp.status_code})")
            declared = int(resp.headers.get("Content-Length") or 0)
            if declared > max_bytes:
                raise FileTooLargeError(declared, max_bytes)
            async for chunk in resp.aiter_bytes():
                fh.write(chunk)
                if fh.tell() > max_bytes:
                    raise FileTooLargeError(fh.tell(), max_bytes)
            return resp.headers.get("Content-Type")


def _decode_utf8_stream(fh: IO[bytes]) -> Optional[str]:
    """Decode fh as UTF-8 chunk by chunk; None as soon as a chunk is not valid UTF-8."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    parts: List[str] = []
    try:
        while chunk := fh.read(TRANSFER_CHUNK_BYTES):
            parts.append(decoder.decode(chunk))
        parts.append(decoder.decode(b"", final=True))
    except UnicodeDecodeError:
        return None
    return "".join(parts)


async def read_drive_file_text(request_obj, mime_type: str, size_hint: Optional[int] = None) -> str:
    """Download a Drive file and return its readable text.

    Office XML files are parsed for text, anything else is decoded as UTF-8, and
    binary content is reported by size. The raw bytes only ever live in a temp
    file on disk; only the extracted text is held in memory.
    """
    binary_note = "[Binary or unsupported text encoding for mimeType '{mime}' - {size} bytes]"
    if mime_type.startswith(BINARY_MIME_PREFIXES):
        return binary_note.format(mime=mime_type, size=size_hint if size_hint is not None else "unknown")
    try:
        with tempfile.TemporaryFile() as fh:
            size = await download_drive_media(request_obj, fh)
            if mime_type in OFFICE_MIME_TYPES:
                fh.seek(0)
                office_text = await asyncio.to_thread(extract_office_xml_text, fh, mime_type)
                if office_text:
                    return office_text
            fh.seek(0)
            text = _decode_utf8_stream(fh)
            return text if text is not None else binary_note.format(mime=mime_type, size=size)
    finally:
        release_memory()


def _stream_xml(source: IO[bytes], consume: Callable[[ET.Element], bool]) -> None:
    """Incrementally parse XML, calling consume(elem) when each element closes.

    When consume returns True the element is detached from its parent, so memory
    stays proportional to the deepest open element rather than the whole document.
    Return False to keep an element until an ancestor consumes it.
    """
    stack: List[ET.Element] = []
    for event, elem in ET.iterparse(source, events=("start", "end")):
        if event == "start":
            stack.append(elem)
            continue
        stack.pop()
        if consume(elem) and stack:
            stack[-1].remove(elem)


def check_credentials_directory_permissions(credentials_dir: str = None) -> None:
    """
    Check if the service has appropriate permissions to create and write to the .credentials directory.

    Args:
        credentials_dir: Path to the credentials directory (default: uses get_default_credentials_dir())

    Raises:
        PermissionError: If the service lacks necessary permissions
        OSError: If there are other file system issues
    """
    if credentials_dir is None:
        from auth.google_auth import get_default_credentials_dir

        credentials_dir = get_default_credentials_dir()

    try:
        # Check if directory exists
        if os.path.exists(credentials_dir):
            # Directory exists, check if we can write to it
            test_file = os.path.join(credentials_dir, ".permission_test")
            try:
                with open(test_file, "w") as f:
                    f.write("test")
                os.remove(test_file)
                logger.info(
                    f"Credentials directory permissions check passed: {os.path.abspath(credentials_dir)}"
                )
            except (PermissionError, OSError) as e:
                raise PermissionError(
                    f"Cannot write to existing credentials directory '{os.path.abspath(credentials_dir)}': {e}"
                )
        else:
            # Directory doesn't exist, try to create it and its parent directories
            try:
                os.makedirs(credentials_dir, exist_ok=True)
                # Test writing to the new directory
                test_file = os.path.join(credentials_dir, ".permission_test")
                with open(test_file, "w") as f:
                    f.write("test")
                os.remove(test_file)
                logger.info(
                    f"Created credentials directory with proper permissions: {os.path.abspath(credentials_dir)}"
                )
            except (PermissionError, OSError) as e:
                # Clean up if we created the directory but can't write to it
                try:
                    if os.path.exists(credentials_dir):
                        os.rmdir(credentials_dir)
                except (PermissionError, OSError):
                    pass
                raise PermissionError(
                    f"Cannot create or write to credentials directory '{os.path.abspath(credentials_dir)}': {e}"
                )

    except PermissionError:
        raise
    except Exception as e:
        raise OSError(
            f"Unexpected error checking credentials directory permissions: {e}"
        )


def extract_office_xml_text(source: Union[bytes, IO[bytes]], mime_type: str) -> Optional[str]:
    """
    Very light-weight XML scraper for Word, Excel, PowerPoint files.
    Returns plain-text if something readable is found, else None.
    No external deps – just std-lib zipfile + ElementTree.

    Members are parsed incrementally: a 10MB .xlsx expands to ~100MB of XML, and
    building the full tree for it costs well over 1GB of RAM.
    """
    ns_excel_main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    tag_si, tag_t = f"{{{ns_excel_main}}}si", f"{{{ns_excel_main}}}t"
    tag_c, tag_v = f"{{{ns_excel_main}}}c", f"{{{ns_excel_main}}}v"
    is_excel = mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    shared_strings: List[str] = []

    try:
        with zipfile.ZipFile(io.BytesIO(source) if isinstance(source, bytes) else source) as zf:
            targets: List[str] = []
            # Map MIME → iterable of XML files to inspect
            if (
                mime_type
                == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            ):
                targets = ["word/document.xml"]
            elif (
                mime_type
                == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            ):
                targets = [n for n in zf.namelist() if n.startswith("ppt/slides/slide")]
            elif is_excel:
                targets = [
                    n
                    for n in zf.namelist()
                    if n.startswith("xl/worksheets/sheet") and "drawing" not in n
                ]

                # Concatenate every <t> inside each <si>, simple or within <r> runs
                def consume_shared_string(elem: ET.Element) -> bool:
                    if elem.tag == tag_si:
                        shared_strings.append("".join(t.text for t in elem.iter(tag_t) if t.text))
                        return True
                    return False  # keep runs/text until their <si> closes

                # Attempt to parse sharedStrings.xml for Excel files
                try:
                    with zf.open("xl/sharedStrings.xml") as f:
                        _stream_xml(f, consume_shared_string)
                except KeyError:
                    logger.info(
                        "No sharedStrings.xml found in Excel file (this is optional)."
                    )
                except ET.ParseError as e:
                    logger.error(f"Error parsing sharedStrings.xml: {e}")
                except (
                    Exception
                ) as e:  # Catch any other unexpected error during sharedStrings parsing
                    logger.error(
                        f"Unexpected error processing sharedStrings.xml: {e}",
                        exc_info=True,
                    )
            else:
                return None

            pieces: List[str] = []
            for member in targets:
                member_texts: List[str] = []

                def consume_cell(elem: ET.Element) -> bool:
                    if elem.tag == tag_v:
                        return False  # read when its <c> closes
                    if elem.tag != tag_c:
                        return True
                    value_element = elem.find(tag_v)
                    # Skip if cell has no value element or value element has no text
                    if value_element is None or value_element.text is None:
                        return True
                    if elem.get("t") == "s":  # Shared string
                        try:
                            ss_idx = int(value_element.text)
                            if 0 <= ss_idx < len(shared_strings):
                                member_texts.append(shared_strings[ss_idx])
                            else:
                                logger.warning(
                                    f"Invalid shared string index {ss_idx} in {member}. Max index: {len(shared_strings)-1}"
                                )
                        except ValueError:
                            logger.warning(
                                f"Non-integer shared string index: '{value_element.text}' in {member}."
                            )
                    else:  # Direct value (number, boolean, inline string if not 's')
                        member_texts.append(value_element.text)
                    return True

                def consume_text_run(elem: ET.Element) -> bool:
                    # Word: <w:t> (wordprocessingml), PowerPoint: <a:t> (drawingml)
                    if elem.tag.endswith("}t") and elem.text:
                        cleaned_text = elem.text.strip()
                        if cleaned_text:  # Add only if there's non-whitespace text
                            member_texts.append(cleaned_text)
                    return True

                try:
                    with zf.open(member) as f:
                        _stream_xml(f, consume_cell if is_excel else consume_text_run)
                    if member_texts:
                        pieces.append(
                            " ".join(member_texts)
                        )  # Join texts from one member with spaces

                except ET.ParseError as e:
                    logger.warning(
                        f"Could not parse XML in member '{member}' for {mime_type} file: {e}"
                    )
                except Exception as e:
                    logger.error(
                        f"Error processing member '{member}' for {mime_type}: {e}",
                        exc_info=True,
                    )
                    # continue processing other members

            if not pieces:  # If no text was extracted at all
                return None

            # Join content from different members (sheets/slides) with double newlines for separation
            text = "\n\n".join(pieces).strip()
            return text or None  # Ensure None is returned if text is empty after strip

    except zipfile.BadZipFile:
        logger.warning(f"File is not a valid ZIP archive (mime_type: {mime_type}).")
        return None
    except (
        ET.ParseError
    ) as e:  # Catch parsing errors at the top level if zipfile itself is XML-like
        logger.error(f"XML parsing error at a high level for {mime_type}: {e}")
        return None
    except Exception as e:
        logger.error(
            f"Failed to extract office XML text for {mime_type}: {e}", exc_info=True
        )
        return None


def handle_http_errors(tool_name: str, is_read_only: bool = False, service_type: Optional[str] = None):
    """
    A decorator to handle Google API HttpErrors and transient SSL errors in a standardized way.

    It wraps a tool function, catches HttpError, logs a detailed error message,
    and raises a generic Exception with a user-friendly message.

    If is_read_only is True, it will also catch ssl.SSLError and retry with
    exponential backoff. After exhausting retries, it raises a TransientNetworkError.

    Args:
        tool_name (str): The name of the tool being decorated (e.g., 'list_calendars').
        is_read_only (bool): If True, the operation is considered safe to retry on
                             transient network errors. Defaults to False.
        service_type (str): Optional. The Google service type (e.g., 'calendar', 'gmail').
    """

    def decorator(func):
        async def call_with_retries(*args, **kwargs):
            max_retries = 3
            base_delay = 1

            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except ssl.SSLError as e:
                    if is_read_only and attempt < max_retries - 1:
                        delay = base_delay * (2**attempt)
                        logger.warning(
                            f"SSL error in {tool_name} on attempt {attempt + 1}: {e}. Retrying in {delay} seconds..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            f"SSL error in {tool_name} on final attempt: {e}. Raising exception."
                        )
                        raise TransientNetworkError(
                            f"A transient SSL error occurred in '{tool_name}' after {max_retries} attempts. "
                            "This is likely a temporary network or certificate issue. Please try again shortly."
                        ) from e
                except HttpError as error:
                    user_google_email = kwargs.get("user_google_email", "N/A")
                    error_details = str(error)
                    status_code = error.resp.status

                    # Check if this is an API not enabled error
                    if status_code == 403 and "accessNotConfigured" in error_details:
                        enablement_msg = get_api_enablement_message(error_details, service_type)

                        if enablement_msg:
                            message = (
                                f"API error in {tool_name}: {enablement_msg} "
                                f"User: {user_google_email}"
                            )
                        else:
                            message = (
                                f"API error in {tool_name}: {error}. "
                                f"The required API is not enabled for your project. "
                                f"Please check the Google Cloud Console to enable it."
                            )
                    else:
                        message = (
                            f"API error in {tool_name}: {error}. "
                            f"You might need to re-authenticate for user '{user_google_email}'. "
                            f"LLM: Try 'start_google_auth' with the user's email and the appropriate service_name."
                        )

                    # Log tool parameters on error for debugging (exclude internal/sensitive keys)
                    _exclude_keys = {"service", "access_token", "token", "credentials"}
                    safe_params = {k: v for k, v in kwargs.items() if k not in _exclude_keys}
                    logger.error(
                        f"API error in {tool_name}: {error}\n"
                        f"  Tool params: {safe_params}",
                        exc_info=True
                    )
                    raise ToolError(message)
                except TransientNetworkError:
                    # Re-raise without wrapping to preserve the specific error type
                    raise
                except GoogleAuthenticationError:
                    # Re-raise authentication errors without wrapping
                    raise
                except Exception as e:
                    _exclude_keys = {"service", "access_token", "token", "credentials"}
                    safe_params = {k: v for k, v in kwargs.items() if k not in _exclude_keys}
                    message = f"An unexpected error occurred in {tool_name}: {e}"
                    logger.exception(f"{message}\n  Tool params: {safe_params}")
                    raise Exception(message) from e

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # One line per call so a memory jump in the container can be traced to the tool that caused it.
            started, rss_before, outcome = time.monotonic(), _rss_mb(), "error"
            try:
                result = await call_with_retries(*args, **kwargs)
                outcome = "ok"
                return result
            finally:
                rss_after = _rss_mb()
                logger.info(
                    f"[tool_call] tool={tool_name} outcome={outcome} "
                    f"duration_ms={(time.monotonic() - started) * 1000:.0f} "
                    f"rss_mb={rss_after} rss_delta_mb={rss_after - rss_before}"
                )

        return wrapper

    return decorator
