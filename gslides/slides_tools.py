"""
Google Slides MCP Tools

Provides MCP tools for creating and editing Google Slides presentations.
Standard slide: 720pt wide × 405pt tall. Origin (0,0) = top-left.
"""

import html
import logging
import asyncio
import uuid
import math
import re
from typing import List, Dict, Any, Literal, Optional

import base64
import httpx
from pydantic import Field
from mcp.types import TextContent, ImageContent

from googleapiclient.errors import HttpError

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors
from mcp.server.fastmcp.exceptions import ToolError
from core.response import success_response
from core.comments import create_comment_tools

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Layout -> placeholder type mapping
# Each entry maps (API placeholder type, parameter name used in add_slide)
LAYOUT_PLACEHOLDERS = {
    "TITLE_AND_BODY": [("TITLE", "title"), ("BODY", "body")],
    "TITLE_AND_TWO_COLUMNS": [("TITLE", "title"), ("BODY", "body")],
    "SECTION_HEADER": [("TITLE", "title"), ("SUBTITLE", "body")],
    "TITLE_ONLY": [("TITLE", "title")],
    "TITLE": [("CENTERED_TITLE", "title"), ("SUBTITLE", "body")],
    "ONE_COLUMN_TEXT": [("TITLE", "title"), ("BODY", "body")],
    "MAIN_POINT": [("TITLE", "title")],
    "BIG_NUMBER": [("TITLE", "title"), ("BODY", "body")],
    "CAPTION_ONLY": [("BODY", "body")],
    "BLANK": [],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Markdown patterns to strip from LLM-generated text
_MARKDOWN_PATTERNS = [
    (re.compile(r'\*\*\*(.+?)\*\*\*'), r'\1'),     # ***bold italic***
    (re.compile(r'\*\*(.+?)\*\*'), r'\1'),           # **bold**
    (re.compile(r'(?<!\w)\*(.+?)\*(?!\w)'), r'\1'),  # *italic* (not mid-word)
    (re.compile(r'__(.+?)__'), r'\1'),               # __bold__
    (re.compile(r'(?<!\w)_(.+?)_(?!\w)'), r'\1'),    # _italic_ (not mid-word)
    (re.compile(r'~~(.+?)~~'), r'\1'),               # ~~strikethrough~~
    (re.compile(r'`(.+?)`'), r'\1'),                 # `code`
    (re.compile(r'^#{1,6}\s+', re.MULTILINE), ''),   # # headings
    (re.compile(r'\[([^\]]+)\]\([^\)]+\)'), r'\1'),  # [text](url)
]

# Bullet-like prefixes that LLMs add (we strip them when bullets=True)
_BULLET_PREFIX = re.compile(r'^[\s]*[•●○■▪►➤\-\*]\s+', re.MULTILINE)


def _clean_text(text: Optional[str], strip_bullets: bool = False) -> Optional[str]:
    """Strip markdown syntax, fix escape sequences, decode HTML entities, and optionally strip bullet prefixes."""
    if not text:
        return text
    # Fix literal escape sequences: LLMs often send \\n and \\t as two-char strings
    # instead of actual newline/tab characters. Convert them.
    text = text.replace('\\n', '\n').replace('\\t', '\t')
    # Decode HTML entities: LLMs often produce &amp; &lt; &gt; etc.
    text = html.unescape(text)
    for pattern, replacement in _MARKDOWN_PATTERNS:
        text = pattern.sub(replacement, text)
    if strip_bullets:
        text = _BULLET_PREFIX.sub('', text)
    # Collapse blank lines to prevent empty bullet points
    text = re.sub(r'\n{2,}', '\n', text)
    return text


def _extract_text(text_content: Optional[dict]) -> Optional[str]:
    """Extract plain text from a textElements array."""
    if not text_content:
        return None
    parts = [
        te.get("textRun", {}).get("content", "")
        for te in text_content.get("textElements", [])
        if "textRun" in te
    ]
    text = "".join(parts).strip()
    return text if text else None


def _hex_to_rgb(hex_color: str) -> Dict[str, float]:
    """Convert '#RRGGBB' to Google Slides API RGB dict (0.0-1.0 floats)."""
    hex_color = hex_color.lstrip('#')
    return {
        "red": int(hex_color[0:2], 16) / 255.0,
        "green": int(hex_color[2:4], 16) / 255.0,
        "blue": int(hex_color[4:6], 16) / 255.0,
    }


def _rgb_to_hex(rgb: dict) -> Optional[str]:
    """Convert Google Slides API RGB dict (0.0-1.0) to '#RRGGBB'."""
    if not rgb:
        return None
    r = int(rgb.get("red", 0) * 255)
    g = int(rgb.get("green", 0) * 255)
    b = int(rgb.get("blue", 0) * 255)
    return f"#{r:02X}{g:02X}{b:02X}"


def _pt_to_emu(pt: float) -> int:
    """Convert points to EMU (English Metric Units). 1 pt = 12700 EMU."""
    return int(pt * 12700)


def _emu_to_pt(emu) -> float:
    """Convert EMU to points. 1 pt = 12700 EMU."""
    return round(emu / 12700, 1) if emu else 0


def _element_properties(slide_id: str, x: float, y: float, width: float, height: float) -> Dict:
    """Build a PageElementProperties dict for creating elements on a slide."""
    return {
        "pageObjectId": slide_id,
        "size": {
            "width": {"magnitude": _pt_to_emu(width), "unit": "EMU"},
            "height": {"magnitude": _pt_to_emu(height), "unit": "EMU"},
        },
        "transform": {
            "scaleX": 1,
            "scaleY": 1,
            "translateX": _pt_to_emu(x),
            "translateY": _pt_to_emu(y),
            "unit": "EMU",
        },
    }


async def _batch_update(service, presentation_id: str, requests: list) -> dict:
    """Execute a batchUpdate call on the Slides API."""
    return await asyncio.to_thread(
        service.presentations().batchUpdate(
            presentationId=presentation_id,
            body={"requests": requests}
        ).execute
    )


async def _get_element_transform(service, presentation_id: str, element_id: str) -> dict:
    """Fetch the current transform of a page element."""
    result = await asyncio.to_thread(
        service.presentations().get(
            presentationId=presentation_id,
            fields="slides.pageElements.objectId,slides.pageElements.transform"
        ).execute
    )
    for slide in result.get("slides", []):
        for elem in slide.get("pageElements", []):
            if elem.get("objectId") == element_id:
                return elem.get("transform", {})
    return {}


async def _get_element_data(service, presentation_id: str, element_id: str) -> Optional[dict]:
    """Fetch the current size and transform of a page element."""
    result = await asyncio.to_thread(
        service.presentations().get(
            presentationId=presentation_id,
            fields="slides.pageElements.objectId,slides.pageElements.size,slides.pageElements.transform"
        ).execute
    )
    for slide in result.get("slides", []):
        for elem in slide.get("pageElements", []):
            if elem.get("objectId") == element_id:
                return {
                    "size": elem.get("size", {}),
                    "transform": elem.get("transform", {}),
                }
    return None


async def _get_speaker_notes_id(service, presentation_id: str, slide_id: str) -> Optional[str]:
    """Find the speaker notes shape ID for a given slide."""
    page = await asyncio.to_thread(
        service.presentations().pages().get(
            presentationId=presentation_id,
            pageObjectId=slide_id
        ).execute
    )
    return _find_notes_shape_id(page)


def _find_notes_shape_id(page: dict) -> Optional[str]:
    """Extract the speaker notes shape ID from a page dict."""
    notes_page = page.get("slideProperties", {}).get("notesPage", {})
    for elem in notes_page.get("pageElements", []):
        shape = elem.get("shape", {})
        placeholder = shape.get("placeholder", {})
        if placeholder.get("type") == "BODY":
            return elem.get("objectId")
    return None


def _notes_has_text(page: dict, notes_shape_id: str) -> bool:
    """Check if the speaker notes shape has non-empty text content."""
    notes_page = page.get("slideProperties", {}).get("notesPage", {})
    for elem in notes_page.get("pageElements", []):
        if elem.get("objectId") == notes_shape_id:
            text_elements = elem.get("shape", {}).get("text", {}).get("textElements", [])
            for te in text_elements:
                if te.get("textRun", {}).get("content", "").strip():
                    return True
            return False
    return False


def _find_placeholders(page_elements: list) -> Dict[str, str]:
    """Find placeholder element IDs by type from page elements."""
    placeholders = {}
    for elem in page_elements:
        shape = elem.get("shape", {})
        placeholder = shape.get("placeholder", {})
        p_type = placeholder.get("type")
        if p_type:
            placeholders[p_type] = elem.get("objectId")
    return placeholders


def _has_text(page_elements: list, element_id: str) -> bool:
    """Check if a page element has non-empty text content."""
    for elem in page_elements:
        if elem.get("objectId") == element_id:
            text_elements = elem.get("shape", {}).get("text", {}).get("textElements", [])
            for te in text_elements:
                if te.get("textRun", {}).get("content", "").strip():
                    return True
            return False
    return False


def _map_slide(slide: Dict[str, Any], index: int) -> Dict[str, Any]:
    """Map a raw slide to a clean shape with content extraction."""
    mapped = {
        "index": index,
        "slide_id": slide.get("objectId"),
        "element_count": len(slide.get("pageElements", [])),
    }
    # Extract text from all placeholders
    for elem in slide.get("pageElements", []):
        shape = elem.get("shape", {})
        placeholder = shape.get("placeholder", {})
        p_type = placeholder.get("type")
        if not p_type:
            continue
        text = _extract_text(shape.get("text"))
        if not text:
            continue
        if p_type in ("TITLE", "CENTERED_TITLE"):
            mapped["title"] = text
        elif p_type == "BODY":
            mapped["body"] = text
        elif p_type == "SUBTITLE":
            mapped["subtitle"] = text
    return mapped


def _map_page_element(element: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw page element to a structured dict with position, text, and style."""
    mapped = {"id": element.get("objectId")}

    # Position (common to all element types)
    transform = element.get("transform", {})
    size = element.get("size", {})
    mapped["position"] = {
        "x": _emu_to_pt(transform.get("translateX", 0)),
        "y": _emu_to_pt(transform.get("translateY", 0)),
        "width": _emu_to_pt(size.get("width", {}).get("magnitude", 0)),
        "height": _emu_to_pt(size.get("height", {}).get("magnitude", 0)),
    }

    if "shape" in element:
        mapped["type"] = "shape"
        mapped["shape_type"] = element["shape"].get("shapeType")
        placeholder = element["shape"].get("placeholder", {})
        if placeholder.get("type"):
            mapped["placeholder_type"] = placeholder["type"]

        # Plain text + length
        text = _extract_text(element["shape"].get("text"))
        mapped["text"] = text
        mapped["text_length"] = len(text) if text else 0

        # Text runs with formatting (only non-default props included)
        text_elements = element["shape"].get("text", {}).get("textElements", [])
        text_runs = []
        for te in text_elements:
            tr = te.get("textRun")
            if not tr:
                continue
            style = tr.get("style", {})
            run = {
                "content": tr.get("content", ""),
                "start_index": te.get("startIndex", 0),
                "end_index": te.get("endIndex", 0),
            }
            if style.get("bold"):
                run["bold"] = True
            if style.get("italic"):
                run["italic"] = True
            if style.get("underline"):
                run["underline"] = True
            if style.get("fontSize"):
                run["font_size"] = style["fontSize"].get("magnitude")
            if style.get("fontFamily"):
                run["font_family"] = style["fontFamily"]
            fg = style.get("foregroundColor", {}).get("opaqueColor", {}).get("rgbColor")
            if fg:
                run["color"] = _rgb_to_hex(fg)
            link_url = style.get("link", {}).get("url")
            if link_url:
                run["link_url"] = link_url
            text_runs.append(run)
        if text_runs:
            mapped["text_runs"] = text_runs

        # Shape fill/outline colors
        props = element["shape"].get("shapeProperties", {})
        fill_rgb = props.get("shapeBackgroundFill", {}).get("solidFill", {}).get("color", {}).get("rgbColor")
        if fill_rgb:
            mapped["fill_color"] = _rgb_to_hex(fill_rgb)
        outline_rgb = props.get("outline", {}).get("outlineFill", {}).get("solidFill", {}).get("color", {}).get("rgbColor")
        if outline_rgb:
            mapped["outline_color"] = _rgb_to_hex(outline_rgb)

    elif "table" in element:
        mapped["type"] = "table"
        table = element["table"]
        mapped["rows"] = table.get("rows")
        mapped["columns"] = table.get("columns")
        cells = []
        for row in table.get("tableRows", []):
            row_data = []
            for cell in row.get("tableCells", []):
                row_data.append(_extract_text(cell.get("text")) or "")
            cells.append(row_data)
        if cells:
            mapped["cell_data"] = cells

    elif "line" in element:
        mapped["type"] = "line"
        line = element["line"]
        mapped["line_type"] = line.get("lineType")
        mapped["line_category"] = line.get("lineCategory")
        props = line.get("lineProperties", {})
        weight = props.get("weight", {}).get("magnitude")
        if weight:
            mapped["weight_pt"] = weight
        dash = props.get("dashStyle")
        if dash:
            mapped["dash_style"] = dash
        fill_rgb = props.get("lineFill", {}).get("solidFill", {}).get("color", {}).get("rgbColor")
        if fill_rgb:
            mapped["color"] = _rgb_to_hex(fill_rgb)

    elif "image" in element:
        mapped["type"] = "image"
        img = element["image"]
        mapped["content_url"] = img.get("contentUrl")
        mapped["source_url"] = img.get("sourceUrl")

    elif "video" in element:
        mapped["type"] = "video"
        vid = element["video"]
        mapped["video_source"] = vid.get("source")
        mapped["video_id"] = vid.get("id")
        mapped["video_url"] = vid.get("url")

    elif "sheetsChart" in element:
        mapped["type"] = "sheets_chart"
        chart = element["sheetsChart"]
        mapped["spreadsheet_id"] = chart.get("spreadsheetId")
        mapped["chart_id"] = chart.get("chartId")
        mapped["content_url"] = chart.get("contentUrl")

    elif "wordArt" in element:
        mapped["type"] = "word_art"
        mapped["rendered_text"] = element["wordArt"].get("renderedText")

    elif "elementGroup" in element:
        mapped["type"] = "group"
        children = element["elementGroup"].get("children", [])
        mapped["children"] = [_map_page_element(c) for c in children]

    else:
        mapped["type"] = "unknown"
    return mapped


async def _add_single_slide(service, presentation_id: str, title: Optional[str],
                            body: Optional[str], speaker_notes: Optional[str],
                            layout: str, insertion_index: Optional[int],
                            bullets: bool) -> Dict[str, Any]:
    """Internal helper: create one slide with content. Returns slide info dict."""
    title = _clean_text(title)
    body = _clean_text(body, strip_bullets=bullets)
    speaker_notes = _clean_text(speaker_notes)

    slide_id = uuid.uuid4().hex

    # Step 1: Create slide WITHOUT placeholder mappings — let Google assign IDs
    create_req = {
        "createSlide": {
            "objectId": slide_id,
            "slideLayoutReference": {"predefinedLayout": layout},
        }
    }
    if insertion_index is not None:
        create_req["createSlide"]["insertionIndex"] = insertion_index
    await _batch_update(service, presentation_id, [create_req])

    # Step 2: Read the slide back to find actual placeholder IDs
    page = await asyncio.to_thread(
        service.presentations().pages().get(
            presentationId=presentation_id,
            pageObjectId=slide_id
        ).execute
    )
    found_placeholders = _find_placeholders(page.get('pageElements', []))

    # Step 3: Map params to placeholder types and insert text
    placeholder_ids = {}
    layout_spec = LAYOUT_PLACEHOLDERS.get(layout, [])
    requests = []
    body_element_id = None

    for p_type, p_name in layout_spec:
        element_id = found_placeholders.get(p_type)
        if element_id:
            placeholder_ids[p_name] = element_id
            text = title if p_name == "title" else body
            if text:
                requests.append({
                    "insertText": {
                        "objectId": element_id,
                        "text": text,
                        "insertionIndex": 0,
                    }
                })
                if p_name == "body":
                    body_element_id = element_id

    # Step 4: Bullets for body
    if bullets and body_element_id and body:
        requests.append({
            "createParagraphBullets": {
                "objectId": body_element_id,
                "textRange": {"type": "ALL"},
                "bulletPreset": "BULLET_DISC_CIRCLE_SQUARE",
            }
        })

    if requests:
        await _batch_update(service, presentation_id, requests)

    # Step 5: Speaker notes (requires finding notes shape ID)
    if speaker_notes:
        notes_shape_id = await _get_speaker_notes_id(service, presentation_id, slide_id)
        if notes_shape_id:
            await _batch_update(service, presentation_id, [{
                "insertText": {
                    "objectId": notes_shape_id,
                    "text": speaker_notes,
                    "insertionIndex": 0,
                }
            }])

    return {
        "slide_id": slide_id,
        "placeholder_ids": placeholder_ids,
        "layout": layout,
    }


# ---------------------------------------------------------------------------
# Presentation Management Tools
# ---------------------------------------------------------------------------

@server.tool()
@handle_http_errors("create_presentation", service_type="slides")
@require_google_service("slides", "slides")
async def create_presentation(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    title: str = Field("Untitled Presentation", description="The title for the new presentation. This title is also auto-filled into the first slide's heading. PLAIN TEXT ONLY — no markdown (**, #, -). Markdown renders as literal characters on slide."),
    subtitle: str = Field(None, description="Optional subtitle for the auto-generated first (cover) slide. PLAIN TEXT ONLY — no markdown (**, #, -). Markdown renders as literal characters on slide. Example: 'Prepared by Strategy Team — March 2026'"),
    slides: Optional[List[Dict[str, Any]]] = Field(None, description="""Optional list of slide definitions to create after the cover slide, so you can build an entire deck in one call.

Each slide is a dict with these keys (all optional except layout):
  {
    "layout": "TITLE_AND_BODY",   (default if omitted)
    "title": "Slide title text",
    "body": "Body text with lines separated by \\n",
    "speaker_notes": "Presenter notes",
    "bullets": true/false          (default false — set true to auto-add bullet points to body)
  }

AVAILABLE LAYOUTS:
  "TITLE"                 -> Cover/opening slide (centered title + subtitle)
  "TITLE_AND_BODY"        -> Standard content slide with bullet points or paragraphs
  "SECTION_HEADER"        -> Section divider between topics (title + subtitle)
  "TITLE_ONLY"            -> Slide with title only — add images/shapes/tables separately
  "TITLE_AND_TWO_COLUMNS" -> Side-by-side comparison content
  "ONE_COLUMN_TEXT"        -> Long-form text content
  "MAIN_POINT"            -> Single impactful statement or takeaway (title only)
  "BIG_NUMBER"            -> Key statistic or KPI highlight (title=number, body=label)
  "CAPTION_ONLY"          -> Quote, footnote, or attribution (body only)
  "BLANK"                 -> Fully custom — add all elements manually

TEXT RULES — PLAIN TEXT ONLY:
  Use \\n for new lines.
  Use \\t for indent levels (with bullets).
  Do NOT use markdown (**, *, #, -, •).
  Set "bullets": true to auto-add bullet points — do NOT manually add bullet characters.

FULL PROFESSIONAL DECK EXAMPLE:
[
  {"layout": "TITLE_AND_BODY", "title": "Key Metrics", "body": "Revenue grew 34% YoY\\n12 new enterprise clients\\nNPS score reached 72", "bullets": true},
  {"layout": "BIG_NUMBER", "title": "$4.2M", "body": "Annual Recurring Revenue"},
  {"layout": "SECTION_HEADER", "title": "Market Analysis", "body": "Understanding our competitive landscape"},
  {"layout": "TITLE_AND_BODY", "title": "Regional Revenue", "body": "North America: $2.1M (+28%)\\nEurope: $1.4M (+41%)\\nAsia-Pacific: $0.7M (+63%)", "bullets": true, "speaker_notes": "Highlight APAC as fastest growing region"},
  {"layout": "TITLE_AND_BODY", "title": "Product Roadmap", "body": "Q1 Deliverables\\n\\tCheckout redesign\\n\\tMobile app v2.0\\nQ2 Deliverables\\n\\tAI recommendations\\n\\tPartner API launch", "bullets": true},
  {"layout": "MAIN_POINT", "title": "We exceeded every target this quarter."},
  {"layout": "TITLE", "title": "Thank You", "body": "Questions? team@company.com"}
]"""),
) -> str:
    """
    Create a new Google Slides presentation with an auto-filled cover slide and optional additional slides.

    The first slide is automatically populated with the title and optional subtitle.
    Pass the 'slides' parameter to build an entire deck in one call.

    Returns:
        JSON with presentation_id, title, link, slide_count, and slides_created details.
    """
    logger.info(f"[create_presentation] Invoked. Email: '{user_google_email}', Title: '{title}'")

    body = {'title': title}
    result = await asyncio.to_thread(
        service.presentations().create(body=body).execute
    )

    presentation_id = result.get('presentationId')
    created_slides = result.get('slides', [])
    slides_info = []

    # Auto-fill the default first slide with title and subtitle
    if created_slides:
        first_slide = created_slides[0]
        first_slide_id = first_slide.get('objectId')
        page_elements = first_slide.get('pageElements', [])
        placeholders = _find_placeholders(page_elements)

        fill_requests = []
        title_placeholder_id = placeholders.get("CENTERED_TITLE") or placeholders.get("TITLE")
        subtitle_placeholder_id = placeholders.get("SUBTITLE") or placeholders.get("BODY")

        if title_placeholder_id and title:
            fill_requests.append({
                "insertText": {
                    "objectId": title_placeholder_id,
                    "text": _clean_text(title),
                    "insertionIndex": 0,
                }
            })
        if subtitle_placeholder_id and subtitle:
            fill_requests.append({
                "insertText": {
                    "objectId": subtitle_placeholder_id,
                    "text": _clean_text(subtitle),
                    "insertionIndex": 0,
                }
            })

        if fill_requests:
            await _batch_update(service, presentation_id, fill_requests)

        slides_info.append({
            "slide_id": first_slide_id,
            "layout": "TITLE",
            "role": "cover",
        })

    # Create additional slides if provided
    if slides:
        for i, slide_def in enumerate(slides):
            slide_layout = slide_def.get("layout", "TITLE_AND_BODY")
            info = await _add_single_slide(
                service, presentation_id,
                title=slide_def.get("title"),
                body=slide_def.get("body"),
                speaker_notes=slide_def.get("speaker_notes"),
                layout=slide_layout,
                insertion_index=None,  # append to end
                bullets=slide_def.get("bullets", False),
            )
            slides_info.append(info)

    total_slides = 1 + (len(slides) if slides else 0)
    logger.info(f"Presentation created successfully for {user_google_email} with {total_slides} slides")
    return success_response({
        "presentation_id": presentation_id,
        "title": title,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
        "slide_count": total_slides,
        "slides_created": slides_info,
    })


@server.tool()
@handle_http_errors("get_presentation", is_read_only=True, service_type="slides")
@require_google_service("slides", "slides_read")
async def get_presentation(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL: https://docs.google.com/presentation/d/{presentation_id}/edit. Use the FULL ID."),
) -> str:
    """
    Get presentation metadata and slide list with content summaries.

    Returns slide-level data (slide_id, title, body, element_count) but NOT individual
    element IDs. Use get_page(slide_id=...) to get element IDs for a specific slide.

    Returns:
        JSON with presentation metadata and slides array.
    """
    logger.info(f"[get_presentation] Invoked. Email: '{user_google_email}', ID: '{presentation_id}'")

    result = await asyncio.to_thread(
        service.presentations().get(presentationId=presentation_id).execute
    )

    slides = result.get('slides', [])
    page_size = result.get('pageSize', {})
    width = page_size.get('width', {})
    height = page_size.get('height', {})

    mapped_slides = [_map_slide(s, i) for i, s in enumerate(slides, 1)]

    logger.info(f"Presentation retrieved successfully for {user_google_email}")
    return success_response({
        "presentation_id": presentation_id,
        "title": result.get('title'),
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
        "slide_count": len(slides),
        "page_size": {
            "width": width.get('magnitude'),
            "height": height.get('magnitude'),
            "unit": width.get('unit'),
        } if width else None,
        "slides": mapped_slides,
    })


@server.tool()
@handle_http_errors("batch_update_presentation", service_type="slides")
@require_google_service("slides", "slides")
async def batch_update_presentation(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    requests: List[Dict[str, Any]] = Field(..., description="List of Google Slides API batchUpdate request objects. Each dict must have exactly ONE request type key. Colors: rgbColor with float 0.0-1.0 (NOT hex). Table ranges need location + rowSpan + columnSpan. See docstring for all supported request types with copy-paste examples. PREFER high-level tools when available."),
) -> str:
    """
    Apply raw batch updates to a Google Slides presentation for operations
    not covered by high-level tools (table styling, bullets, line/image properties, etc.).

    FORMAT RULES:
    - Colors: rgbColor with FLOAT 0.0-1.0 (NOT hex, NOT 0-255)
      Convert hex "#1A73E8" → {"red": 0.102, "green": 0.451, "blue": 0.910}
    - "fields" mask: dot-notation path of properties to update
    - Table ranges: {"location": {"rowIndex": 0, "columnIndex": 0}, "rowSpan": 1, "columnSpan": 1}
    - Each request dict must have exactly ONE operation key
    - All indices are 0-based

    ⚠️ COMMON MISTAKES — DO NOT make these errors:
    1. WRONG: Combining two operations in one dict (e.g. {"updateTableCellProperties": {...}, "updateTextStyle": {...}})
       RIGHT: Each operation must be a SEPARATE dict in the requests list
    2. WRONG: "fields" inside "tableCellProperties" (e.g. {"tableCellProperties": {"fields": "..."}})
       RIGHT: "fields" goes at the "updateTableCellProperties" level, as a SIBLING of "tableCellProperties"
    3. WRONG: "contentAlignment" inside "tableCellBackgroundFill"
       RIGHT: "contentAlignment" goes at the "tableCellProperties" level, as a SIBLING of "tableCellBackgroundFill"
    4. WRONG: Using hex colors like "#FF0000"
       RIGHT: Use float rgbColor like {"red": 1.0, "green": 0, "blue": 0}
    5. WRONG: "pageObjectId" in updatePageProperties
       RIGHT: "objectId" (same as all other request types)
    6. WRONG: "properties" in updatePageProperties
       RIGHT: "pageProperties" (the full field name)

    ═══ TABLE CELL STYLING ═══

    # Cell background color (single cell)
    {"updateTableCellProperties": {
        "objectId": "<table_id>",
        "tableRange": {"location": {"rowIndex": 0, "columnIndex": 0}, "rowSpan": 1, "columnSpan": 1},
        "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": {"red": 0.2, "green": 0.4, "blue": 0.8}}}}},
        "fields": "tableCellBackgroundFill.solidFill.color"
    }}

    # Cell background — entire header row (span all columns)
    {"updateTableCellProperties": {
        "objectId": "<table_id>",
        "tableRange": {"location": {"rowIndex": 0, "columnIndex": 0}, "rowSpan": 1, "columnSpan": 3},
        "tableCellProperties": {"tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": {"red": 0.15, "green": 0.15, "blue": 0.15}}}}},
        "fields": "tableCellBackgroundFill.solidFill.color"
    }}

    # Cell content alignment (vertical): TOP, MIDDLE, BOTTOM
    {"updateTableCellProperties": {
        "objectId": "<table_id>",
        "tableRange": {"location": {"rowIndex": 0, "columnIndex": 0}, "rowSpan": 1, "columnSpan": 1},
        "tableCellProperties": {"contentAlignment": "MIDDLE"},
        "fields": "contentAlignment"
    }}

    # Cell border (borderPosition: TOP, BOTTOM, LEFT, RIGHT, INNER_HORIZONTAL, INNER_VERTICAL)
    {"updateTableBorderProperties": {
        "objectId": "<table_id>",
        "tableRange": {"location": {"rowIndex": 0, "columnIndex": 0}, "rowSpan": 1, "columnSpan": 3},
        "borderPosition": "BOTTOM",
        "tableBorderProperties": {
            "tableBorderFill": {"solidFill": {"color": {"rgbColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}}},
            "weight": {"magnitude": 1, "unit": "PT"}
        },
        "fields": "tableBorderFill,weight"
    }}

    # Column width (EMU: 1pt = 12700 EMU)
    {"updateTableColumnProperties": {
        "objectId": "<table_id>",
        "columnIndices": [0],
        "tableColumnProperties": {"columnWidth": {"magnitude": 2000000, "unit": "EMU"}},
        "fields": "columnWidth"
    }}

    # Row height
    {"updateTableRowProperties": {
        "objectId": "<table_id>",
        "rowIndices": [0],
        "tableRowProperties": {"minRowHeight": {"magnitude": 500000, "unit": "EMU"}},
        "fields": "minRowHeight"
    }}

    ═══ TABLE STRUCTURE ═══

    # Merge cells (span row 0 across 3 columns)
    {"mergeTableCells": {
        "objectId": "<table_id>",
        "tableRange": {"location": {"rowIndex": 0, "columnIndex": 0}, "rowSpan": 1, "columnSpan": 3}
    }}

    # Unmerge cells
    {"unmergeTableCells": {
        "objectId": "<table_id>",
        "tableRange": {"location": {"rowIndex": 0, "columnIndex": 0}, "rowSpan": 1, "columnSpan": 3}
    }}

    # Insert 2 rows below row 1
    {"insertTableRows": {
        "tableObjectId": "<table_id>",
        "cellLocation": {"rowIndex": 1},
        "insertBelow": true,
        "number": 2
    }}

    # Delete row 2
    {"deleteTableRow": {
        "tableObjectId": "<table_id>",
        "cellLocation": {"rowIndex": 2}
    }}

    # Insert 1 column right of column 0
    {"insertTableColumns": {
        "tableObjectId": "<table_id>",
        "cellLocation": {"columnIndex": 0},
        "insertRight": true,
        "number": 1
    }}

    # Delete column 2
    {"deleteTableColumn": {
        "tableObjectId": "<table_id>",
        "cellLocation": {"columnIndex": 2}
    }}

    ═══ TABLE CELL TEXT STYLING ═══

    # Bold white text in a table cell (e.g., header row)
    {"updateTextStyle": {
        "objectId": "<table_id>",
        "cellLocation": {"rowIndex": 0, "columnIndex": 0},
        "textRange": {"type": "ALL"},
        "style": {"bold": true, "foregroundColor": {"opaqueColor": {"rgbColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}}},
        "fields": "bold,foregroundColor"
    }}

    # Font size and family in table cell
    {"updateTextStyle": {
        "objectId": "<table_id>",
        "cellLocation": {"rowIndex": 1, "columnIndex": 2},
        "textRange": {"type": "ALL"},
        "style": {"fontSize": {"magnitude": 14, "unit": "PT"}, "fontFamily": "Roboto"},
        "fields": "fontSize,fontFamily"
    }}

    # Center-align text in a table cell
    {"updateParagraphStyle": {
        "objectId": "<table_id>",
        "cellLocation": {"rowIndex": 0, "columnIndex": 0},
        "textRange": {"type": "ALL"},
        "style": {"alignment": "CENTER"},
        "fields": "alignment"
    }}

    ═══ TABLE CELL TEXT (replace existing) ═══

    # Step 1: Delete existing text, Step 2: Insert new text (send both in one requests list)
    {"deleteText": {"objectId": "<table_id>", "cellLocation": {"rowIndex": 1, "columnIndex": 0}, "textRange": {"type": "ALL"}}}
    {"insertText": {"objectId": "<table_id>", "cellLocation": {"rowIndex": 1, "columnIndex": 0}, "text": "New text", "insertionIndex": 0}}

    ═══ BULLETS ═══

    # Add bullets to text (presets: BULLET_DISC_CIRCLE_SQUARE, BULLET_ARROW_DIAMOND_DISC, NUMBERED_DIGIT_ALPHA_ROMAN, etc.)
    {"createParagraphBullets": {
        "objectId": "<element_id>",
        "textRange": {"type": "ALL"},
        "bulletPreset": "BULLET_DISC_CIRCLE_SQUARE"
    }}

    # Remove bullets
    {"deleteParagraphBullets": {
        "objectId": "<element_id>",
        "textRange": {"type": "ALL"}
    }}

    ═══ IMAGE / VIDEO / LINE PROPERTIES ═══

    # Image transparency and crop
    {"updateImageProperties": {
        "objectId": "<image_id>",
        "imageProperties": {"transparency": 0.5, "cropProperties": {"leftOffset": 0.1, "topOffset": 0.1, "rightOffset": 0.1, "bottomOffset": 0.1}},
        "fields": "transparency,cropProperties"
    }}

    # Line color, weight, and dash style (dashStyle: SOLID, DOT, DASH, DASH_DOT, LONG_DASH, LONG_DASH_DOT)
    {"updateLineProperties": {
        "objectId": "<line_id>",
        "lineProperties": {
            "lineFill": {"solidFill": {"color": {"rgbColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}}},
            "weight": {"magnitude": 2, "unit": "PT"},
            "dashStyle": "DASH"
        },
        "fields": "lineFill.solidFill.color,weight,dashStyle"
    }}

    # Line with arrow (arrowStyle: NONE, STEALTH_ARROW, FILL_ARROW, FILL_CIRCLE, FILL_SQUARE, FILL_DIAMOND, OPEN_ARROW, OPEN_CIRCLE, OPEN_SQUARE, OPEN_DIAMOND)
    {"updateLineProperties": {
        "objectId": "<line_id>",
        "lineProperties": {"startArrow": "NONE", "endArrow": "OPEN_ARROW"},
        "fields": "startArrow,endArrow"
    }}

    ═══ REPLACE & EMBED ═══

    # Replace all shapes containing {{logo}} with an image
    {"replaceAllShapesWithImage": {
        "imageUrl": "https://example.com/logo.png",
        "imageReplaceMethod": "CENTER_INSIDE",
        "containsText": {"text": "{{logo}}", "matchCase": true}
    }}

    # Replace an existing image source
    {"replaceImage": {
        "imageObjectId": "<image_id>",
        "imageReplaceMethod": "CENTER_INSIDE",
        "url": "https://example.com/new-image.png"
    }}

    # Embed a Google Sheets chart
    {"createSheetsChart": {
        "spreadsheetId": "<spreadsheet_id>",
        "chartId": 12345,
        "linkingMode": "LINKED",
        "elementProperties": {
            "pageObjectId": "<slide_id>",
            "size": {"width": {"magnitude": 5000000, "unit": "EMU"}, "height": {"magnitude": 3000000, "unit": "EMU"}},
            "transform": {"scaleX": 1, "scaleY": 1, "translateX": 500000, "translateY": 500000, "unit": "EMU"}
        }
    }}

    ═══ CHARTS & VIDEO ═══

    # Refresh a linked Sheets chart to pull latest data
    {"refreshSheetsChart": {"objectId": "<chart_id>"}}

    # Replace all shapes containing {{chart}} with a Sheets chart
    {"replaceAllShapesWithSheetsChart": {
        "spreadsheetId": "<spreadsheet_id>",
        "chartId": 12345,
        "linkingMode": "LINKED",
        "containsText": {"text": "{{chart}}", "matchCase": true}
    }}

    # Video autoplay and timing (seconds)
    {"updateVideoProperties": {
        "objectId": "<video_id>",
        "videoProperties": {"autoPlay": true, "start": 10, "end": 60},
        "fields": "autoPlay,start,end"
    }}

    ═══ PARAGRAPH STYLE ═══

    # Line spacing (percentage, 100=single) and paragraph margins
    {"updateParagraphStyle": {
        "objectId": "<element_id>",
        "textRange": {"type": "ALL"},
        "style": {"lineSpacing": 150, "spaceAbove": {"magnitude": 10, "unit": "PT"}, "spaceBelow": {"magnitude": 5, "unit": "PT"}},
        "fields": "lineSpacing,spaceAbove,spaceBelow"
    }}

    ═══ ACCESSIBILITY ═══

    # Set alt text for screen readers
    {"updatePageElementAltText": {
        "objectId": "<element_id>",
        "title": "Chart title",
        "description": "Bar chart showing Q1 revenue by region"
    }}

    ═══ PAGE PROPERTIES ═══

    # Set slide background color (use set_slide_background tool instead for single slides)
    {"updatePageProperties": {
        "objectId": "<slide_id>",
        "pageProperties": {"pageBackgroundFill": {"solidFill": {"color": {"rgbColor": {"red": 0.06, "green": 0.09, "blue": 0.16}}}}},
        "fields": "pageBackgroundFill.solidFill.color"
    }}

    Returns:
        JSON with requests_applied count and reply details.
    """
    logger.info(f"[batch_update_presentation] Invoked. Email: '{user_google_email}', ID: '{presentation_id}', Requests: {len(requests)}")

    # Basic validation
    if not requests:
        raise ToolError("requests list is empty — nothing to update")
    for i, req in enumerate(requests):
        if not isinstance(req, dict) or len(req) == 0:
            raise ToolError(f"Request at index {i} must be a non-empty dict with one request type key (e.g. 'updateTableCellProperties')")

    result = await _batch_update(service, presentation_id, requests)

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
        "presentation_id": presentation_id,
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
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to inspect. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id), get_presentation (slides[].slide_id), or add_slide (slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
) -> str:
    """
    Get full details about a specific slide: elements, text, formatting, positions, and styles.

    Returns:
        JSON with slide metadata, elements array (id, type, position, text, formatting),
        background, and content (title, body, subtitle, speaker_notes).
    """
    logger.info(f"[get_page] Invoked. Email: '{user_google_email}', Presentation: '{presentation_id}', Slide: '{slide_id}'")

    result = await asyncio.to_thread(
        service.presentations().pages().get(
            presentationId=presentation_id,
            pageObjectId=slide_id
        ).execute
    )

    page_elements = result.get('pageElements', [])

    # Extract text content from placeholders
    content = {"title": None, "body": None, "subtitle": None, "speaker_notes": None}
    for elem in page_elements:
        shape = elem.get('shape', {})
        placeholder = shape.get('placeholder', {})
        p_type = placeholder.get('type')
        text = _extract_text(shape.get('text'))
        if p_type in ("TITLE", "CENTERED_TITLE"):
            content["title"] = text
        elif p_type == "BODY":
            content["body"] = text
        elif p_type == "SUBTITLE":
            content["subtitle"] = text

    # Speaker notes
    notes_page = result.get("slideProperties", {}).get("notesPage", {})
    for elem in notes_page.get("pageElements", []):
        shape = elem.get("shape", {})
        placeholder = shape.get("placeholder", {})
        if placeholder.get("type") == "BODY":
            content["speaker_notes"] = _extract_text(shape.get("text"))
            break

    # Extract background
    bg = result.get("pageProperties", {}).get("pageBackgroundFill", {})
    background = {}
    bg_rgb = bg.get("solidFill", {}).get("color", {}).get("rgbColor")
    if bg_rgb:
        background["color"] = _rgb_to_hex(bg_rgb)
    bg_image = bg.get("stretchedPictureFill", {}).get("contentUrl")
    if bg_image:
        background["image_url"] = bg_image

    logger.info(f"Page retrieved successfully for {user_google_email}")
    return success_response({
        "presentation_id": presentation_id,
        "slide_id": slide_id,
        "page_type": result.get('pageType'),
        "element_count": len(page_elements),
        "elements": [_map_page_element(e) for e in page_elements],
        "background": background,
        **content,
    })


@server.tool()
@handle_http_errors("get_page_thumbnail", is_read_only=True, service_type="slides")
@require_google_service("slides", "slides_read")
async def get_page_thumbnail(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to get thumbnail for. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id), get_presentation (slides[].slide_id), or add_slide (slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
    thumbnail_size: Literal["SMALL", "MEDIUM", "LARGE"] = Field("SMALL", description="""Thumbnail resolution. The image is returned inline — larger sizes consume significantly more tokens.
  'SMALL'  (default) — ~220px wide, ~15-25KB, low token cost. Best for routine inspect→change→verify steps.
  'MEDIUM' — ~800px wide, ~80-150KB, moderate token cost. Use when you need to read small text or check fine details.
  'LARGE'  — ~1600px wide, ~350-800KB, high token cost. Only use when you need pixel-level precision.
  PREFER SMALL to minimize token usage. Only upgrade if SMALL is insufficient."""),
):
    """
    Generate a thumbnail image for a specific slide, returned inline for visual inspection.

    Returns:
        JSON metadata with thumbnail_url, plus the thumbnail image rendered inline.
    """
    logger.info(f"[get_page_thumbnail] Invoked. Email: '{user_google_email}', Presentation: '{presentation_id}', Slide: '{slide_id}', Size: '{thumbnail_size}'")

    result = await asyncio.to_thread(
        service.presentations().pages().getThumbnail(
            presentationId=presentation_id,
            pageObjectId=slide_id,
            thumbnailProperties_thumbnailSize=thumbnail_size,
            thumbnailProperties_mimeType='PNG'
        ).execute
    )

    content_url = result.get('contentUrl')

    # Fetch the actual image bytes for inline display
    image_data = None
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(content_url)
            if resp.status_code == 200:
                image_data = resp.content
    except Exception:
        logger.warning(f"Failed to fetch thumbnail image for slide {slide_id}, returning URL only")

    json_response = success_response({
        "presentation_id": presentation_id,
        "slide_id": slide_id,
        "size": thumbnail_size,
        "thumbnail_url": content_url,
    })

    logger.info(f"Thumbnail generated successfully for {user_google_email}")

    if image_data:
        return [
            TextContent(type="text", text=json_response),
            ImageContent(
                type="image",
                data=base64.b64encode(image_data).decode(),
                mimeType="image/png",
            ),
        ]
    return json_response


# ---------------------------------------------------------------------------
# A. Slide Content Tools
# ---------------------------------------------------------------------------

@server.tool()
@handle_http_errors("add_slide", service_type="slides")
@require_google_service("slides", "slides")
async def add_slide(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    title: str = Field(None, description="Title text for the slide. PLAIN TEXT ONLY — no markdown, no **, no #. Optional for BLANK and CAPTION_ONLY layouts."),
    body: str = Field(None, description="""Body text for the slide. PLAIN TEXT ONLY — do NOT use markdown.

Use \\n for new lines. Use \\t at line start for indent levels (with bullets=True).
Do NOT include bullet characters — set bullets=True instead.

CORRECT examples:
  "Revenue grew 34%\\nNew clients: 12\\nNPS: 72"  (with bullets=True for bullet list)
  "Main point\\n\\tSub-point A\\n\\tSub-point B"  (with bullets=True for nested bullets)
  "Line one\\nLine two\\nLine three"  (without bullets for plain paragraphs)

WRONG examples — DO NOT do this:
  "- Revenue grew 34%\\n- New clients: 12"  (literal dashes shown on slide)
  "**Revenue** grew 34%"  (literal asterisks shown on slide)
  "• Point 1\\n• Point 2"  (literal bullet chars — use bullets=True instead)

Not available for TITLE_ONLY and MAIN_POINT layouts."""),
    speaker_notes: str = Field(None, description="Speaker notes shown in presenter view. PLAIN TEXT ONLY. Use \\n for new lines. For talking points, timing cues, or additional context not shown on the slide."),
    layout: Literal[
        "TITLE_AND_BODY", "TITLE", "SECTION_HEADER", "TITLE_ONLY",
        "TITLE_AND_TWO_COLUMNS", "ONE_COLUMN_TEXT", "MAIN_POINT",
        "BIG_NUMBER", "CAPTION_ONLY", "BLANK",
    ] = Field("TITLE_AND_BODY", description="""Slide layout:
  "TITLE_AND_BODY"        -> Standard content slide with title + body text/bullets (DEFAULT)
  "TITLE"                 -> Cover/opening slide (centered title + subtitle)
  "SECTION_HEADER"        -> Section divider between topics (title + subtitle)
  "TITLE_ONLY"            -> Title with no body — add images/shapes/tables separately
  "TITLE_AND_TWO_COLUMNS" -> Title + two-column body
  "ONE_COLUMN_TEXT"        -> Title + single text column
  "MAIN_POINT"            -> Single large centered statement (title only)
  "BIG_NUMBER"            -> KPI/stat highlight (title=the number, body=the label)
  "CAPTION_ONLY"          -> Body text only — for quotes, footnotes, attributions
  "BLANK"                 -> Empty slide — add all elements manually"""),
    insertion_index: int = Field(None, description="Position to insert the slide (0-based). If None, appends to the end."),
    bullets: bool = Field(False, description="If True, auto-formats body text as a bullet list. Use \\n to separate items and \\t for nested levels. Do NOT manually add bullet characters when this is True."),
) -> str:
    """
    Create a new slide with title, body content, speaker notes, and optional bullet formatting.

    Returns:
        JSON with slide_id, placeholder_ids, layout, and presentation link.
    """
    logger.info(f"[add_slide] Invoked. Email: '{user_google_email}', Layout: '{layout}'")

    valid_layouts = list(LAYOUT_PLACEHOLDERS.keys())
    if layout not in valid_layouts:
        raise ToolError(f"Invalid layout '{layout}'. Valid options: {', '.join(valid_layouts)}")

    info = await _add_single_slide(
        service, presentation_id,
        title=title, body=body, speaker_notes=speaker_notes,
        layout=layout, insertion_index=insertion_index, bullets=bullets,
    )

    logger.info(f"Slide added successfully for {user_google_email}")
    return success_response({
        **info,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("update_slide_content", service_type="slides")
@require_google_service("slides", "slides")
async def update_slide_content(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to update. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id), get_presentation (slides[].slide_id), or add_slide (slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
    title: str = Field(None, description="New title text. PLAIN TEXT ONLY — no markdown (**, *, #, -, •). Markdown renders as literal characters on slides. Replaces existing title. Pass None to leave unchanged. WRONG: '**Bold Title**' shows literal asterisks. For bold/color/font styling, use format_slide_text after updating text."),
    body: str = Field(None, description="New body text. PLAIN TEXT ONLY — no markdown (**, *, #, -, •). Markdown renders as literal characters on slides. Use \\n for new lines, \\t for indent levels. Replaces existing body. Pass None to leave unchanged. WRONG: '**Bold**' shows literal asterisks. '- Item' shows literal dashes. '# Heading' shows literal hash. For bullet lists, set bullets=True. For bold/color/font styling, use format_slide_text after updating text."),
    speaker_notes: str = Field(None, description="New speaker notes. PLAIN TEXT ONLY — no markdown (**, *, #, -, •). Markdown renders as literal characters. Use \\n for new lines. Replaces existing notes. Pass None to leave unchanged."),
    bullets: bool = Field(False, description="If True, auto-formats body text as bullet list after replacing."),
) -> str:
    """
    Replace text content on an existing slide (title, body, and/or speaker notes).
    Pass None for any field to leave it unchanged.

    Returns:
        JSON with slide_id, updated_fields list, and link.
    """
    logger.info(f"[update_slide_content] Invoked. Email: '{user_google_email}', Slide: '{slide_id}'")

    title = _clean_text(title)
    body = _clean_text(body, strip_bullets=bullets)
    speaker_notes = _clean_text(speaker_notes)

    # Get current slide to find placeholders
    page = await asyncio.to_thread(
        service.presentations().pages().get(
            presentationId=presentation_id,
            pageObjectId=slide_id
        ).execute
    )

    placeholders = _find_placeholders(page.get('pageElements', []))
    updated_fields = []
    requests = []

    # Map placeholder types to our parameters
    field_mapping = {
        "title": ["TITLE", "CENTERED_TITLE"],
        "body": ["BODY", "SUBTITLE"],
    }

    for field_name, placeholder_types in field_mapping.items():
        new_text = title if field_name == "title" else body
        if new_text is None:
            continue

        element_id = None
        for p_type in placeholder_types:
            if p_type in placeholders:
                element_id = placeholders[p_type]
                break

        if not element_id:
            continue

        # Delete existing text only if element has content (empty → skip to avoid API error)
        if _has_text(page.get('pageElements', []), element_id):
            requests.append({
                "deleteText": {
                    "objectId": element_id,
                    "textRange": {"type": "ALL"},
                }
            })
        requests.append({
            "insertText": {
                "objectId": element_id,
                "text": new_text,
                "insertionIndex": 0,
            }
        })
        updated_fields.append(field_name)

        # Apply bullets to body
        if field_name == "body" and bullets:
            requests.append({
                "createParagraphBullets": {
                    "objectId": element_id,
                    "textRange": {"type": "ALL"},
                    "bulletPreset": "BULLET_DISC_CIRCLE_SQUARE",
                }
            })

    if requests:
        await _batch_update(service, presentation_id, requests)

    # Speaker notes
    if speaker_notes is not None:
        notes_shape_id = _find_notes_shape_id(page)
        if not notes_shape_id:
            notes_shape_id = await _get_speaker_notes_id(service, presentation_id, slide_id)
        if notes_shape_id:
            notes_requests = []
            if _notes_has_text(page, notes_shape_id):
                notes_requests.append({"deleteText": {"objectId": notes_shape_id, "textRange": {"type": "ALL"}}})
            notes_requests.append({"insertText": {"objectId": notes_shape_id, "text": speaker_notes, "insertionIndex": 0}})
            await _batch_update(service, presentation_id, notes_requests)
            updated_fields.append("speaker_notes")

    logger.info(f"Slide content updated successfully for {user_google_email}")
    return success_response({
        "slide_id": slide_id,
        "updated_fields": updated_fields,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("replace_all_text", service_type="slides")
@require_google_service("slides", "slides")
async def replace_all_text(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    find_text: str = Field(..., description="The text to search for across all slides."),
    replace_text: str = Field(..., description="The replacement text. PLAIN TEXT ONLY — no markdown (**, *, #, -, •). Markdown renders as literal characters on slides. Use \\n for line breaks."),
    match_case: bool = Field(True, description="If True (default), search is case-sensitive."),
) -> str:
    """
    Find and replace text across the entire presentation (all slides).

    Returns:
        JSON with occurrences_replaced count.
    """
    logger.info(f"[replace_all_text] Invoked. Email: '{user_google_email}', Find: '{find_text}'")

    result = await _batch_update(service, presentation_id, [{
        "replaceAllText": {
            "containsText": {"text": find_text, "matchCase": match_case},
            "replaceText": replace_text,
        }
    }])

    count = 0
    for reply in result.get('replies', []):
        if 'replaceAllText' in reply:
            count = reply['replaceAllText'].get('occurrencesChanged', 0)

    logger.info(f"Replace all text completed for {user_google_email}")
    return success_response({
        "presentation_id": presentation_id,
        "find_text": find_text,
        "replace_text": replace_text,
        "occurrences_replaced": count,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


# ---------------------------------------------------------------------------
# B. Slide Management Tools
# ---------------------------------------------------------------------------

@server.tool()
@handle_http_errors("delete_slide", service_type="slides")
@require_google_service("slides", "slides")
async def delete_slide(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to delete. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id) or get_presentation (slides[].slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
) -> str:
    """
    Delete a slide from the presentation.

    Returns:
        str: JSON with slide_id, deleted=true, and link.
    """
    logger.info(f"[delete_slide] Invoked. Email: '{user_google_email}', Slide: '{slide_id}'")

    await _batch_update(service, presentation_id, [{
        "deleteObject": {"objectId": slide_id}
    }])

    logger.info(f"Slide deleted successfully for {user_google_email}")
    return success_response({
        "slide_id": slide_id,
        "deleted": True,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("duplicate_slide", service_type="slides")
@require_google_service("slides", "slides")
async def duplicate_slide(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to duplicate. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id) or get_presentation (slides[].slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
) -> str:
    """
    Create an exact copy of a slide with all content and formatting preserved.

    Returns:
        JSON with original_slide_id, new_slide_id, and link.
    """
    logger.info(f"[duplicate_slide] Invoked. Email: '{user_google_email}', Slide: '{slide_id}'")

    result = await _batch_update(service, presentation_id, [{
        "duplicateObject": {"objectId": slide_id}
    }])

    new_id = None
    for reply in result.get('replies', []):
        if 'duplicateObject' in reply:
            new_id = reply['duplicateObject'].get('objectId')

    logger.info(f"Slide duplicated successfully for {user_google_email}")
    return success_response({
        "original_slide_id": slide_id,
        "new_slide_id": new_id,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("reorder_slides", service_type="slides")
@require_google_service("slides", "slides")
async def reorder_slides(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_ids: List[str] = Field(..., description="List of slide IDs to move. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual IDs from: create_presentation response (slides_created[].slide_id) or get_presentation response (slides[].slide_id)."),
    insertion_index: int = Field(..., description="The 0-based target position. 0=beginning of deck."),
) -> str:
    """
    Move one or more slides to a new position in the presentation.

    Returns:
        str: JSON with slide_ids, new_position, and link.
    """
    logger.info(f"[reorder_slides] Invoked. Email: '{user_google_email}', Slides: {slide_ids}")

    await _batch_update(service, presentation_id, [{
        "updateSlidesPosition": {
            "slideObjectIds": slide_ids,
            "insertionIndex": insertion_index,
        }
    }])

    logger.info(f"Slides reordered successfully for {user_google_email}")
    return success_response({
        "slide_ids": slide_ids,
        "new_position": insertion_index,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


# ---------------------------------------------------------------------------
# C. Visual Element Tools
# ---------------------------------------------------------------------------

@server.tool()
@handle_http_errors("add_slide_image", service_type="slides")
@require_google_service("slides", "slides")
async def add_slide_image(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to add the image to. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id), get_presentation (slides[].slide_id), or add_slide (slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
    image_url: str = Field(..., description="""Public HTTPS URL of a PNG, JPEG, or GIF image. Google's servers fetch this URL server-side.

SUPPORTED FORMATS: PNG, JPEG, GIF only. WebP, SVG, BMP, and TIFF are NOT supported and will fail.
REQUIREMENTS: The URL must be a direct link to an image file that is publicly accessible without authentication, redirects, or bot protection. Many CDNs (Medium, Miro, Notion) block Google's server-side fetches.

COMMON FAILURE: URLs containing 'format:webp' or ending in '.webp' will ALWAYS fail — Google Slides does not support WebP.

IF THIS TOOL FAILS, recover by:
  1. Try a different URL that serves PNG/JPEG/GIF format (many CDNs support format params, e.g. change 'format:webp' to 'format:png')
  2. Use create_drive_file(fileUrl=...) to download and re-host the image on Google Drive, then use the Drive sharing URL
  3. Ask the user to provide a direct PNG/JPEG/GIF URL"""),
    x: float = Field(100, description="X position in points from left edge. Standard slide is 720pt wide."),
    y: float = Field(100, description="Y position in points from top edge. Standard slide is 405pt tall."),
    width: float = Field(300, description="Width in points."),
    height: float = Field(200, description="Height in points."),
) -> str:
    """
    Insert an image from a URL onto a slide.

    Returns:
        JSON with element_id, slide_id, and link.
    """
    logger.info(f"[add_slide_image] Invoked. Email: '{user_google_email}', Slide: '{slide_id}'")

    element_id = f"img_{uuid.uuid4().hex[:24]}"
    try:
        await _batch_update(service, presentation_id, [{
            "createImage": {
                "objectId": element_id,
                "url": image_url,
                "elementProperties": _element_properties(slide_id, x, y, width, height),
            }
        }])
    except HttpError as e:
        error_msg = str(e)
        if "problem retrieving the image" in error_msg.lower() or "invalid requests[0].createimage" in error_msg.lower():
            # Detect likely WebP format from URL
            is_webp = "webp" in image_url.lower()
            format_hint = (
                " The URL serves WebP format which Google Slides does NOT support."
                if is_webp else ""
            )
            raise ToolError(
                f"Google's servers could not fetch or process this image URL.{format_hint} "
                f"Supported formats: PNG, JPEG, GIF only (NOT WebP, SVG, BMP). "
                f"To fix: (1) Try changing the URL to serve PNG/JPEG format "
                f"(e.g. replace 'format:webp' with 'format:png' in the URL), "
                f"(2) Use create_drive_file(fileUrl=..., file_name='image.png') to re-host "
                f"the image on Google Drive and use the Drive sharing URL, or "
                f"(3) Ask the user for a direct PNG/JPEG/GIF URL. "
                f"Original URL: {image_url}"
            )
        raise  # Re-raise non-image-fetch errors for @handle_http_errors

    logger.info(f"Image added successfully for {user_google_email}")
    return success_response({
        "element_id": element_id,
        "slide_id": slide_id,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("add_slide_shape", service_type="slides")
@require_google_service("slides", "slides")
async def add_slide_shape(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to add the shape to. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id), get_presentation (slides[].slide_id), or add_slide (slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
    shape_type: Literal[
        "TEXT_BOX", "RECTANGLE", "ROUND_RECTANGLE", "ELLIPSE",
        "TRIANGLE", "DIAMOND", "HEXAGON", "CHEVRON",
        "RIGHT_ARROW", "LEFT_ARROW", "UP_ARROW", "DOWN_ARROW",
        "STAR_5", "STAR_10", "HEART", "CLOUD",
    ] = Field("TEXT_BOX", description="""MUST be one of these EXACT values — do NOT invent or modify names:

  TEXT_BOX        -> labels, callouts, footnotes (DEFAULT)
  RECTANGLE       -> cards, badges, color blocks
  ROUND_RECTANGLE -> buttons, tags, modern cards
  ELLIPSE         -> KPI circles, avatars
  TRIANGLE        -> warning indicators
  DIAMOND         -> decision nodes in flowcharts
  HEXAGON         -> tech/API labels, honeycomb
  CHEVRON         -> process flow steps
  RIGHT_ARROW     -> flow direction, next step
  LEFT_ARROW      -> back/previous direction
  UP_ARROW        -> growth indicator
  DOWN_ARROW      -> decline indicator, funnel
  STAR_5          -> ratings, highlights
  STAR_10         -> decorative star
  HEART           -> customer love metrics
  CLOUD           -> cloud architecture diagrams"""),
    text: str = Field(None, description="Text inside the shape. PLAIN TEXT ONLY — no markdown (**, *, #, -, •). Markdown renders as literal characters on shapes. Use \\n for multiple lines. Example: 'Total Revenue\\n$4.2M'. WRONG: '**Bold**' shows literal asterisks. After creation, use format_slide_text to style (bold, color, size) and update_shape_properties for fill/outline."),
    x: float = Field(100, description="X position in points from left edge (0-720)."),
    y: float = Field(100, description="Y position in points from top edge (0-405)."),
    width: float = Field(200, description="Width in points."),
    height: float = Field(100, description="Height in points."),
) -> str:
    """
    Add a shape or text box to a slide.

    Returns:
        JSON with element_id, shape_type, slide_id, and link.
    """
    logger.info(f"[add_slide_shape] Invoked. Email: '{user_google_email}', Shape: '{shape_type}'")

    text = _clean_text(text)
    element_id = f"shape_{uuid.uuid4().hex[:24]}"
    requests = [{
        "createShape": {
            "objectId": element_id,
            "shapeType": shape_type,
            "elementProperties": _element_properties(slide_id, x, y, width, height),
        }
    }]

    if text:
        requests.append({
            "insertText": {
                "objectId": element_id,
                "text": text,
                "insertionIndex": 0,
            }
        })

    await _batch_update(service, presentation_id, requests)

    logger.info(f"Shape added successfully for {user_google_email}")
    return success_response({
        "element_id": element_id,
        "shape_type": shape_type,
        "slide_id": slide_id,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("add_slide_line", service_type="slides")
@require_google_service("slides", "slides")
async def add_slide_line(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to add the line to. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id), get_presentation (slides[].slide_id), or add_slide (slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
    line_category: Literal["STRAIGHT", "BENT", "CURVED"] = Field("STRAIGHT", description="Line type: 'STRAIGHT' (default), 'BENT' (right-angle connector), 'CURVED' (curved connector)."),
    x: float = Field(100, description="Start X position in points (standard slide: 720pt wide)."),
    y: float = Field(200, description="Start Y position in points (standard slide: 405pt tall)."),
    width: float = Field(500, description="Horizontal extent in points. For STRAIGHT lines: use 0 for vertical line. For BENT/CURVED connectors: MUST be > 0 (both width and height are required to define the bend path)."),
    height: float = Field(0, description="Vertical extent in points. For STRAIGHT lines: use 0 for horizontal line. For BENT/CURVED connectors: MUST be > 0 (both width and height are required to define the bend path)."),
) -> str:
    """
    Add a line or connector to a slide.

    Returns:
        JSON with element_id, line_category, slide_id, and link.
    """
    logger.info(f"[add_slide_line] Invoked. Email: '{user_google_email}', Category: '{line_category}'")

    # Auto-fix negative dimensions by adjusting start position
    if width < 0:
        x = x + width
        width = abs(width)
    if height < 0:
        y = y + height
        height = abs(height)

    # BENT/CURVED connectors require both width and height > 0
    if line_category in ("BENT", "CURVED"):
        if width == 0:
            width = max(1, height)
        if height == 0:
            height = max(1, width)

    element_id = f"line_{uuid.uuid4().hex[:24]}"
    await _batch_update(service, presentation_id, [{
        "createLine": {
            "objectId": element_id,
            "lineCategory": line_category,
            "elementProperties": _element_properties(slide_id, x, y, width, height),
        }
    }])

    logger.info(f"Line added successfully for {user_google_email}")
    return success_response({
        "element_id": element_id,
        "line_category": line_category,
        "slide_id": slide_id,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("add_slide_table", service_type="slides")
@require_google_service("slides", "slides")
async def add_slide_table(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to add the table to. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id), get_presentation (slides[].slide_id), or add_slide (slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
    rows: int = Field(..., description="Number of rows (including header row). Minimum 1."),
    columns: int = Field(..., description="Number of columns. Minimum 1."),
    data: Optional[List[List[str]]] = Field(None, description="""Optional 2D array of cell values. First row is typically the header.
Format: [["row1col1", "row1col2"], ["row2col1", "row2col2"], ...]
Each inner list is one row. All values must be strings.

EXAMPLE — Revenue table:
  [["Region", "Revenue", "Growth"],
   ["North America", "$2.1M", "+28%"],
   ["Europe", "$1.4M", "+41%"],
   ["Asia-Pacific", "$0.7M", "+63%"]]

EXAMPLE — Feature comparison:
  [["Feature", "Basic", "Pro", "Enterprise"],
   ["Storage", "10 GB", "100 GB", "Unlimited"],
   ["Users", "5", "50", "Unlimited"],
   ["Support", "Email", "Priority", "24/7 Dedicated"],
   ["Price", "$10/mo", "$50/mo", "Custom"]]

EXAMPLE — Timeline:
  [["Phase", "Timeline", "Status"],
   ["Research", "Jan-Feb", "Complete"],
   ["Development", "Mar-May", "In Progress"],
   ["Launch", "Jul", "Planned"]]

If None, creates an empty table to fill manually."""),
    x: float = Field(60, description="X position in points from left edge (standard slide: 720pt wide)."),
    y: float = Field(100, description="Y position in points from top edge (standard slide: 405pt tall)."),
    width: float = Field(600, description="Width in points. 600pt is a good default for a full-width table below a title."),
    height: float = Field(250, description="Height in points."),
) -> str:
    """
    Create a table on a slide with optional pre-filled data.

    Style table cells with batch_update_presentation (updateTableCellProperties, updateTextStyle with cellLocation).
    format_slide_text and update_shape_properties do NOT work on table cells.

    Returns:
        JSON with element_id, rows, columns, slide_id, and link.
    """
    logger.info(f"[add_slide_table] Invoked. Email: '{user_google_email}', {rows}x{columns}")

    element_id = f"tbl_{uuid.uuid4().hex[:24]}"
    requests = [{
        "createTable": {
            "objectId": element_id,
            "rows": rows,
            "columns": columns,
            "elementProperties": _element_properties(slide_id, x, y, width, height),
        }
    }]

    await _batch_update(service, presentation_id, requests)

    # Fill data if provided
    if data:
        fill_requests = []
        for row_idx, row_data in enumerate(data):
            for col_idx, cell_text in enumerate(row_data):
                if cell_text:
                    fill_requests.append({
                        "insertText": {
                            "objectId": element_id,
                            "cellLocation": {
                                "rowIndex": row_idx,
                                "columnIndex": col_idx,
                            },
                            "text": str(cell_text),
                            "insertionIndex": 0,
                        }
                    })
        if fill_requests:
            await _batch_update(service, presentation_id, fill_requests)

    logger.info(f"Table added successfully for {user_google_email}")
    return success_response({
        "element_id": element_id,
        "rows": rows,
        "columns": columns,
        "slide_id": slide_id,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("add_slide_video", service_type="slides")
@require_google_service("slides", "slides")
async def add_slide_video(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to add the video to. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id), get_presentation (slides[].slide_id), or add_slide (slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
    video_id: str = Field(..., description="YouTube video ID ONLY (not the full URL). Example: 'dQw4w9WgXcQ' from https://youtube.com/watch?v=dQw4w9WgXcQ"),
    x: float = Field(160, description="X position in points (standard slide: 720pt wide)."),
    y: float = Field(90, description="Y position in points (standard slide: 405pt tall)."),
    width: float = Field(400, description="Width in points. 400pt is good for centered 16:9 video."),
    height: float = Field(225, description="Height in points. 225pt matches 16:9 aspect ratio with 400pt width."),
) -> str:
    """
    Embed a YouTube video on a slide. Only YouTube videos are supported.

    Returns:
        JSON with element_id, video_id, slide_id, and link.
    """
    logger.info(f"[add_slide_video] Invoked. Email: '{user_google_email}', Video: '{video_id}'")

    # Auto-extract video ID from common YouTube URL formats
    if '/' in video_id or '=' in video_id:
        # https://www.youtube.com/watch?v=VIDEO_ID or &v=VIDEO_ID
        match = re.search(r'[?&]v=([a-zA-Z0-9_-]{11})', video_id)
        if not match:
            # https://youtu.be/VIDEO_ID
            match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})', video_id)
        if not match:
            # https://www.youtube.com/embed/VIDEO_ID
            match = re.search(r'embed/([a-zA-Z0-9_-]{11})', video_id)
        if match:
            video_id = match.group(1)
            logger.info(f"Extracted video ID from URL: {video_id}")

    element_id = f"vid_{uuid.uuid4().hex[:24]}"
    await _batch_update(service, presentation_id, [{
        "createVideo": {
            "objectId": element_id,
            "source": "YOUTUBE",
            "id": video_id,
            "elementProperties": _element_properties(slide_id, x, y, width, height),
        }
    }])

    logger.info(f"Video added successfully for {user_google_email}")
    return success_response({
        "element_id": element_id,
        "video_id": video_id,
        "slide_id": slide_id,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


# ---------------------------------------------------------------------------
# D. Styling & Formatting Tools
# ---------------------------------------------------------------------------

@server.tool()
@handle_http_errors("format_slide_text", service_type="slides")
@require_google_service("slides", "slides")
async def format_slide_text(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    element_id: str = Field(..., description="The element ID containing the text. Get from add_slide response (placeholder_ids.title or placeholder_ids.body), add_slide_shape response (element_id), or get_page response (elements[].id). For table cell text styling, use batch_update_presentation with updateTextStyle + cellLocation instead."),
    bold: bool = Field(None, description="Set True for bold, False to remove bold, None to leave unchanged."),
    italic: bool = Field(None, description="Set True for italic, False to remove italic, None to leave unchanged."),
    underline: bool = Field(None, description="Set True for underline, False to remove, None to leave unchanged."),
    font_size: int = Field(None, description="Font size in points. Common sizes: 12 (body), 18 (subtitle), 24 (heading), 36 (large title), 48 (hero number), 72 (big number)."),
    font_family: str = Field(None, description='Font family name (e.g. "Montserrat", "Georgia", "Arial", "Roboto Mono").'),
    color: str = Field(None, description='Text color as hex "#RRGGBB" (e.g. "#FFFFFF", "#000000", "#1A73E8").'),
    link_url: str = Field(None, description="URL to hyperlink the text to. Example: 'https://example.com'. Set to empty string '' to remove link."),
    alignment: Optional[Literal["START", "CENTER", "END", "JUSTIFIED"]] = Field(None, description="Paragraph alignment: 'START' (left), 'CENTER', 'END' (right), 'JUSTIFIED'."),
    start_index: int = Field(None, description="Start character index for partial formatting (0-based). To format ALL text, omit both start_index and end_index (do NOT pass 0, 0)."),
    end_index: int = Field(None, description="End character index for partial formatting (exclusive). Must be > start_index. To format ALL text, omit both indices. Example: to bold '$4.2M' at the start, use start_index=0, end_index=5. If end_index exceeds text length, it will be automatically clamped."),
) -> str:
    """
    Apply text formatting (bold, color, font, size, links, alignment) to text in any element.

    For table cell text, use batch_update_presentation with updateTextStyle + cellLocation instead.

    Returns:
        JSON with element_id, formatting_applied list, and link.
    """
    logger.info(f"[format_slide_text] Invoked. Email: '{user_google_email}', Element: '{element_id}'")

    requests = []
    formatting_applied = []

    # Text range
    if start_index is not None and end_index is not None and start_index < end_index:
        # Clamp end_index to actual text length to prevent API errors
        pres_data = await asyncio.to_thread(
            service.presentations().get(
                presentationId=presentation_id,
                fields="slides.pageElements.objectId,slides.pageElements.shape.text"
            ).execute
        )
        for slide in pres_data.get("slides", []):
            for elem in slide.get("pageElements", []):
                if elem.get("objectId") == element_id:
                    text_elements = elem.get("shape", {}).get("text", {}).get("textElements", [])
                    text_length = sum(len(te.get("textRun", {}).get("content", "")) for te in text_elements)
                    if text_length > 0 and end_index > text_length:
                        end_index = text_length
                    break

        text_range = {"type": "FIXED_RANGE", "startIndex": start_index, "endIndex": end_index}
    else:
        # Invalid range (e.g., 0,0) or indices omitted — format all text
        text_range = {"type": "ALL"}

    # Text style
    style = {}
    fields = []

    if bold is not None:
        style["bold"] = bold
        fields.append("bold")
    if italic is not None:
        style["italic"] = italic
        fields.append("italic")
    if underline is not None:
        style["underline"] = underline
        fields.append("underline")
    if font_size is not None:
        style["fontSize"] = {"magnitude": font_size, "unit": "PT"}
        fields.append("fontSize")
    if font_family is not None:
        style["fontFamily"] = font_family
        fields.append("fontFamily")
    if color is not None:
        style["foregroundColor"] = {"opaqueColor": {"rgbColor": _hex_to_rgb(color)}}
        fields.append("foregroundColor")
    if link_url is not None:
        if link_url:
            style["link"] = {"url": link_url}
        else:
            style["link"] = {}
        fields.append("link")

    if style:
        requests.append({
            "updateTextStyle": {
                "objectId": element_id,
                "textRange": text_range,
                "style": style,
                "fields": ",".join(fields),
            }
        })
        formatting_applied.extend(fields)

    # Paragraph alignment
    if alignment is not None:
        requests.append({
            "updateParagraphStyle": {
                "objectId": element_id,
                "textRange": text_range,
                "style": {"alignment": alignment},
                "fields": "alignment",
            }
        })
        formatting_applied.append("alignment")

    if requests:
        await _batch_update(service, presentation_id, requests)

    logger.info(f"Text formatted successfully for {user_google_email}")
    return success_response({
        "element_id": element_id,
        "formatting_applied": formatting_applied,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("set_slide_background", service_type="slides")
@require_google_service("slides", "slides")
async def set_slide_background(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to set the background for. NEVER guess — always get from API responses. Get from: create_presentation (slides_created[].slide_id), get_presentation (slides[].slide_id), or add_slide (slide_id). slide_id is NOT element_id — element_id starts with img_/shape_/line_/tbl_/vid_."),
    color: str = Field(None, description='Background color as hex "#RRGGBB". Provide color OR image_url, not both.'),
    image_url: str = Field(None, description="Public URL of background image. The image will be stretched to fill the slide. Provide image_url OR color, not both."),
) -> str:
    """
    Set the background of a slide to a solid color or an image.

    Returns:
        JSON with slide_id, background_type, and link.
    """
    logger.info(f"[set_slide_background] Invoked. Email: '{user_google_email}', Slide: '{slide_id}'")

    properties = {}
    bg_type = None

    if color:
        properties["pageBackgroundFill"] = {
            "solidFill": {"color": {"rgbColor": _hex_to_rgb(color)}}
        }
        bg_type = "solid_color"
    elif image_url:
        properties["pageBackgroundFill"] = {
            "stretchedPictureFill": {"contentUrl": image_url}
        }
        bg_type = "image"

    if properties:
        await _batch_update(service, presentation_id, [{
            "updatePageProperties": {
                "objectId": slide_id,
                "pageProperties": properties,
                "fields": "pageBackgroundFill",
            }
        }])

    logger.info(f"Background set successfully for {user_google_email}")
    return success_response({
        "slide_id": slide_id,
        "background_type": bg_type,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("update_shape_properties", service_type="slides")
@require_google_service("slides", "slides")
async def update_shape_properties(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    element_id: str = Field(..., description="The element ID of the shape. Get from add_slide_shape response (element_id) or get_page response (elements[].id)."),
    fill_color: str = Field(None, description='Fill color as hex "#RRGGBB".'),
    outline_color: str = Field(None, description='Outline/border color as hex "#RRGGBB".'),
    outline_weight: float = Field(None, description="Outline thickness in points. Common: 1 (thin), 2 (medium), 3 (thick)."),
) -> str:
    """
    Update visual properties of a shape (fill color, outline color, border weight).
    Works on shapes and text boxes ONLY — not table cells.

    Returns:
        JSON with element_id, properties_updated list, and link.
    """
    logger.info(f"[update_shape_properties] Invoked. Email: '{user_google_email}', Element: '{element_id}'")

    properties = {}
    field_list = []
    updated = []

    if fill_color is not None:
        properties["shapeBackgroundFill"] = {
            "solidFill": {"color": {"rgbColor": _hex_to_rgb(fill_color)}}
        }
        field_list.append("shapeBackgroundFill")
        updated.append("fill_color")

    if outline_color is not None or outline_weight is not None:
        outline = {}
        if outline_color is not None:
            outline["outlineFill"] = {
                "solidFill": {"color": {"rgbColor": _hex_to_rgb(outline_color)}}
            }
            field_list.append("outline.outlineFill")
            updated.append("outline_color")
        if outline_weight is not None:
            outline["weight"] = {"magnitude": outline_weight, "unit": "PT"}
            field_list.append("outline.weight")
            updated.append("outline_weight")
        properties["outline"] = outline

    if properties:
        await _batch_update(service, presentation_id, [{
            "updateShapeProperties": {
                "objectId": element_id,
                "shapeProperties": properties,
                "fields": ",".join(field_list),
            }
        }])

    logger.info(f"Shape properties updated for {user_google_email}")
    return success_response({
        "element_id": element_id,
        "properties_updated": updated,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


# ---------------------------------------------------------------------------
# E. Element Manipulation Tools
# ---------------------------------------------------------------------------

@server.tool()
@handle_http_errors("transform_element", service_type="slides")
@require_google_service("slides", "slides")
async def transform_element(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    element_id: str = Field(..., description="The element ID to transform. Get from add_slide_shape/image/line/table/video response (element_id) or get_page response (elements[].id)."),
    x: float = Field(None, description="New X position in points from left edge (0-720)."),
    y: float = Field(None, description="New Y position in points from top edge (0-405)."),
    width: float = Field(None, description="New width in points."),
    height: float = Field(None, description="New height in points."),
    rotation: float = Field(None, description="Rotation angle in degrees. Positive = counter-clockwise."),
) -> str:
    """
    Move, resize, or rotate any element on a slide.

    Returns:
        JSON with element_id, transforms_applied list, and link.
    """
    logger.info(f"[transform_element] Invoked. Email: '{user_google_email}', Element: '{element_id}'")

    transforms_applied = []

    # Fetch current element size and transform in one call
    elem_data = await _get_element_data(service, presentation_id, element_id)
    if not elem_data:
        raise ToolError(f"Element '{element_id}' not found in presentation")

    current_transform = elem_data["transform"]
    current_size = elem_data["size"]

    # Build a single ABSOLUTE transform that handles position, size, and rotation
    transform = {
        "unit": "EMU",
        "shearX": current_transform.get("shearX", 0),
        "shearY": current_transform.get("shearY", 0),
    }

    # Position
    if x is not None:
        transform["translateX"] = _pt_to_emu(x)
        transforms_applied.append("x")
    else:
        transform["translateX"] = current_transform.get("translateX", 0)

    if y is not None:
        transform["translateY"] = _pt_to_emu(y)
        transforms_applied.append("y")
    else:
        transform["translateY"] = current_transform.get("translateY", 0)

    # Size: compute scale ratio = desired_size / element_intrinsic_size
    orig_w = current_size.get("width", {}).get("magnitude", 1)
    orig_h = current_size.get("height", {}).get("magnitude", 1)

    # Extract existing rotation from current matrix to separate scale from rotation
    cur_scaleX = current_transform.get("scaleX", 1)
    cur_shearY = current_transform.get("shearY", 0)
    cur_scaleY = current_transform.get("scaleY", 1)
    cur_shearX = current_transform.get("shearX", 0)
    existing_angle = math.atan2(cur_shearY, cur_scaleX) if (cur_scaleX or cur_shearY) else 0
    # Pure scale = magnitude of the scale/shear vectors
    existing_sx = math.hypot(cur_scaleX, cur_shearY) if (cur_scaleX or cur_shearY) else 1
    existing_sy = math.hypot(cur_scaleY, cur_shearX) if (cur_scaleY or cur_shearX) else 1

    if width is not None:
        sx = _pt_to_emu(width) / orig_w if orig_w else 1
        transforms_applied.append("width")
    else:
        sx = existing_sx

    if height is not None:
        sy = _pt_to_emu(height) / orig_h if orig_h else 1
        transforms_applied.append("height")
    else:
        sy = existing_sy

    # Rotation — AffineTransform has no "rotation" field; encode via matrix:
    #   scaleX = sx*cos(θ), shearX = -sy*sin(θ)
    #   shearY = sx*sin(θ), scaleY = sy*cos(θ)
    if rotation is not None:
        angle = rotation * (math.pi / 180)  # degrees to radians
        transforms_applied.append("rotation")
    else:
        angle = existing_angle

    cos_a = math.cos(angle)
    sin_a = math.sin(angle)
    transform["scaleX"] = sx * cos_a
    transform["shearX"] = -sy * sin_a
    transform["shearY"] = sx * sin_a
    transform["scaleY"] = sy * cos_a

    if transforms_applied:
        await _batch_update(service, presentation_id, [{
            "updatePageElementTransform": {
                "objectId": element_id,
                "transform": transform,
                "applyMode": "ABSOLUTE",
            }
        }])

    logger.info(f"Element transformed for {user_google_email}")
    return success_response({
        "element_id": element_id,
        "transforms_applied": transforms_applied,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("group_elements", service_type="slides")
@require_google_service("slides", "slides")
async def group_elements(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    element_ids: List[str] = Field(..., description="List of at least 2 element IDs to group. Get from add_slide_shape/image/line/table/video response (element_id) or get_page response (elements[].id). All must be on the same slide."),
) -> str:
    """
    Group multiple elements so they move, resize, and rotate as one unit.

    Returns:
        JSON with group_id, children, and link.
    """
    logger.info(f"[group_elements] Invoked. Email: '{user_google_email}', Elements: {element_ids}")

    group_id = uuid.uuid4().hex
    await _batch_update(service, presentation_id, [{
        "groupObjects": {
            "groupObjectId": group_id,
            "childrenObjectIds": element_ids,
        }
    }])

    logger.info(f"Elements grouped for {user_google_email}")
    return success_response({
        "group_id": group_id,
        "children": element_ids,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("ungroup_elements", service_type="slides")
@require_google_service("slides", "slides")
async def ungroup_elements(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    group_id: str = Field(..., description="The group element ID to ungroup. Get from group_elements response (group_id) or get_page response (elements[].id)."),
) -> str:
    """
    Ungroup a previously grouped set of elements.

    Returns:
        JSON with group_id, ungrouped=true, and link.
    """
    logger.info(f"[ungroup_elements] Invoked. Email: '{user_google_email}', Group: '{group_id}'")

    await _batch_update(service, presentation_id, [{
        "ungroupObjects": {"objectIds": [group_id]}
    }])

    logger.info(f"Elements ungrouped for {user_google_email}")
    return success_response({
        "group_id": group_id,
        "ungrouped": True,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


# ---------------------------------------------------------------------------
# F. Advanced Element Operations
# ---------------------------------------------------------------------------

@server.tool()
@handle_http_errors("delete_element", service_type="slides")
@require_google_service("slides", "slides")
async def delete_element(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    element_id: str = Field(..., description="The element ID to delete. Get from add_slide_shape/image/line/table/video response (element_id), group_elements response (group_id), or get_page response (elements[].id)."),
) -> str:
    """
    Delete any element from a slide (shape, image, table, line, video, or group).

    Returns:
        JSON with element_id, deleted=true, and link.
    """
    logger.info(f"[delete_element] Invoked. Email: '{user_google_email}', Element: '{element_id}'")

    await _batch_update(service, presentation_id, [{
        "deleteObject": {"objectId": element_id}
    }])

    logger.info(f"Element deleted for {user_google_email}")
    return success_response({
        "element_id": element_id,
        "deleted": True,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


@server.tool()
@handle_http_errors("set_element_z_order", service_type="slides")
@require_google_service("slides", "slides")
async def set_element_z_order(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    element_id: str = Field(..., description="The element ID to reorder. Get from add_slide_shape/image/line/table/video response (element_id) or get_page response (elements[].id)."),
    operation: Literal["BRING_TO_FRONT", "BRING_FORWARD", "SEND_BACKWARD", "SEND_TO_BACK"] = Field("BRING_TO_FRONT", description="Z-order operation: 'BRING_TO_FRONT' (default, topmost), 'BRING_FORWARD' (one step up), 'SEND_BACKWARD' (one step down), 'SEND_TO_BACK' (bottommost)."),
) -> str:
    """
    Change the stacking order (z-order) of an element on a slide.

    Returns:
        str: JSON with element_id, operation, and link.
    """
    logger.info(f"[set_element_z_order] Invoked. Email: '{user_google_email}', Element: '{element_id}', Op: '{operation}'")

    await _batch_update(service, presentation_id, [{
        "updatePageElementsZOrder": {
            "pageElementObjectIds": [element_id],
            "operation": operation,
        }
    }])

    logger.info(f"Z-order updated for {user_google_email}")
    return success_response({
        "element_id": element_id,
        "operation": operation,
        "link": f"https://docs.google.com/presentation/d/{presentation_id}/edit",
    })


# ---------------------------------------------------------------------------
# Comment Tools
# ---------------------------------------------------------------------------

_comment_tools = create_comment_tools("presentation", "presentation_id")
read_presentation_comments = _comment_tools['read_comments']
create_presentation_comment = _comment_tools['create_comment']
reply_to_presentation_comment = _comment_tools['reply_to_comment']
resolve_presentation_comment = _comment_tools['resolve_comment']

# Aliases for intuitive naming
read_slide_comments = read_presentation_comments
create_slide_comment = create_presentation_comment
reply_to_slide_comment = reply_to_presentation_comment
resolve_slide_comment = resolve_presentation_comment
