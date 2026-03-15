"""
Google Slides MCP Tools

This module provides MCP tools for interacting with Google Slides API.

=== TEXT FORMATTING RULES (CRITICAL — READ BEFORE USING ANY TOOL) ===

ALL text fields (title, body, speaker_notes) use PLAIN TEXT only.
DO NOT use markdown syntax. Markdown will appear as literal characters on slides.

FORBIDDEN (will render as ugly literal characters on slides):
  - NO markdown: **, *, #, ##, ###, `, ```, -, [], ()
  - NO bullet characters: bullet, dash, or asterisk prefixes
  - NO HTML tags: <b>, <i>, <br>, <ul>, <li>

CORRECT text formatting:
  - New line:         Use \\n between lines
  - Bullet list:      Set bullets=True, separate items with \\n (bullets are auto-added)
  - Nested bullet:    Use \\t before the line text for indent levels
  - Sub-sub bullet:   Use \\t\\t for deeper nesting
  - Bold/italic:      Use format_slide_text AFTER creating the slide
  - Colors/fonts:     Use format_slide_text AFTER creating the slide

EXAMPLES of CORRECT body text:
  Plain lines:     "Line one\\nLine two\\nLine three"
  Bullet list:     "First point\\nSecond point\\nThird point"  (with bullets=True)
  Nested bullets:  "Main point\\n\\tSub-point A\\n\\tSub-point B\\nAnother main point"  (with bullets=True)
  Two-level nest:  "Topic\\n\\tDetail\\n\\t\\tSub-detail"  (with bullets=True)

EXAMPLES of WRONG body text (DO NOT DO THIS):
  "**Bold text**"           -> shows literal asterisks on slide
  "- First item\\n- Second"  -> shows literal dashes on slide
  "# Heading"               -> shows literal hash on slide
  "[link](url)"             -> shows literal brackets on slide

=== SLIDE DIMENSIONS ===
Standard 16:9 slide = 720pt wide x 405pt tall
Coordinate system: (0,0) = top-left corner

=== MANDATORY THUMBNAIL WORKFLOW FOR STYLING (CRITICAL) ===

EVERY styling or visual change MUST follow the inspect-change-verify cycle:
  1. BEFORE: call get_page_thumbnail(slide_id=...) to inspect current state
  2. APPLY the styling change
  3. AFTER: call get_page_thumbnail(slide_id=...) again to verify the result

This is NOT optional. Skipping thumbnails leads to blind styling — misaligned text,
clashing colors, or broken layouts. You MUST visually confirm every styling decision.

Tools that REQUIRE this workflow:
  - format_slide_text, set_slide_background, update_shape_properties
  - add_slide_shape, add_slide_image, add_slide_table, add_slide_line
  - transform_element, batch_update_presentation
"""

import html
import logging
import asyncio
import uuid
import math
import re
from typing import List, Dict, Any, Optional

import base64
import httpx
from pydantic import Field
from mcp.types import TextContent, ImageContent

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors
from core.response import success_response, error_response
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


def _pt_to_emu(pt: float) -> int:
    """Convert points to EMU (English Metric Units). 1 pt = 12700 EMU."""
    return int(pt * 12700)


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
    """Map a raw page element to a clean shape."""
    mapped = {"id": element.get("objectId")}
    if "shape" in element:
        mapped["type"] = "shape"
        mapped["shape_type"] = element["shape"].get("shapeType")
        placeholder = element["shape"].get("placeholder", {})
        if placeholder.get("type"):
            mapped["placeholder_type"] = placeholder["type"]
        text = _extract_text(element["shape"].get("text"))
        if text:
            mapped["text"] = text
    elif "table" in element:
        mapped["type"] = "table"
        mapped["rows"] = element["table"].get("rows")
        mapped["columns"] = element["table"].get("columns")
    elif "line" in element:
        mapped["type"] = "line"
        mapped["line_type"] = element["line"].get("lineType")
    elif "image" in element:
        mapped["type"] = "image"
    elif "video" in element:
        mapped["type"] = "video"
        mapped["video_source"] = element["video"].get("source")
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
    title: str = Field("Untitled Presentation", description="The title for the new presentation. This title is also auto-filled into the first slide's heading. PLAIN TEXT ONLY."),
    subtitle: str = Field(None, description="Optional subtitle for the auto-generated first (cover) slide. PLAIN TEXT ONLY. Example: 'Prepared by Strategy Team — March 2026'"),
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

    The first slide is automatically populated with the presentation title as heading and optional subtitle.
    Pass the 'slides' parameter to build an entire deck in one call.

    WORKFLOW FOR A PROFESSIONAL PRESENTATION:
    Option A (one call): create_presentation(title=..., subtitle=..., slides=[...]) builds the full deck.
    Option B (step by step):
      Step 1: create_presentation(title=..., subtitle=...) -> get presentation_id
      Step 2: add_slide(...) for each additional slide
      Step 3: add_slide_image/add_slide_table/add_slide_shape for visuals
      Step 4: format_slide_text for styling (bold, color, font)
      Step 5: set_slide_background for slide backgrounds
      Step 6: update_shape_properties for shape fills/outlines

    MANDATORY — THUMBNAIL VERIFICATION FOR STYLING:
    After creating the presentation, you MUST call get_page_thumbnail(slide_id=...)
    on each slide BEFORE applying any styling changes. Do NOT style blindly.
    Follow the inspect → change → verify cycle for every styling tool call.

    Returns:
        str: JSON with presentation id, title, link, slide_count, and slides_created details.
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
    Get details about a Google Slides presentation including slide list with content.

    USE THIS FOR:
    - Getting the list of slide IDs, titles, and content in a presentation
    - Finding a specific slide's ID before updating or reading its content
    - Checking how many slides exist

    VISUAL INSPECTION WORKFLOW (MANDATORY before styling):
    To visually inspect or improve slides, use this tool first to get slide IDs,
    then you MUST call get_page_thumbnail(slide_id=...) for each slide BEFORE making
    any styling changes. Work on one slide at a time: inspect → change → verify with thumbnail.

    Returns:
        str: JSON with presentation metadata and slides array.
             Each slide has: index, slide_id, title, body, subtitle, element_count.
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
    Apply raw batch updates to a Google Slides presentation.

    USE THIS FOR operations not covered by high-level tools:
    - Table cell styling (background, borders, padding, row height, column width)
    - Table structure (merge/unmerge cells, insert/delete rows/columns)
    - Update text in existing table cells
    - Bullet points (create/delete)
    - Image/video/line property updates
    - Replace shapes with images or Sheets charts
    - Accessibility alt text

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

    ═══ ACCESSIBILITY ═══

    # Set alt text for screen readers
    {"updatePageElementAltText": {
        "objectId": "<element_id>",
        "title": "Chart title",
        "description": "Bar chart showing Q1 revenue by region"
    }}

    MANDATORY VISUAL VERIFICATION:
    AFTER: You MUST call get_page_thumbnail(slide_id=...) on affected slides to verify.

    Returns:
        str: JSON with requests_applied count and reply details.
    """
    logger.info(f"[batch_update_presentation] Invoked. Email: '{user_google_email}', ID: '{presentation_id}', Requests: {len(requests)}")

    # Basic validation
    if not requests:
        return error_response(400, "requests list is empty — nothing to update")
    for i, req in enumerate(requests):
        if not isinstance(req, dict) or len(req) == 0:
            return error_response(400, f"Request at index {i} must be a non-empty dict with one request type key (e.g. 'updateTableCellProperties')")

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
    slide_id: str = Field(..., description="The slide ID to inspect. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id), get_presentation response (slides[].slide_id), or add_slide response (slide_id)."),
) -> str:
    """
    Get details about a specific slide: all element IDs, types, and text content.

    USE THIS FOR:
    - Finding element IDs on a slide (shapes, images, tables, etc.)
    - Reading text content (title, body, subtitle, speaker notes)
    - Understanding elements before modifying them
    - Getting placeholder IDs for format_slide_text

    VISUAL INSPECTION WORKFLOW (MANDATORY before styling):
    To visually inspect slides, use get_page_thumbnail(slide_id=...) BEFORE making
    any styling changes. Work on one slide at a time: inspect → change → verify.

    Returns:
        str: JSON with slide metadata, elements array, and text content.
             Elements: id, type, shape_type, placeholder_type, text, rows, columns, etc.
             Content: title, body, subtitle, speaker_notes (each null if empty).
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

    logger.info(f"Page retrieved successfully for {user_google_email}")
    return success_response({
        "presentation_id": presentation_id,
        "slide_id": slide_id,
        "page_type": result.get('pageType'),
        "element_count": len(page_elements),
        "elements": [_map_page_element(e) for e in page_elements],
        **content,
    })


@server.tool()
@handle_http_errors("get_page_thumbnail", is_read_only=True, service_type="slides")
@require_google_service("slides", "slides_read")
async def get_page_thumbnail(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    presentation_id: str = Field(..., description="The presentation ID. Get from create_presentation response (presentation_id) or from URL. Use the FULL ID."),
    slide_id: str = Field(..., description="The slide ID to get thumbnail for. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id), get_presentation response (slides[].slide_id), or add_slide response (slide_id)."),
    thumbnail_size: str = Field("SMALL", description="""Thumbnail resolution. The image is returned inline — larger sizes consume significantly more tokens.
  'SMALL'  (default) — ~220px wide, ~15-25KB, low token cost. Best for routine inspect→change→verify steps.
  'MEDIUM' — ~800px wide, ~80-150KB, moderate token cost. Use when you need to read small text or check fine details.
  'LARGE'  — ~1600px wide, ~350-800KB, high token cost. Only use when you need pixel-level precision.
  PREFER SMALL for the mandatory styling verification workflow. Only upgrade if SMALL is insufficient."""),
):
    """
    Generate a thumbnail for a specific slide and return it inline for visual inspection.

    USE THIS FOR:
    - Getting a visual preview of a slide
    - Verifying slide content visually before and after styling changes

    The thumbnail image is returned inline as an image alongside JSON metadata.
    Use SMALL size (default) for routine verification to minimize token usage.

    Returns:
        JSON metadata with thumbnail_url, plus the thumbnail image rendered inline
        for direct visual inspection.
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
    layout: str = Field("TITLE_AND_BODY", description="""Slide layout. Options:
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

    MANDATORY VISUAL VERIFICATION (inspect → change → verify):
    AFTER: You MUST call get_page_thumbnail(slide_id=...) to verify layout and content.
    If styling improvements are needed, you MUST call get_page_thumbnail first, then style, then verify again.

    FULL PROFESSIONAL EXAMPLES:

    # Cover slide
    add_slide(layout="TITLE", title="Q1 2026 Business Review", body="Prepared by Strategy Team")

    # Content slide with bullets
    add_slide(
        title="Key Achievements",
        body="Revenue grew 34% YoY\\nAcquired 12 enterprise clients\\nExpanded to 3 APAC markets\\nNPS reached 72",
        bullets=True,
        speaker_notes="Open with revenue headline. Pause for emphasis."
    )

    # KPI stat slide
    add_slide(layout="BIG_NUMBER", title="$4.2M", body="Annual Recurring Revenue")

    # Section divider
    add_slide(layout="SECTION_HEADER", title="Market Analysis", body="Understanding our competitive landscape")

    # Nested bullets
    add_slide(
        title="Product Roadmap",
        body="Q1 Deliverables\\n\\tCheckout redesign\\n\\tMobile app v2.0\\nQ2 Deliverables\\n\\tAI recommendations\\n\\tPartner API launch",
        bullets=True
    )

    # Testimonial / key takeaway
    add_slide(layout="MAIN_POINT", title="We exceeded every target this quarter.")

    # Quote slide
    add_slide(layout="CAPTION_ONLY", body="Innovation distinguishes between a leader and a follower. — Steve Jobs")

    Returns:
        str: JSON with slide_id, placeholder_ids, layout, and presentation link.
    """
    logger.info(f"[add_slide] Invoked. Email: '{user_google_email}', Layout: '{layout}'")

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
    slide_id: str = Field(..., description="The slide ID to update. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id), get_presentation response (slides[].slide_id), or add_slide response (slide_id)."),
    title: str = Field(None, description="New title text. PLAIN TEXT ONLY. Replaces existing title. Pass None to leave unchanged."),
    body: str = Field(None, description="New body text. PLAIN TEXT ONLY. Use \\n for new lines, \\t for indent. Replaces existing body. Pass None to leave unchanged."),
    speaker_notes: str = Field(None, description="New speaker notes. PLAIN TEXT ONLY. Replaces existing notes. Pass None to leave unchanged."),
    bullets: bool = Field(False, description="If True, auto-formats body text as bullet list after replacing."),
) -> str:
    """
    Replace text content on an existing slide (title, body, and/or speaker notes).

    USE THIS to update text on slides that already exist.
    Use add_slide to create new slides instead.

    MANDATORY VISUAL VERIFICATION (inspect → change → verify):
    AFTER: You MUST call get_page_thumbnail(slide_id=...) to verify layout and text rendering.

    Returns:
        str: JSON with slide_id, updated_fields list, and link.
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
    replace_text: str = Field(..., description="The replacement text. PLAIN TEXT ONLY."),
    match_case: bool = Field(True, description="If True (default), search is case-sensitive."),
) -> str:
    """
    Find and replace text across the entire presentation (all slides).

    USE THIS FOR:
    - Updating dates, names, or numbers across all slides at once
    - Template variable replacement (e.g., replace '{{company}}' with 'Acme Corp')

    MANDATORY VISUAL VERIFICATION:
    AFTER: You MUST call get_page_thumbnail(slide_id=...) on affected slides to verify layout is not broken.

    Returns:
        str: JSON with occurrences_replaced count.
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
    slide_id: str = Field(..., description="The slide ID to delete. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id) or get_presentation response (slides[].slide_id)."),
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
    slide_id: str = Field(..., description="The slide ID to duplicate. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id) or get_presentation response (slides[].slide_id)."),
) -> str:
    """
    Create an exact copy of a slide with all content and formatting preserved.

    USE THIS FOR:
    - Duplicating a template slide, then updating content for each variation
    - Creating consistent slides with the same design

    MANDATORY VISUAL VERIFICATION:
    AFTER: You MUST call get_page_thumbnail(slide_id=...) on affected slides to verify the result visually.

    Returns:
        str: JSON with original_slide_id, new_slide_id, and link.
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
    slide_id: str = Field(..., description="The slide ID to add the image to. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id), get_presentation response (slides[].slide_id), or add_slide response (slide_id)."),
    image_url: str = Field(..., description="Public HTTPS URL of the image. Google's servers fetch this URL server-side, so it must be a direct link to an image file (PNG, JPEG, GIF) that is publicly accessible without authentication, redirects, or bot protection. Many websites block server-side fetches. If no reliable image URL is available, ask the user to provide one or upload the image to a publicly accessible location first."),
    x: float = Field(100, description="X position in points from left edge. Standard slide is 720pt wide."),
    y: float = Field(100, description="Y position in points from top edge. Standard slide is 405pt tall."),
    width: float = Field(300, description="Width in points."),
    height: float = Field(200, description="Height in points."),
) -> str:
    """
    Insert an image from a URL onto a slide.

    COMMON IMAGE POSITIONS (720x405pt slide):
      Full slide background:  x=0,   y=0,   width=720, height=405
      Center large:           x=110, y=52,  width=500, height=300
      Center medium:          x=210, y=102, width=300, height=200
      Right half:             x=370, y=50,  width=330, height=300
      Left half:              x=20,  y=50,  width=330, height=300
      Top-right logo:         x=620, y=15,  width=80,  height=40
      Bottom-right logo:      x=620, y=360, width=80,  height=40
      Below title full-width: x=60,  y=100, width=600, height=280

    MANDATORY VISUAL VERIFICATION (inspect → change → verify):
    BEFORE: You MUST call get_page_thumbnail(slide_id=...) to inspect current state. Do NOT skip this step.
    AFTER: You MUST call get_page_thumbnail(slide_id=...) again to verify the result. If it looks wrong, iterate.

    Returns:
        str: JSON with element_id, slide_id, and link.
    """
    logger.info(f"[add_slide_image] Invoked. Email: '{user_google_email}', Slide: '{slide_id}'")

    element_id = uuid.uuid4().hex
    result = await _batch_update(service, presentation_id, [{
        "createImage": {
            "objectId": element_id,
            "url": image_url,
            "elementProperties": _element_properties(slide_id, x, y, width, height),
        }
    }])

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
    slide_id: str = Field(..., description="The slide ID to add the shape to. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id), get_presentation response (slides[].slide_id), or add_slide response (slide_id)."),
    shape_type: str = Field("TEXT_BOX", description="""Shape type to create. Options with professional use cases:
  "TEXT_BOX"        -> Custom text anywhere: labels, callouts, footnotes (DEFAULT)
  "RECTANGLE"       -> Card backgrounds, badges, color blocks
  "ROUND_RECTANGLE" -> Buttons, tags, modern cards
  "ELLIPSE"         -> KPI circles, avatars, decorative elements
  "TRIANGLE"        -> Warning indicators, decorative
  "DIAMOND"         -> Decision nodes in flowcharts
  "HEXAGON"         -> Tech/API labels, honeycomb layouts
  "CHEVRON"         -> Process flow steps (Acquire -> Onboard -> Retain)
  "RIGHT_ARROW"     -> Flow direction, next step
  "LEFT_ARROW"      -> Back/previous direction
  "UP_ARROW"        -> Growth indicator
  "DOWN_ARROW"      -> Decline indicator, funnel
  "STAR_5"          -> Ratings, highlights
  "STAR_10"         -> Decorative star
  "HEART"           -> Customer love metrics
  "CLOUD"           -> Cloud architecture diagrams"""),
    text: str = Field(None, description="Text inside the shape. PLAIN TEXT ONLY — no markdown. Use \\n for multiple lines. Example: 'Total Revenue\\n$4.2M'. After creation, use format_slide_text to style (bold, color, size) and update_shape_properties for fill/outline."),
    x: float = Field(100, description="X position in points from left edge (0-720)."),
    y: float = Field(100, description="Y position in points from top edge (0-405)."),
    width: float = Field(200, description="Width in points."),
    height: float = Field(100, description="Height in points."),
) -> str:
    """
    Add a shape or text box to a slide.

    After creating a shape, you can:
    - format_slide_text(element_id=...) to style the text inside
    - update_shape_properties(element_id=...) to set fill color and outline
    - transform_element(element_id=...) to reposition or resize

    MANDATORY VISUAL VERIFICATION (inspect → change → verify):
    BEFORE: You MUST call get_page_thumbnail(slide_id=...) to inspect current state. Do NOT skip this step.
    AFTER: You MUST call get_page_thumbnail(slide_id=...) again to verify the result. If it looks wrong, iterate.

    Returns:
        str: JSON with element_id, shape_type, slide_id, and link.
    """
    logger.info(f"[add_slide_shape] Invoked. Email: '{user_google_email}', Shape: '{shape_type}'")

    text = _clean_text(text)
    element_id = uuid.uuid4().hex
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
    slide_id: str = Field(..., description="The slide ID to add the line to. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id), get_presentation response (slides[].slide_id), or add_slide response (slide_id)."),
    line_category: str = Field("STRAIGHT", description="Line type: 'STRAIGHT' (default), 'BENT' (right-angle connector), 'CURVED' (curved connector)."),
    x: float = Field(100, description="Start X position in points."),
    y: float = Field(200, description="Start Y position in points."),
    width: float = Field(500, description="Horizontal extent in points. Must be >= 0 (use 0 for vertical line). To draw a line going left, set x to the right endpoint and use positive width going right instead."),
    height: float = Field(0, description="Vertical extent in points. Must be >= 0 (use 0 for horizontal line). To draw a line going up, set y to the top endpoint and use positive height going down instead."),
) -> str:
    """
    Add a line or connector to a slide.

    EXAMPLES:
    - Horizontal divider: x=60, y=200, width=600, height=0
    - Vertical separator: x=360, y=50, width=0, height=300
    - Diagonal line:      x=100, y=100, width=500, height=200

    MANDATORY VISUAL VERIFICATION:
    AFTER: You MUST call get_page_thumbnail(slide_id=...) on affected slides to verify the result visually.

    Returns:
        str: JSON with element_id, line_category, slide_id, and link.
    """
    logger.info(f"[add_slide_line] Invoked. Email: '{user_google_email}', Category: '{line_category}'")

    # Auto-fix negative dimensions by adjusting start position
    if width < 0:
        x = x + width
        width = abs(width)
    if height < 0:
        y = y + height
        height = abs(height)

    element_id = uuid.uuid4().hex
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
    slide_id: str = Field(..., description="The slide ID to add the table to. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id), get_presentation response (slides[].slide_id), or add_slide response (slide_id)."),
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
    x: float = Field(60, description="X position in points from left edge."),
    y: float = Field(100, description="Y position in points from top edge."),
    width: float = Field(600, description="Width in points. 600pt is a good default for a full-width table below a title."),
    height: float = Field(250, description="Height in points."),
) -> str:
    """
    Create a table on a slide with optional pre-filled data.

    AFTER CREATING A TABLE, style it with batch_update_presentation:
    - Cell backgrounds: updateTableCellProperties with tableCellBackgroundFill
    - Cell text styling: updateTextStyle with cellLocation (bold, color, font)
    - Cell text alignment: updateParagraphStyle with cellLocation
    - Cell borders: updateTableBorderProperties
    - Column widths: updateTableColumnProperties
    - Row heights: updateTableRowProperties
    See batch_update_presentation docstring for copy-paste examples.

    NOTE: format_slide_text and update_shape_properties do NOT work on table cells.

    MANDATORY VISUAL VERIFICATION:
    AFTER: You MUST call get_page_thumbnail(slide_id=...) on affected slides to verify the result visually.

    Returns:
        str: JSON with element_id, rows, columns, slide_id, and link.
    """
    logger.info(f"[add_slide_table] Invoked. Email: '{user_google_email}', {rows}x{columns}")

    element_id = uuid.uuid4().hex
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
    slide_id: str = Field(..., description="The slide ID to add the video to. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id), get_presentation response (slides[].slide_id), or add_slide response (slide_id)."),
    video_id: str = Field(..., description="YouTube video ID ONLY (not the full URL). Example: 'dQw4w9WgXcQ' from https://youtube.com/watch?v=dQw4w9WgXcQ"),
    x: float = Field(160, description="X position in points."),
    y: float = Field(90, description="Y position in points."),
    width: float = Field(400, description="Width in points. 400pt is good for centered 16:9 video."),
    height: float = Field(225, description="Height in points. 225pt matches 16:9 aspect ratio with 400pt width."),
) -> str:
    """
    Embed a YouTube video on a slide. Only YouTube videos are supported.

    MANDATORY VISUAL VERIFICATION:
    AFTER: You MUST call get_page_thumbnail(slide_id=...) on affected slides to verify the result visually.

    Returns:
        str: JSON with element_id, video_id, slide_id, and link.
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

    element_id = uuid.uuid4().hex
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
    font_family: str = Field(None, description="""Font family name. Common options:
  Modern:    "Montserrat", "Poppins", "Inter", "Roboto"
  Classic:   "Georgia", "Times New Roman", "Garamond"
  Clean:     "Arial", "Helvetica", "Open Sans", "Lato"
  Monospace: "Roboto Mono", "Source Code Pro" """),
    color: str = Field(None, description="""Text color as hex string "#RRGGBB". Common professional colors:
  White: "#FFFFFF"    Black: "#000000"    Dark text: "#212121"
  Muted: "#757575"    Light gray: "#9E9E9E"
  Blue:  "#1A73E8"    Red: "#EA4335"      Green: "#34A853"
  Orange: "#FB8C00"   Purple: "#7C3AED" """),
    link_url: str = Field(None, description="URL to hyperlink the text to. Example: 'https://example.com'. Set to empty string '' to remove link."),
    alignment: str = Field(None, description="Paragraph alignment: 'START' (left), 'CENTER', 'END' (right), 'JUSTIFIED'."),
    start_index: int = Field(None, description="Start character index for partial formatting (0-based). Omit to format ALL text in the element."),
    end_index: int = Field(None, description="End character index for partial formatting (exclusive). Omit to format ALL text. Example: to bold '$4.2M' at the start, use start_index=0, end_index=5. If end_index exceeds text length, it will be automatically clamped."),
) -> str:
    """
    Apply text formatting (bold, color, font, size, links, alignment) to text in any element.

    PROFESSIONAL STYLING RECIPES:

    # White title on dark background
    format_slide_text(element_id=..., bold=True, font_size=36, font_family="Montserrat", color="#FFFFFF", alignment="CENTER")

    # Brand-colored subtitle
    format_slide_text(element_id=..., font_size=18, font_family="Open Sans", color="#6C757D")

    # Hyperlinked CTA text
    format_slide_text(element_id=..., bold=True, color="#1A73E8", link_url="https://example.com")

    # Highlight specific number (e.g., "$4.2M" at start of text)
    format_slide_text(element_id=..., bold=True, color="#E53935", font_size=48, start_index=0, end_index=5)

    # Italic quote attribution
    format_slide_text(element_id=..., italic=True, font_size=14, color="#9E9E9E")

    For table cell text styling, use batch_update_presentation with updateTextStyle + cellLocation.

    MANDATORY VISUAL VERIFICATION (inspect → change → verify):
    BEFORE: You MUST call get_page_thumbnail(slide_id=...) to inspect current state. Do NOT skip this step.
    AFTER: You MUST call get_page_thumbnail(slide_id=...) again to verify the result. If it looks wrong, iterate.

    Returns:
        str: JSON with element_id, formatting_applied list, and link.
    """
    logger.info(f"[format_slide_text] Invoked. Email: '{user_google_email}', Element: '{element_id}'")

    requests = []
    formatting_applied = []

    # Text range
    if start_index is not None and end_index is not None:
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
    slide_id: str = Field(..., description="The slide ID to set the background for. NEVER guess or hardcode (e.g. 'p' is wrong). Get the actual ID from: create_presentation response (slides_created[].slide_id), get_presentation response (slides[].slide_id), or add_slide response (slide_id)."),
    color: str = Field(None, description="""Background color as hex "#RRGGBB". Provide color OR image_url, not both.
Professional colors:
  Dark:   "#1E1E2E", "#2D2D3F", "#0F172A", "#1A1A2E"
  Light:  "#FFFFFF", "#F8F9FA", "#F5F5F5", "#E8EAF6"
  Blue:   "#1A73E8", "#E3F2FD"    Green: "#E8F5E9"
  Warm:   "#FFF3E0", "#FFFDE7"    Purple: "#F3E5F5" """),
    image_url: str = Field(None, description="Public URL of background image. The image will be stretched to fill the slide. Provide image_url OR color, not both."),
) -> str:
    """
    Set the background of a slide to a solid color or an image.

    MANDATORY VISUAL VERIFICATION (inspect → change → verify):
    BEFORE: You MUST call get_page_thumbnail(slide_id=...) to inspect current state. Do NOT skip this step.
    AFTER: You MUST call get_page_thumbnail(slide_id=...) again to verify the result. If it looks wrong, iterate.

    Returns:
        str: JSON with slide_id, background_type, and link.
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
    fill_color: str = Field(None, description="""Fill color as hex "#RRGGBB". Common colors:
  Blue: "#1A73E8"    Red: "#EA4335"    Green: "#34A853"
  Orange: "#FB8C00"  Purple: "#7C3AED"  Dark: "#1E1E2E"
  Light: "#F5F5F5"   White: "#FFFFFF" """),
    outline_color: str = Field(None, description='Outline/border color as hex "#RRGGBB".'),
    outline_weight: float = Field(None, description="Outline thickness in points. Common: 1 (thin), 2 (medium), 3 (thick)."),
) -> str:
    """
    Update visual properties of a shape (fill color, outline color, border weight).

    Works on shapes and text boxes ONLY. For table cell backgrounds, use
    batch_update_presentation with updateTableCellProperties instead.

    MANDATORY VISUAL VERIFICATION (inspect → change → verify):
    BEFORE: You MUST call get_page_thumbnail(slide_id=...) to inspect current state. Do NOT skip this step.
    AFTER: You MUST call get_page_thumbnail(slide_id=...) again to verify the result. If it looks wrong, iterate.

    Returns:
        str: JSON with element_id, properties_updated list, and link.
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

    MANDATORY VISUAL VERIFICATION (inspect → change → verify):
    BEFORE: You MUST call get_page_thumbnail(slide_id=...) to inspect current state. Do NOT skip this step.
    AFTER: You MUST call get_page_thumbnail(slide_id=...) again to verify the result. If it looks wrong, iterate.

    Returns:
        str: JSON with element_id, transforms_applied list, and link.
    """
    logger.info(f"[transform_element] Invoked. Email: '{user_google_email}', Element: '{element_id}'")

    transforms_applied = []

    # Fetch current element size and transform in one call
    elem_data = await _get_element_data(service, presentation_id, element_id)
    if not elem_data:
        return error_response(404, f"Element '{element_id}' not found in presentation")

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

    MANDATORY VISUAL VERIFICATION:
    AFTER: You MUST call get_page_thumbnail(slide_id=...) on affected slides to verify the result visually.

    Returns:
        str: JSON with group_id, children, and link.
    """
    logger.info(f"[group_elements] Invoked. Email: '{user_google_email}', Elements: {element_ids}")

    group_id = uuid.uuid4().hex
    result = await _batch_update(service, presentation_id, [{
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

    MANDATORY VISUAL VERIFICATION:
    AFTER: You MUST call get_page_thumbnail(slide_id=...) on affected slides to verify the result visually.

    Returns:
        str: JSON with group_id, ungrouped=true, and link.
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

    MANDATORY VISUAL VERIFICATION:
    AFTER: You MUST call get_page_thumbnail(slide_id=...) on affected slides to verify the result visually.

    Returns:
        str: JSON with element_id, deleted=true, and link.
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
    operation: str = Field("BRING_TO_FRONT", description="Z-order operation: 'BRING_TO_FRONT' (default, topmost), 'BRING_FORWARD' (one step up), 'SEND_BACKWARD' (one step down), 'SEND_TO_BACK' (bottommost)."),
) -> str:
    """
    Change the stacking order (z-order) of an element on a slide.

    USE THIS FOR:
    - Bringing text above background shapes: set_element_z_order(text_id, "BRING_TO_FRONT")
    - Sending background rectangles behind content: set_element_z_order(bg_id, "SEND_TO_BACK")

    MANDATORY VISUAL VERIFICATION:
    AFTER: You MUST call get_page_thumbnail(slide_id=...) on affected slides to verify the result visually.

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
