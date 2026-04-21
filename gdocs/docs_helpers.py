"""
Google Docs Helper Functions

This module provides utility functions for common Google Docs operations
to simplify the implementation of document editing tools.
"""
import logging
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


def hex_to_rgb_color(hex_color: str) -> Dict[str, float]:
    """
    Convert a hex color string to Google Docs API RgbColor format.

    Args:
        hex_color: Hex color string (e.g., '#FF0000', 'FF0000', '#f00')

    Returns:
        Dictionary with 'red', 'green', 'blue' float values (0.0-1.0)
    """
    hex_color = hex_color.lstrip('#')

    # Support shorthand hex (e.g., 'f00' -> 'ff0000')
    if len(hex_color) == 3:
        hex_color = ''.join(c * 2 for c in hex_color)

    if len(hex_color) != 6:
        raise ValueError(f"Invalid hex color: #{hex_color}. Expected format: #RRGGBB or #RGB")

    r = int(hex_color[0:2], 16) / 255.0
    g = int(hex_color[2:4], 16) / 255.0
    b = int(hex_color[4:6], 16) / 255.0

    return {'red': r, 'green': g, 'blue': b}


def _build_api_color(hex_color: str) -> Dict[str, Any]:
    """
    Build a Google Docs API color object from a hex color string.

    Args:
        hex_color: Hex color string (e.g., '#FF0000')

    Returns:
        API color object: {'color': {'rgbColor': {'red': ..., 'green': ..., 'blue': ...}}}
    """
    return {'color': {'rgbColor': hex_to_rgb_color(hex_color)}}


def build_text_style(
    bold: bool = None,
    italic: bool = None,
    underline: bool = None,
    strikethrough: bool = None,
    small_caps: bool = None,
    font_size: int = None,
    font_family: str = None,
    foreground_color: str = None,
    background_color: str = None,
    baseline_offset: str = None,
    link_url: str = None
) -> tuple[Dict[str, Any], list[str]]:
    """
    Build text style object for Google Docs API requests.

    Args:
        bold: Whether text should be bold
        italic: Whether text should be italic
        underline: Whether text should be underlined
        strikethrough: Whether text should have strikethrough
        small_caps: Whether text should be in small caps
        font_size: Font size in points
        font_family: Font family name
        foreground_color: Text color as hex string (e.g., '#FF0000')
        background_color: Text highlight/background color as hex string
        baseline_offset: Baseline offset: 'SUPERSCRIPT', 'SUBSCRIPT', or 'NONE'
        link_url: URL to create a hyperlink

    Returns:
        Tuple of (text_style_dict, list_of_field_names)
    """
    text_style = {}
    fields = []

    if bold is not None:
        text_style['bold'] = bold
        fields.append('bold')

    if italic is not None:
        text_style['italic'] = italic
        fields.append('italic')

    if underline is not None:
        text_style['underline'] = underline
        fields.append('underline')

    if strikethrough is not None:
        text_style['strikethrough'] = strikethrough
        fields.append('strikethrough')

    if small_caps is not None:
        text_style['smallCaps'] = small_caps
        fields.append('smallCaps')

    if font_size is not None:
        text_style['fontSize'] = {'magnitude': font_size, 'unit': 'PT'}
        fields.append('fontSize')

    if font_family is not None:
        text_style['weightedFontFamily'] = {'fontFamily': font_family}
        fields.append('weightedFontFamily')

    if foreground_color is not None:
        text_style['foregroundColor'] = _build_api_color(foreground_color)
        fields.append('foregroundColor')

    if background_color is not None:
        text_style['backgroundColor'] = _build_api_color(background_color)
        fields.append('backgroundColor')

    if baseline_offset is not None:
        text_style['baselineOffset'] = baseline_offset
        fields.append('baselineOffset')

    if link_url is not None:
        text_style['link'] = {'url': link_url}
        fields.append('link')

    return text_style, fields

def create_insert_text_request(index: int, text: str) -> Dict[str, Any]:
    """
    Create an insertText request for Google Docs API.
    
    Args:
        index: Position to insert text
        text: Text to insert
    
    Returns:
        Dictionary representing the insertText request
    """
    return {
        'insertText': {
            'location': {'index': index},
            'text': text
        }
    }

def create_delete_range_request(start_index: int, end_index: int) -> Dict[str, Any]:
    """
    Create a deleteContentRange request for Google Docs API.
    
    Args:
        start_index: Start position of content to delete
        end_index: End position of content to delete
    
    Returns:
        Dictionary representing the deleteContentRange request
    """
    return {
        'deleteContentRange': {
            'range': {
                'startIndex': start_index,
                'endIndex': end_index
            }
        }
    }

def create_format_text_request(
    start_index: int,
    end_index: int,
    bold: bool = None,
    italic: bool = None,
    underline: bool = None,
    font_size: int = None,
    font_family: str = None,
    strikethrough: bool = None,
    small_caps: bool = None,
    foreground_color: str = None,
    background_color: str = None,
    baseline_offset: str = None,
    link_url: str = None
) -> Optional[Dict[str, Any]]:
    """
    Create an updateTextStyle request for Google Docs API.

    Args:
        start_index: Start position of text to format
        end_index: End position of text to format
        bold: Whether text should be bold
        italic: Whether text should be italic
        underline: Whether text should be underlined
        font_size: Font size in points
        font_family: Font family name
        strikethrough: Whether text should have strikethrough
        small_caps: Whether text should be in small caps
        foreground_color: Text color as hex string (e.g., '#FF0000')
        background_color: Text highlight/background color as hex string
        baseline_offset: Baseline offset: 'SUPERSCRIPT', 'SUBSCRIPT', or 'NONE'
        link_url: URL to create a hyperlink

    Returns:
        Dictionary representing the updateTextStyle request, or None if no styles provided
    """
    text_style, fields = build_text_style(
        bold=bold, italic=italic, underline=underline,
        strikethrough=strikethrough, small_caps=small_caps,
        font_size=font_size, font_family=font_family,
        foreground_color=foreground_color, background_color=background_color,
        baseline_offset=baseline_offset, link_url=link_url
    )

    if not text_style:
        return None

    return {
        'updateTextStyle': {
            'range': {
                'startIndex': start_index,
                'endIndex': end_index
            },
            'textStyle': text_style,
            'fields': ','.join(fields)
        }
    }

def create_find_replace_request(
    find_text: str, 
    replace_text: str, 
    match_case: bool = False
) -> Dict[str, Any]:
    """
    Create a replaceAllText request for Google Docs API.
    
    Args:
        find_text: Text to find
        replace_text: Text to replace with
        match_case: Whether to match case exactly
    
    Returns:
        Dictionary representing the replaceAllText request
    """
    return {
        'replaceAllText': {
            'containsText': {
                'text': find_text,
                'matchCase': match_case
            },
            'replaceText': replace_text
        }
    }

def create_insert_table_request(index: int, rows: int, columns: int) -> Dict[str, Any]:
    """
    Create an insertTable request for Google Docs API.
    
    Args:
        index: Position to insert table
        rows: Number of rows
        columns: Number of columns
    
    Returns:
        Dictionary representing the insertTable request
    """
    return {
        'insertTable': {
            'location': {'index': index},
            'rows': rows,
            'columns': columns
        }
    }

def create_insert_page_break_request(index: int) -> Dict[str, Any]:
    """
    Create an insertPageBreak request for Google Docs API.
    
    Args:
        index: Position to insert page break
    
    Returns:
        Dictionary representing the insertPageBreak request
    """
    return {
        'insertPageBreak': {
            'location': {'index': index}
        }
    }

def create_insert_image_request(
    index: int, 
    image_uri: str,
    width: int = None,
    height: int = None
) -> Dict[str, Any]:
    """
    Create an insertInlineImage request for Google Docs API.
    
    Args:
        index: Position to insert image
        image_uri: URI of the image (Drive URL or public URL)
        width: Image width in points
        height: Image height in points
    
    Returns:
        Dictionary representing the insertInlineImage request
    """
    request = {
        'insertInlineImage': {
            'location': {'index': index},
            'uri': image_uri
        }
    }
    
    # Add size properties if specified
    object_size = {}
    if width is not None:
        object_size['width'] = {'magnitude': width, 'unit': 'PT'}
    if height is not None:
        object_size['height'] = {'magnitude': height, 'unit': 'PT'}
    
    if object_size:
        request['insertInlineImage']['objectSize'] = object_size
    
    return request

def create_bullet_list_request(
    start_index: int, 
    end_index: int,
    list_type: str = "UNORDERED"
) -> Dict[str, Any]:
    """
    Create a createParagraphBullets request for Google Docs API.
    
    Args:
        start_index: Start of text range to convert to list
        end_index: End of text range to convert to list
        list_type: Type of list ("UNORDERED" or "ORDERED")
    
    Returns:
        Dictionary representing the createParagraphBullets request
    """
    bullet_preset = (
        'BULLET_DISC_CIRCLE_SQUARE' 
        if list_type == "UNORDERED" 
        else 'NUMBERED_DECIMAL_ALPHA_ROMAN'
    )
    
    return {
        'createParagraphBullets': {
            'range': {
                'startIndex': start_index,
                'endIndex': end_index
            },
            'bulletPreset': bullet_preset
        }
    }

def create_paragraph_style_request(
    start_index: int,
    end_index: int,
    named_style_type: str = None,
    alignment: str = None,
    line_spacing: float = None,
    space_above: float = None,
    space_below: float = None,
    indent_first_line: float = None,
    indent_start: float = None,
    indent_end: float = None
) -> Optional[Dict[str, Any]]:
    """
    Create an updateParagraphStyle request for Google Docs API.

    Args:
        start_index: Start position of paragraph range
        end_index: End position of paragraph range
        named_style_type: Paragraph style type (e.g., 'HEADING_1', 'HEADING_2', ..., 'HEADING_6',
                          'TITLE', 'SUBTITLE', 'NORMAL_TEXT')
        alignment: Text alignment ('START', 'CENTER', 'END', 'JUSTIFIED')
        line_spacing: Line spacing as percentage (e.g., 100 for single, 200 for double)
        space_above: Space above paragraph in points
        space_below: Space below paragraph in points
        indent_first_line: First line indent in points
        indent_start: Left indent in points
        indent_end: Right indent in points

    Returns:
        Dictionary representing the updateParagraphStyle request, or None if no styles provided
    """
    paragraph_style = {}
    fields = []

    if named_style_type is not None:
        paragraph_style['namedStyleType'] = named_style_type
        fields.append('namedStyleType')

    if alignment is not None:
        paragraph_style['alignment'] = alignment
        fields.append('alignment')

    if line_spacing is not None:
        paragraph_style['lineSpacing'] = line_spacing
        fields.append('lineSpacing')

    if space_above is not None:
        paragraph_style['spaceAbove'] = {'magnitude': space_above, 'unit': 'PT'}
        fields.append('spaceAbove')

    if space_below is not None:
        paragraph_style['spaceBelow'] = {'magnitude': space_below, 'unit': 'PT'}
        fields.append('spaceBelow')

    if indent_first_line is not None:
        paragraph_style['indentFirstLine'] = {'magnitude': indent_first_line, 'unit': 'PT'}
        fields.append('indentFirstLine')

    if indent_start is not None:
        paragraph_style['indentStart'] = {'magnitude': indent_start, 'unit': 'PT'}
        fields.append('indentStart')

    if indent_end is not None:
        paragraph_style['indentEnd'] = {'magnitude': indent_end, 'unit': 'PT'}
        fields.append('indentEnd')

    if not paragraph_style:
        return None

    return {
        'updateParagraphStyle': {
            'range': {
                'startIndex': start_index,
                'endIndex': end_index
            },
            'paragraphStyle': paragraph_style,
            'fields': ','.join(fields)
        }
    }


def create_delete_bullets_request(start_index: int, end_index: int) -> Dict[str, Any]:
    """
    Create a deleteParagraphBullets request for Google Docs API.

    Args:
        start_index: Start of text range to remove bullets from
        end_index: End of text range to remove bullets from

    Returns:
        Dictionary representing the deleteParagraphBullets request
    """
    return {
        'deleteParagraphBullets': {
            'range': {
                'startIndex': start_index,
                'endIndex': end_index
            }
        }
    }


def create_insert_table_row_request(
    table_start_index: int,
    row_index: int,
    insert_below: bool = True
) -> Dict[str, Any]:
    """
    Create an insertTableRow request for Google Docs API.

    Args:
        table_start_index: Starting index of the table
        row_index: Reference row index
        insert_below: If True, insert below the reference row; otherwise above

    Returns:
        Dictionary representing the insertTableRow request
    """
    return {
        'insertTableRow': {
            'tableCellLocation': {
                'tableStartLocation': {'index': table_start_index},
                'rowIndex': row_index,
                'columnIndex': 0
            },
            'insertBelow': insert_below
        }
    }


def create_insert_table_column_request(
    table_start_index: int,
    column_index: int,
    insert_right: bool = True
) -> Dict[str, Any]:
    """
    Create an insertTableColumn request for Google Docs API.

    Args:
        table_start_index: Starting index of the table
        column_index: Reference column index
        insert_right: If True, insert to the right; otherwise to the left

    Returns:
        Dictionary representing the insertTableColumn request
    """
    return {
        'insertTableColumn': {
            'tableCellLocation': {
                'tableStartLocation': {'index': table_start_index},
                'rowIndex': 0,
                'columnIndex': column_index
            },
            'insertRight': insert_right
        }
    }


def create_delete_table_row_request(
    table_start_index: int,
    row_index: int
) -> Dict[str, Any]:
    """
    Create a deleteTableRow request for Google Docs API.

    Args:
        table_start_index: Starting index of the table
        row_index: Index of the row to delete

    Returns:
        Dictionary representing the deleteTableRow request
    """
    return {
        'deleteTableRow': {
            'tableCellLocation': {
                'tableStartLocation': {'index': table_start_index},
                'rowIndex': row_index,
                'columnIndex': 0
            }
        }
    }


def create_delete_table_column_request(
    table_start_index: int,
    column_index: int
) -> Dict[str, Any]:
    """
    Create a deleteTableColumn request for Google Docs API.

    Args:
        table_start_index: Starting index of the table
        column_index: Index of the column to delete

    Returns:
        Dictionary representing the deleteTableColumn request
    """
    return {
        'deleteTableColumn': {
            'tableCellLocation': {
                'tableStartLocation': {'index': table_start_index},
                'rowIndex': 0,
                'columnIndex': column_index
            }
        }
    }


def create_merge_table_cells_request(
    table_start_index: int,
    row_index: int,
    column_index: int,
    row_span: int,
    column_span: int
) -> Dict[str, Any]:
    """
    Create a mergeTableCells request for Google Docs API.

    Args:
        table_start_index: Starting index of the table
        row_index: Starting row index of the merge range
        column_index: Starting column index of the merge range
        row_span: Number of rows to merge
        column_span: Number of columns to merge

    Returns:
        Dictionary representing the mergeTableCells request
    """
    return {
        'mergeTableCells': {
            'tableRange': {
                'tableCellLocation': {
                    'tableStartLocation': {'index': table_start_index},
                    'rowIndex': row_index,
                    'columnIndex': column_index
                },
                'rowSpan': row_span,
                'columnSpan': column_span
            }
        }
    }


def create_unmerge_table_cells_request(
    table_start_index: int,
    row_index: int,
    column_index: int,
    row_span: int,
    column_span: int
) -> Dict[str, Any]:
    """
    Create an unmergeTableCells request for Google Docs API.

    Args:
        table_start_index: Starting index of the table
        row_index: Starting row index of the unmerge range
        column_index: Starting column index of the unmerge range
        row_span: Number of rows to unmerge
        column_span: Number of columns to unmerge

    Returns:
        Dictionary representing the unmergeTableCells request
    """
    return {
        'unmergeTableCells': {
            'tableRange': {
                'tableCellLocation': {
                    'tableStartLocation': {'index': table_start_index},
                    'rowIndex': row_index,
                    'columnIndex': column_index
                },
                'rowSpan': row_span,
                'columnSpan': column_span
            }
        }
    }


def validate_operation(operation: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate a batch operation dictionary.
    
    Args:
        operation: Operation dictionary to validate
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    op_type = operation.get('type')
    if not op_type:
        return False, "Missing 'type' field"
    
    # Validate required fields for each operation type
    required_fields = {
        'insert_text': ['index', 'text'],
        'delete_text': ['start_index', 'end_index'],
        'replace_text': ['start_index', 'end_index', 'text'],
        'format_text': ['start_index', 'end_index'],
        'format_paragraph': ['start_index', 'end_index'],
        'insert_table': ['index', 'rows', 'columns'],
        'insert_page_break': ['index'],
        'find_replace': ['find_text', 'replace_text'],
        'delete_bullets': ['start_index', 'end_index'],
        'insert_table_row': ['table_start_index', 'row_index'],
        'insert_table_column': ['table_start_index', 'column_index'],
        'delete_table_row': ['table_start_index', 'row_index'],
        'delete_table_column': ['table_start_index', 'column_index'],
        'merge_table_cells': ['table_start_index', 'row_index', 'column_index', 'row_span', 'column_span'],
        'unmerge_table_cells': ['table_start_index', 'row_index', 'column_index', 'row_span', 'column_span'],
    }
    
    if op_type not in required_fields:
        return False, f"Unsupported operation type: {op_type or 'None'}"
    
    for field in required_fields[op_type]:
        if field not in operation:
            return False, f"Missing required field: {field}"
    
    return True, ""

