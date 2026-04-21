"""
Batch Operation Manager

This module provides high-level batch operation management for Google Docs,
extracting complex validation and request building logic.
"""
import logging
import asyncio
from typing import Any, Union, Dict, List, Tuple

from gdocs.docs_helpers import (
    create_insert_text_request,
    create_delete_range_request,
    create_format_text_request,
    create_find_replace_request,
    create_insert_table_request,
    create_insert_page_break_request,
    create_paragraph_style_request,
    create_delete_bullets_request,
    create_insert_table_row_request,
    create_insert_table_column_request,
    create_delete_table_row_request,
    create_delete_table_column_request,
    create_merge_table_cells_request,
    create_unmerge_table_cells_request,
    validate_operation
)

logger = logging.getLogger(__name__)


class BatchOperationManager:
    """
    High-level manager for Google Docs batch operations.
    
    Handles complex multi-operation requests including:
    - Operation validation and request building
    - Batch execution with proper error handling
    - Operation result processing and reporting
    """
    
    def __init__(self, service):
        """
        Initialize the batch operation manager.
        
        Args:
            service: Google Docs API service instance
        """
        self.service = service
        
    async def execute_batch_operations(
        self,
        document_id: str,
        operations: list[dict[str, Any]]
    ) -> tuple[bool, str, dict[str, Any]]:
        """
        Execute multiple document operations in a single atomic batch.
        
        This method extracts the complex logic from batch_update_doc tool function.
        
        Args:
            document_id: ID of the document to update
            operations: List of operation dictionaries
            
        Returns:
            Tuple of (success, message, metadata)
        """
        logger.info(f"Executing batch operations on document {document_id}")
        logger.info(f"Operations count: {len(operations)}")
        
        if not operations:
            return False, "No operations provided. Please provide at least one operation.", {}
            
        try:
            # Validate and build requests
            requests, operation_descriptions = await self._validate_and_build_requests(operations)
            
            if not requests:
                return False, "No valid requests could be built from operations", {}
            
            # Execute the batch
            result = await self._execute_batch_requests(document_id, requests)
            
            # Process results
            metadata = {
                'operations_count': len(operations),
                'requests_count': len(requests),
                'replies_count': len(result.get('replies', [])),
                'operation_summary': operation_descriptions[:5]  # First 5 operations
            }
            
            summary = self._build_operation_summary(operation_descriptions)
            
            return True, f"Successfully executed {len(operations)} operations ({summary})", metadata
            
        except Exception as e:
            logger.error(f"Failed to execute batch operations: {str(e)}")
            return False, f"Batch operation failed: {str(e)}", {}
    
    async def _validate_and_build_requests(
        self,
        operations: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """
        Validate operations and build API requests.
        
        Args:
            operations: List of operation dictionaries
            
        Returns:
            Tuple of (requests, operation_descriptions)
        """
        requests = []
        operation_descriptions = []
        
        for i, op in enumerate(operations):
            # Validate operation structure
            is_valid, error_msg = validate_operation(op)
            if not is_valid:
                raise ValueError(f"Operation {i+1}: {error_msg}")
            
            op_type = op.get('type')
            
            try:
                # Build request based on operation type
                result = self._build_operation_request(op, op_type)
                
                # Handle both single request and list of requests
                if isinstance(result[0], list):
                    # Multiple requests (e.g., replace_text)
                    for req in result[0]:
                        requests.append(req)
                    operation_descriptions.append(result[1])
                elif result[0]:
                    # Single request
                    requests.append(result[0])
                    operation_descriptions.append(result[1])
                    
            except KeyError as e:
                raise ValueError(f"Operation {i+1} ({op_type}) missing required field: {e}")
            except Exception as e:
                raise ValueError(f"Operation {i+1} ({op_type}) failed validation: {str(e)}")
                
        return requests, operation_descriptions
    
    def _build_operation_request(
        self,
        op: dict[str, Any],
        op_type: str
    ) -> Tuple[Union[Dict[str, Any], List[Dict[str, Any]]], str]:
        """
        Build a single operation request.
        
        Args:
            op: Operation dictionary
            op_type: Operation type
            
        Returns:
            Tuple of (request, description)
        """
        if op_type == 'insert_text':
            request = create_insert_text_request(op['index'], op['text'])
            description = f"insert text at {op['index']}"
            
        elif op_type == 'delete_text':
            request = create_delete_range_request(op['start_index'], op['end_index'])
            description = f"delete text {op['start_index']}-{op['end_index']}"
            
        elif op_type == 'replace_text':
            # Replace is delete + insert (must be done in this order)
            delete_request = create_delete_range_request(op['start_index'], op['end_index'])
            insert_request = create_insert_text_request(op['start_index'], op['text'])
            # Return both requests as a list
            request = [delete_request, insert_request]
            description = f"replace text {op['start_index']}-{op['end_index']} with '{op['text'][:20]}{'...' if len(op['text']) > 20 else ''}'"
            
        elif op_type == 'format_text':
            request = create_format_text_request(
                op['start_index'], op['end_index'],
                op.get('bold'), op.get('italic'), op.get('underline'),
                op.get('font_size'), op.get('font_family'),
                strikethrough=op.get('strikethrough'),
                small_caps=op.get('small_caps'),
                foreground_color=op.get('foreground_color'),
                background_color=op.get('background_color'),
                baseline_offset=op.get('baseline_offset'),
                link_url=op.get('link_url')
            )

            if not request:
                raise ValueError("No formatting options provided")

            # Build format description
            format_changes = []
            for param, name in [
                ('bold', 'bold'), ('italic', 'italic'), ('underline', 'underline'),
                ('strikethrough', 'strikethrough'), ('small_caps', 'small caps'),
                ('font_size', 'font size'), ('font_family', 'font family'),
                ('foreground_color', 'text color'), ('background_color', 'bg color'),
                ('baseline_offset', 'baseline'), ('link_url', 'link')
            ]:
                if op.get(param) is not None:
                    value = f"{op[param]}pt" if param == 'font_size' else op[param]
                    format_changes.append(f"{name}: {value}")

            description = f"format text {op['start_index']}-{op['end_index']} ({', '.join(format_changes)})"
            
        elif op_type == 'insert_table':
            request = create_insert_table_request(op['index'], op['rows'], op['columns'])
            description = f"insert {op['rows']}x{op['columns']} table at {op['index']}"
            
        elif op_type == 'insert_page_break':
            request = create_insert_page_break_request(op['index'])
            description = f"insert page break at {op['index']}"
            
        elif op_type == 'find_replace':
            request = create_find_replace_request(
                op['find_text'], op['replace_text'], op.get('match_case', False)
            )
            description = f"find/replace '{op['find_text']}' → '{op['replace_text']}'"

        elif op_type == 'format_paragraph':
            request = create_paragraph_style_request(
                op['start_index'], op['end_index'],
                named_style_type=op.get('named_style_type'),
                alignment=op.get('alignment'),
                line_spacing=op.get('line_spacing'),
                space_above=op.get('space_above'),
                space_below=op.get('space_below'),
                indent_first_line=op.get('indent_first_line'),
                indent_start=op.get('indent_start'),
                indent_end=op.get('indent_end')
            )
            if not request:
                raise ValueError("No paragraph formatting options provided")
            description = f"format paragraph {op['start_index']}-{op['end_index']}"

        elif op_type == 'delete_bullets':
            request = create_delete_bullets_request(op['start_index'], op['end_index'])
            description = f"delete bullets {op['start_index']}-{op['end_index']}"

        elif op_type == 'insert_table_row':
            request = create_insert_table_row_request(
                op['table_start_index'], op['row_index'], op.get('insert_below', True)
            )
            description = f"insert table row at {op['row_index']}"

        elif op_type == 'insert_table_column':
            request = create_insert_table_column_request(
                op['table_start_index'], op['column_index'], op.get('insert_right', True)
            )
            description = f"insert table column at {op['column_index']}"

        elif op_type == 'delete_table_row':
            request = create_delete_table_row_request(
                op['table_start_index'], op['row_index']
            )
            description = f"delete table row {op['row_index']}"

        elif op_type == 'delete_table_column':
            request = create_delete_table_column_request(
                op['table_start_index'], op['column_index']
            )
            description = f"delete table column {op['column_index']}"

        elif op_type == 'merge_table_cells':
            request = create_merge_table_cells_request(
                op['table_start_index'], op['row_index'], op['column_index'],
                op['row_span'], op['column_span']
            )
            description = f"merge cells at ({op['row_index']},{op['column_index']}) span {op['row_span']}x{op['column_span']}"

        elif op_type == 'unmerge_table_cells':
            request = create_unmerge_table_cells_request(
                op['table_start_index'], op['row_index'], op['column_index'],
                op['row_span'], op['column_span']
            )
            description = f"unmerge cells at ({op['row_index']},{op['column_index']}) span {op['row_span']}x{op['column_span']}"

        else:
            supported_types = [
                'insert_text', 'delete_text', 'replace_text', 'format_text',
                'format_paragraph', 'insert_table', 'insert_page_break', 'find_replace',
                'delete_bullets', 'insert_table_row', 'insert_table_column',
                'delete_table_row', 'delete_table_column',
                'merge_table_cells', 'unmerge_table_cells'
            ]
            raise ValueError(f"Unsupported operation type '{op_type}'. Supported: {', '.join(supported_types)}")
            
        return request, description
    
    async def _execute_batch_requests(
        self,
        document_id: str,
        requests: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Execute the batch requests against the Google Docs API.
        
        Args:
            document_id: Document ID
            requests: List of API requests
            
        Returns:
            API response
        """
        return await asyncio.to_thread(
            self.service.documents().batchUpdate(
                documentId=document_id,
                body={'requests': requests}
            ).execute
        )
    
    def _build_operation_summary(self, operation_descriptions: list[str]) -> str:
        """
        Build a concise summary of operations performed.
        
        Args:
            operation_descriptions: List of operation descriptions
            
        Returns:
            Summary string
        """
        if not operation_descriptions:
            return "no operations"
            
        summary_items = operation_descriptions[:3]  # Show first 3 operations
        summary = ', '.join(summary_items)
        
        if len(operation_descriptions) > 3:
            remaining = len(operation_descriptions) - 3
            summary += f" and {remaining} more operation{'s' if remaining > 1 else ''}"
            
        return summary
    
    def get_supported_operations(self) -> dict[str, Any]:
        """
        Get information about supported batch operations.
        
        Returns:
            Dictionary with supported operation types and their required parameters
        """
        return {
            'supported_operations': {
                'insert_text': {
                    'required': ['index', 'text'],
                    'description': 'Insert text at specified index'
                },
                'delete_text': {
                    'required': ['start_index', 'end_index'],
                    'description': 'Delete text in specified range'
                },
                'replace_text': {
                    'required': ['start_index', 'end_index', 'text'],
                    'description': 'Replace text in range with new text'
                },
                'format_text': {
                    'required': ['start_index', 'end_index'],
                    'optional': ['bold', 'italic', 'underline', 'font_size', 'font_family'],
                    'description': 'Apply formatting to text range'
                },
                'insert_table': {
                    'required': ['index', 'rows', 'columns'],
                    'description': 'Insert table at specified index'
                },
                'insert_page_break': {
                    'required': ['index'],
                    'description': 'Insert page break at specified index'
                },
                'find_replace': {
                    'required': ['find_text', 'replace_text'],
                    'optional': ['match_case'],
                    'description': 'Find and replace text throughout document'
                },
                'format_paragraph': {
                    'required': ['start_index', 'end_index'],
                    'optional': ['named_style_type', 'alignment', 'line_spacing', 'space_above', 'space_below', 'indent_first_line', 'indent_start', 'indent_end'],
                    'description': 'Apply paragraph formatting (headings, alignment, spacing, indentation)'
                },
                'delete_bullets': {
                    'required': ['start_index', 'end_index'],
                    'description': 'Remove bullet/list formatting from text range'
                },
                'insert_table_row': {
                    'required': ['table_start_index', 'row_index'],
                    'optional': ['insert_below'],
                    'description': 'Insert a row in an existing table'
                },
                'insert_table_column': {
                    'required': ['table_start_index', 'column_index'],
                    'optional': ['insert_right'],
                    'description': 'Insert a column in an existing table'
                },
                'delete_table_row': {
                    'required': ['table_start_index', 'row_index'],
                    'description': 'Delete a row from an existing table'
                },
                'delete_table_column': {
                    'required': ['table_start_index', 'column_index'],
                    'description': 'Delete a column from an existing table'
                },
                'merge_table_cells': {
                    'required': ['table_start_index', 'row_index', 'column_index', 'row_span', 'column_span'],
                    'description': 'Merge a range of table cells'
                },
                'unmerge_table_cells': {
                    'required': ['table_start_index', 'row_index', 'column_index', 'row_span', 'column_span'],
                    'description': 'Unmerge previously merged table cells'
                }
            },
            'example_operations': [
                {"type": "insert_text", "index": 1, "text": "Hello World"},
                {"type": "format_text", "start_index": 1, "end_index": 12, "bold": True},
                {"type": "format_paragraph", "start_index": 1, "end_index": 12, "named_style_type": "HEADING_1", "alignment": "CENTER"},
                {"type": "insert_table", "index": 20, "rows": 2, "columns": 3}
            ]
        }