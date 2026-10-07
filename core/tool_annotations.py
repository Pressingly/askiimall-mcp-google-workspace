"""
Read/write hints published with every tool.

MCP clients read these to decide whether a call needs the user's confirmation
before it runs. Every tool states which of the three it is; a tool with no hint
is treated by clients as a write.
"""

from mcp.types import ToolAnnotations

# Reads data and changes nothing.
READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)

# Creates something new and leaves existing data as it was.
ADDITIVE_WRITE = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=False,
    openWorldHint=True,
)

# Can change or remove data that already exists.
DESTRUCTIVE_WRITE = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=True,
    idempotentHint=False,
    openWorldHint=True,
)
