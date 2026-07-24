# =============================================================================
# askiimall-mcp-google-workspace — local development Makefile
# =============================================================================
# make sync   install deps      make dev   run the MCP server (streamable-http)
# make stop   stop the server (pattern-based)
#
# Port is env-driven: main.py reads PORT / WORKSPACE_MCP_PORT from .env
# (default 8000; this repo's .env sets 8004). Override the tool set with
# `make dev TOOLS="gmail drive"`.
# =============================================================================
.DEFAULT_GOAL := help
SHELL := /bin/bash

UV    ?= uv
TOOLS ?= gmail slides drive calendar docs sheets forms tasks

.PHONY: help sync dev stop

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_-]+:.*?## / {printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

sync: ## Install deps via uv
	$(UV) sync

dev: ## Run the MCP server (streamable-http; port from WORKSPACE_MCP_PORT/.env)
	$(UV) run main.py --transport streamable-http --tools $(TOOLS)

stop: ## Stop the running MCP server (pattern-based)
	@pkill -f "main.py --transport streamable-http --tools" 2>/dev/null || true; \
	 echo "google-workspace MCP stopped."
