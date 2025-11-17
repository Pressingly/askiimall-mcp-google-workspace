"""
Google Custom Search (PSE) MCP Tools

This module provides MCP tools for interacting with Google Programmable Search Engine.
"""

import logging
import asyncio
import os
from typing import Optional, List, Literal

from pydantic import Field

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors

logger = logging.getLogger(__name__)


@server.tool()
@handle_http_errors("search_custom", is_read_only=True, service_type="customsearch")
@require_google_service("customsearch", "customsearch")
async def search_custom(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    q: str = Field(..., description="The search query string."),
    num: int = Field(10, description="Number of results to return. Must be between 1 and 10. Defaults to 10."),
    start: int = Field(1, description="The index of the first result to return (1-based). Use this for pagination. Defaults to 1."),
    safe: Literal["active", "moderate", "off"] = Field("off", description="Safe search level. Options: 'active' (strict filtering), 'moderate' (moderate filtering), 'off' (no filtering). Defaults to 'off'."),
    search_type: Optional[Literal["image"]] = Field(None, description="Search type. If set to 'image', searches for images. If None, performs a regular web search."),
    site_search: Optional[str] = Field(None, description="Restrict search to a specific site or domain. Example: 'example.com' or '*.edu'."),
    site_search_filter: Optional[Literal["e", "i"]] = Field(None, description="Filter for site_search. Options: 'e' (exclude site_search results), 'i' (include only site_search results). Only used when site_search is provided."),
    date_restrict: Optional[str] = Field(None, description="Restrict results by date. Examples: 'd5' (past 5 days), 'w2' (past 2 weeks), 'm3' (past 3 months), 'y1' (past year)."),
    file_type: Optional[str] = Field(None, description="Filter by file type. Examples: 'pdf', 'doc', 'xls', 'ppt', 'rtf', 'swf', 'ps'."),
    language: Optional[str] = Field(None, description="Language code for results. Examples: 'lang_en' (English), 'lang_es' (Spanish), 'lang_fr' (French)."),
    country: Optional[str] = Field(None, description="Country code for results. Examples: 'countryUS' (United States), 'countryGB' (United Kingdom), 'countryCA' (Canada)."),
) -> str:
    """
    Performs a search using Google Custom Search JSON API.

    Args:
        user_google_email: The user's Google email address.
        q: The search query string.
        num: Number of results to return. Must be between 1 and 10. Defaults to 10.
        start: The index of the first result to return (1-based). Use this for pagination. Defaults to 1.
        safe: Safe search level. Options: 'active' (strict filtering), 'moderate' (moderate filtering), 'off' (no filtering). Defaults to 'off'.
        search_type: Search type. If set to 'image', searches for images. If None, performs a regular web search.
        site_search: Restrict search to a specific site or domain. Example: 'example.com' or '*.edu'.
        site_search_filter: Filter for site_search. Options: 'e' (exclude site_search results), 'i' (include only site_search results). Only used when site_search is provided.
        date_restrict: Restrict results by date. Examples: 'd5' (past 5 days), 'w2' (past 2 weeks), 'm3' (past 3 months), 'y1' (past year).
        file_type: Filter by file type. Examples: 'pdf', 'doc', 'xls', 'ppt', 'rtf', 'swf', 'ps'.
        language: Language code for results. Examples: 'lang_en' (English), 'lang_es' (Spanish), 'lang_fr' (French).
        country: Country code for results. Examples: 'countryUS' (United States), 'countryGB' (United Kingdom), 'countryCA' (Canada).

    Returns:
        str: Formatted search results including title, link, and snippet for each result.
    """
    # Get API key and search engine ID from environment
    api_key = os.environ.get('GOOGLE_PSE_API_KEY')
    if not api_key:
        raise ValueError("GOOGLE_PSE_API_KEY environment variable not set. Please set it to your Google Custom Search API key.")
    
    cx = os.environ.get('GOOGLE_PSE_ENGINE_ID')
    if not cx:
        raise ValueError("GOOGLE_PSE_ENGINE_ID environment variable not set. Please set it to your Programmable Search Engine ID.")

    logger.info(f"[search_custom] Invoked. Email: '{user_google_email}', Query: '{q}', CX: '{cx}'")

    # Build the request parameters
    params = {
        'key': api_key,
        'cx': cx,
        'q': q,
        'num': num,
        'start': start,
        'safe': safe
    }

    # Add optional parameters
    if search_type:
        params['searchType'] = search_type
    if site_search:
        params['siteSearch'] = site_search
    if site_search_filter:
        params['siteSearchFilter'] = site_search_filter
    if date_restrict:
        params['dateRestrict'] = date_restrict
    if file_type:
        params['fileType'] = file_type
    if language:
        params['lr'] = language
    if country:
        params['cr'] = country

    # Execute the search request
    result = await asyncio.to_thread(
        service.cse().list(**params).execute
    )

    # Extract search information
    search_info = result.get('searchInformation', {})
    total_results = search_info.get('totalResults', '0')
    search_time = search_info.get('searchTime', 0)

    # Extract search results
    items = result.get('items', [])

    # Format the response
    confirmation_message = f"""Search Results for {user_google_email}:
- Query: "{q}"
- Search Engine ID: {cx}
- Total Results: {total_results}
- Search Time: {search_time:.3f} seconds
- Results Returned: {len(items)} (showing {start} to {start + len(items) - 1})

"""

    if items:
        confirmation_message += "Results:\n"
        for i, item in enumerate(items, start):
            title = item.get('title', 'No title')
            link = item.get('link', 'No link')
            snippet = item.get('snippet', 'No description available').replace('\n', ' ')

            confirmation_message += f"\n{i}. {title}\n"
            confirmation_message += f"   URL: {link}\n"
            confirmation_message += f"   Snippet: {snippet}\n"

            # Add additional metadata if available
            if 'pagemap' in item:
                pagemap = item['pagemap']
                if 'metatags' in pagemap and pagemap['metatags']:
                    metatag = pagemap['metatags'][0]
                    if 'og:type' in metatag:
                        confirmation_message += f"   Type: {metatag['og:type']}\n"
                    if 'article:published_time' in metatag:
                        confirmation_message += f"   Published: {metatag['article:published_time'][:10]}\n"
    else:
        confirmation_message += "\nNo results found."

    # Add information about pagination
    queries = result.get('queries', {})
    if 'nextPage' in queries:
        next_start = queries['nextPage'][0].get('startIndex', 0)
        confirmation_message += f"\n\nTo see more results, search again with start={next_start}"

    logger.info(f"Search completed successfully for {user_google_email}")
    return confirmation_message


@server.tool()
@handle_http_errors("get_search_engine_info", is_read_only=True, service_type="customsearch")
@require_google_service("customsearch", "customsearch")
async def get_search_engine_info(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
) -> str:
    """
    Retrieves metadata about a Programmable Search Engine.

    Args:
        user_google_email: The user's Google email address.

    Returns:
        str: Information about the search engine including its configuration and available refinements.
    """
    # Get API key and search engine ID from environment
    api_key = os.environ.get('GOOGLE_PSE_API_KEY')
    if not api_key:
        raise ValueError("GOOGLE_PSE_API_KEY environment variable not set. Please set it to your Google Custom Search API key.")
    
    cx = os.environ.get('GOOGLE_PSE_ENGINE_ID')
    if not cx:
        raise ValueError("GOOGLE_PSE_ENGINE_ID environment variable not set. Please set it to your Programmable Search Engine ID.")

    logger.info(f"[get_search_engine_info] Invoked. Email: '{user_google_email}', CX: '{cx}'")

    # Perform a minimal search to get the search engine context
    params = {
        'key': api_key,
        'cx': cx,
        'q': 'test',  # Minimal query to get metadata
        'num': 1
    }

    result = await asyncio.to_thread(
        service.cse().list(**params).execute
    )

    # Extract context information
    context = result.get('context', {})
    title = context.get('title', 'Unknown')

    confirmation_message = f"""Search Engine Information for {user_google_email}:
- Search Engine ID: {cx}
- Title: {title}
"""

    # Add facet information if available
    if 'facets' in context:
        confirmation_message += "\nAvailable Refinements:\n"
        for facet in context['facets']:
            for item in facet:
                label = item.get('label', 'Unknown')
                anchor = item.get('anchor', 'Unknown')
                confirmation_message += f"  - {label} (anchor: {anchor})\n"

    # Add search information
    search_info = result.get('searchInformation', {})
    if search_info:
        total_results = search_info.get('totalResults', 'Unknown')
        confirmation_message += "\nSearch Statistics:\n"
        confirmation_message += f"  - Total indexed results: {total_results}\n"

    logger.info(f"Search engine info retrieved successfully for {user_google_email}")
    return confirmation_message


@server.tool()
@handle_http_errors("search_custom_siterestrict", is_read_only=True, service_type="customsearch")
@require_google_service("customsearch", "customsearch")
async def search_custom_siterestrict(
    service,
    user_google_email: str = Field(..., description="The user's Google email address."),
    q: str = Field(..., description="The search query string."),
    sites: List[str] = Field(..., description="List of sites or domains to search within. Example: ['example.com', 'another-site.org']. The search will be restricted to these sites."),
    num: int = Field(10, description="Number of results to return. Must be between 1 and 10. Defaults to 10."),
    start: int = Field(1, description="The index of the first result to return (1-based). Use this for pagination. Defaults to 1."),
    safe: Literal["active", "moderate", "off"] = Field("off", description="Safe search level. Options: 'active' (strict filtering), 'moderate' (moderate filtering), 'off' (no filtering). Defaults to 'off'."),
) -> str:
    """
    Performs a search restricted to specific sites using Google Custom Search.

    Args:
        user_google_email: The user's Google email address.
        q: The search query string.
        sites: List of sites or domains to search within. Example: ['example.com', 'another-site.org']. The search will be restricted to these sites.
        num: Number of results to return. Must be between 1 and 10. Defaults to 10.
        start: The index of the first result to return (1-based). Use this for pagination. Defaults to 1.
        safe: Safe search level. Options: 'active' (strict filtering), 'moderate' (moderate filtering), 'off' (no filtering). Defaults to 'off'.

    Returns:
        str: Formatted search results from the specified sites.
    """
    logger.info(f"[search_custom_siterestrict] Invoked. Email: '{user_google_email}', Query: '{q}', Sites: {sites}")

    # Build site restriction query
    site_query = " OR ".join([f"site:{site}" for site in sites])
    full_query = f"{q} ({site_query})"

    # Use the main search function with the modified query
    return await search_custom(
        service=service,
        user_google_email=user_google_email,
        q=full_query,
        num=num,
        start=start,
        safe=safe
    )