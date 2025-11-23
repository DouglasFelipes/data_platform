"""URL normalizer utilities.

Functions to normalize/clean URLs and remove tracking params.
"""

from __future__ import annotations

from typing import Iterable
from urllib.parse import ParseResult, parse_qsl, urlencode, urlparse, urlunparse

DEFAULT_REMOVE_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
}


def normalize_url(
    url: str, remove_params: Iterable[str] | None = None, strip_fragment: bool = True
) -> str:
    """Return a normalized URL: absolute, cleaned query and optional fragment removal.

    This function is intentionally conservative: it only removes common tracking
    params and strips empty query strings.
    """
    remove = set(remove_params or DEFAULT_REMOVE_PARAMS)
    p: ParseResult = urlparse(url)
    q = [
        (k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k not in remove
    ]
    query = urlencode(q, doseq=True)
    fragment = "" if strip_fragment else p.fragment
    cleaned = urlunparse(
        (p.scheme, p.netloc, p.path or "", p.params or "", query or "", fragment or "")
    )
    return cleaned
