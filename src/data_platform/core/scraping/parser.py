"""HTML parsing helpers: link extraction and normalization.
"""

from __future__ import annotations

from typing import List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from data_platform.core.scraping.normalizer import normalize_url


def extract_links_from_html(
    html: str, base_url: str, patterns: Optional[List[str]] = None
) -> List[Tuple[str, str]]:
    """Extract links from HTML and return list of (url, link_text).

    - Normalizes links via `normalize_url`.
    - Returns absolute URLs.
    - Keeps link text for optional filtering.
    """
    soup = BeautifulSoup(html, "html.parser")
    results: List[Tuple[str, str]] = []

    for a in soup.find_all("a", href=True):
        raw = a.get("href")
        link_text = (a.get_text() or "").strip()
        try:
            full = urljoin(base_url, raw)
            norm = normalize_url(full)
            results.append((norm, link_text))
        except Exception:
            continue

    # Deduplicate preserving order by url
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for u, t in results:
        if u not in seen:
            seen.add(u)
            uniq.append((u, t))

    return uniq
