"""Abstract base scraper (plugin) interface used by flows.

Plugins should be small: implement `filter_links` and optionally `clean_text`.
"""

from __future__ import annotations

from typing import Iterable, List, Tuple


class BaseScraper:
    """Minimal plugin interface for domain-specific scrapers."""

    def __init__(self, url: str, params: dict | None = None):
        self.url = url
        self.params = params or {}

    def filter_links(self, links: Iterable[Tuple[str, str]]) -> List[str]:
        """Given iterable of (url, link_text) return filtered list of urls.

        Default behavior is to return all URLs (no-op) so the flow can still
        download everything if the plugin does not restrict.
        """
        return [u for u, _ in links]

    def clean_text(self, text: str) -> str:
        return text.strip()
