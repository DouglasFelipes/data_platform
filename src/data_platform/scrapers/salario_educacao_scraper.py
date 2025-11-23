"""Example plugin for the Salário Educação "consultas" page.

This scraper illustrates how to implement a domain-specific filter that is
small and only contains rules for selecting links of interest.
"""

from __future__ import annotations

from typing import Iterable, List, Tuple
from urllib.parse import urlparse

from .base_scraper import BaseScraper


class SalarioEducacaoScraper(BaseScraper):
    """Filter links for the Salário Educação portal.

    This plugin looks for filenames or link text that contain a configured
    substring. It reads `params['filename_contains']` or falls back to a
    sensible default for the monthly distribution PDF.
    """

    DEFAULT_HINT = "DistribuioMensalporUF"

    """
        Define o método que recebe uma lista de links.
        Cada link é uma tupla (url, texto_do_link).
    """

    def filter_links(self, links: Iterable[Tuple[str, str]]) -> List[str]:
        hint = (self.params or {}).get("filename_contains") or self.DEFAULT_HINT
        hint = str(hint).lower()
        selected: List[str] = []
        for u, text in links:
            try:
                name = urlparse(u).path.split("/")[-1].lower()
                if hint in name or hint in (text or "").lower():
                    selected.append(u)
            except Exception:
                continue

        # fallback: if nothing matched, return all links so flow can decide
        return selected or [u for u, _ in links]
