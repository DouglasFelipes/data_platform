"""Scraper plugin for the FUNDEB / VAAT area of `gov.br`.

This plugin implements a small selection rule set to pick VAAT PDFs from
pages under the FUNDEB / VAAT area. It's intentionally small: only rules
needed to reliably pick VAAT PDFs are included. It returns a list of
URLs to download (strings).
"""

from __future__ import annotations

from typing import Iterable, List, Tuple
from urllib.parse import urlparse

from .base_scraper import BaseScraper


class FundebVaatScraper(BaseScraper):
    """Filter links for FUNDEB / VAAT pages.

    Heuristics used (in order):
    - file name contains "vaat" or "listadefinit" (case-insensitive)
    - link text contains "vaat" or "lista definitiva"
    - file appears to be a PDF and lives under a path containing "vaat"

    Falls back to returning only PDF links if nothing matched, then to
    returning all links as a last resort.
    """

    DEFAULT_HINTS = ("vaat", "listadefinit")

    def filter_links(self, links: Iterable[Tuple[str, str]]) -> List[str]:
        hints = [
            h.lower() for h in (self.params or {}).get("hints", self.DEFAULT_HINTS)
        ]
        selected: List[str] = []

        for u, text in links:
            try:
                path = urlparse(u).path or ""
                name = path.split("/")[-1].lower()
                text_lower = (text or "").lower()

                # direct hints in filename or link text
                if any(h in name for h in hints) or any(h in text_lower for h in hints):
                    selected.append(u)
                    continue

                # prefer PDFs under paths mentioning vaat
                if name.endswith(".pdf") and (
                    "/vaat" in path.lower() or "vaat/" in path.lower()
                ):
                    selected.append(u)
                    continue

            except Exception:
                continue

        if selected:
            return selected

        # fallback 1: return all PDFs
        pdfs = [
            u for u, _ in links if (urlparse(u).path or "").lower().endswith(".pdf")
        ]
        if pdfs:
            return pdfs

        # final fallback: return all links
        return [u for u, _ in links]

    def parse_filename(self, url: str) -> dict:
        """Return simple metadata extracted from a file URL.

        Returns a dict with keys: `filename`, `year` (if found), `is_pdf`.
        """
        from urllib.parse import urlparse

        path = urlparse(url).path or ""
        filename = path.split("/")[-1]
        is_pdf = filename.lower().endswith(".pdf")
        # naive year detection: first 4-digit group between 2000 and 2099
        import re

        year = None
        m = re.search(r"(20\d{2})", filename)
        if m:
            year = int(m.group(1))

        return {"filename": filename, "year": year, "is_pdf": is_pdf}

    def extract_metadata_from_link(
        self, url: str, link_text: str | None = None
    ) -> dict:
        """Combine filename parsing with link text to create metadata.

        Useful for downstream indexing and storage.
        """
        meta = self.parse_filename(url)
        meta.update({"link_text": (link_text or "").strip()})
        return meta
