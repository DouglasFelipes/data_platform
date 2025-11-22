from __future__ import annotations

import datetime
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import requests  # type: ignore
from bs4 import BeautifulSoup

from data_platform.core.interfaces import BaseExtractor


class ScrapingExtractor(BaseExtractor):
    """Generic HTML scraping extractor base.

    Responsibilities:
    - fetch page HTML
    - extract candidate links by href or link text using flexible filters
    - return absolute URLs

    Subclasses should provide defaults for filters or override `find_files()`.
    """

    def fetch_html(self, timeout: int = 15) -> Optional[str]:
        try:
            resp = requests.get(self.url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception:
            return None

    def extract_links(
        self, html: str, patterns: Optional[List[str]] = None
    ) -> List[str]:
        """Extract links from HTML matching any of `patterns` (regexes).

        If `patterns` is None, returns all hrefs (absolute-normalized) found.
        """
        soup = BeautifulSoup(html, "html.parser")
        if patterns:
            compiled = []
            for p in patterns:
                compiled.append(re.compile(p, re.IGNORECASE))
        else:
            compiled = None

        found: List[str] = []
        for a in soup.find_all("a", href=True):
            raw = a["href"]
            href = str(raw).strip()
            if href.startswith("http"):
                full = href
            else:
                full = urljoin(self.url, href)
            if compiled:
                for rx in compiled:
                    if rx.search(full):
                        found.append(full)
                        break
            else:
                found.append(full)

        # dedupe preserving order
        seen = set()
        uniq: List[str] = []
        for u in found:
            if u not in seen:
                seen.add(u)
                uniq.append(u)

        return uniq

    def find_files(self) -> List[str]:
        """Generic find_files fallback.

        Returns absolute hrefs ending with common data file extensions
        (.pdf, .csv, .zip, .xlsx). Subclasses should override for
        targeted discovery.
        """
        html = self.fetch_html()
        if not html:
            return []

        patterns = [
            r"\.pdf$",
            r"\.csv$",
            r"\.zip$",
            r"\.xlsx?$",
        ]
        return self.extract_links(html, patterns=patterns)

    def download_and_save(self, url: str, dest_dir: str = "data") -> Optional[str]:
        """Download a URL and save it to `dest_dir/<YYYYMMDD>/filename`.

        Returns the saved path or None on failure.
        """
        try:
            parsed = urlparse(url)
            filename = Path(parsed.path).name
            if not filename:
                ts = int(datetime.datetime.utcnow().timestamp())
                filename = f"download-{ts}"
            date_folder = datetime.datetime.utcnow().strftime("%Y%m%d")
            out_dir = Path(dest_dir) / date_folder
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / filename
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(out_path, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            fh.write(chunk)
            return str(out_path)
        except Exception:
            return None
