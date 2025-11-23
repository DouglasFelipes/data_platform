"""HTTP fetcher with retries, timeout and optional UA rotation.

Provides a small `Fetcher` object exposing `get` and `stream_get`.
"""

from __future__ import annotations

import random
from typing import Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_UA_POOL = [
    "Mozilla/5.0 (compatible; DataPlatformBot/1.0; +https://example.org/bot)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
]


class Fetcher:
    """Small HTTP client with sensible defaults for scraping.

    Usage:
        f = Fetcher(timeout=15, retries=3)
        resp = f.get(url)
    """

    def __init__(
        self,
        timeout: int = 15,
        retries: int = 3,
        backoff_factor: float = 0.3,
        ua_pool: Optional[list[str]] = None,
    ) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        retry = Retry(
            total=retries,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
            backoff_factor=backoff_factor,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.ua_pool = ua_pool or DEFAULT_UA_POOL

    def _headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        base = {"User-Agent": random.choice(self.ua_pool)}
        if headers:
            base.update(headers)
        return base

    def get(self, url: str, headers: Optional[Dict[str, str]] = None, **kwargs):
        return self.session.get(
            url, headers=self._headers(headers), timeout=self.timeout, **kwargs
        )

    def stream_get(self, url: str, headers: Optional[Dict[str, str]] = None, **kwargs):
        # Streamed GET for downloading large files
        return self.session.get(
            url,
            headers=self._headers(headers),
            timeout=self.timeout,
            stream=True,
            **kwargs,
        )
