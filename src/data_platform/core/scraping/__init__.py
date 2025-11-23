"""Core scraping primitives exported for reuse across extractors and flows.

This package contains small, well-tested building blocks: Fetcher, Detector,
Parser, Normalizer and Downloader, plus Prefect task wrappers.
"""

from .detector import ResourceType, detect_resource_type
from .downloader import Downloader
from .fetcher import Fetcher
from .normalizer import normalize_url
from .parser import extract_links_from_html
from .prefect_tasks import (
    download_file_task,
    extract_links_task,
    fetch_html_task,
    stream_download_task,
)

__all__ = [
    "Fetcher",
    "detect_resource_type",
    "ResourceType",
    "extract_links_from_html",
    "normalize_url",
    "Downloader",
    "fetch_html_task",
    "extract_links_task",
    "download_file_task",
    "stream_download_task",
]
