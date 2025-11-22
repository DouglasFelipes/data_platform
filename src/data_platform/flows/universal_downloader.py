"""Universal downloader flow (Prefect 3)

This single-file flow provides a generic, reusable Prefect flow and tasks
that can be used to crawl a single page (or starting URL), find links to
open-data files (PDF/CSV/XLSX/ZIP) and download them to a local destination.

Design goals:
- Work with arbitrary sites, starting with gov.br portals
- Small tasks with retries and clear logging (Prefect best practices)
- Use a generic base that can accept any URL and filter/patterns
- Provide local saving but keep storage abstraction small for easy extension

Usage:
 - Import and call `universal_download_flow(config_dict)` from other flows
 - Or run as script: examples for FNDE pages are included in `__main__`
"""

from __future__ import annotations

import datetime
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import requests  # type: ignore
from bs4 import BeautifulSoup
from prefect import flow, get_run_logger, task

from data_platform.core.config import PipelineConfig


@task(name="fetch_html", retries=2, retry_delay_seconds=3)
def fetch_html(url: str, timeout: int = 15) -> str:
    """Fetch HTML content from a URL with a simple requests wrapper."""
    logger = get_run_logger()
    logger.info("Fetching URL: %s", url)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


@task(name="extract_links")
def extract_links(
    html: str, base_url: str, patterns: Optional[List[str]] = None
) -> List[str]:
    """Extract candidate links from HTML.

    - `patterns`: optional list of regex patterns to match against the href.
    - If not provided, uses a default set that matches common data file extensions.
    """
    logger = get_run_logger()
    soup = BeautifulSoup(html, "html.parser")

    # Default patterns: pdf, csv, xlsx, zip (case-insensitive)
    default = [r"\.pdf$", r"\.csv$", r"\.xlsx$", r"\.xls$", r"\.zip$"]
    patterns = patterns or default
    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]

    candidates: List[str] = []
    for a in soup.find_all("a", href=True):
        raw = a["href"]
        href = str(raw).strip()
        # Normalize relative urls
        full = urljoin(base_url, href)
        # Check against patterns
        for rx in compiled:
            if rx.search(full):
                candidates.append(full)
                logger.debug("Matched link: %s", full)
                break

    # Deduplicate while preserving order
    seen = set()
    uniq: List[str] = []
    for u in candidates:
        if u not in seen:
            seen.add(u)
            uniq.append(u)

    logger.info("Found %d candidate files", len(uniq))
    return uniq


@task(name="download_file", retries=2, retry_delay_seconds=5)
def download_file(file_url: str, dest_dir: str = "data") -> str:
    """Download a file and save it under `dest_dir/<date>/filename`.

    Returns the saved file path.
    """
    logger = get_run_logger()
    logger.info("Downloading: %s", file_url)

    parsed = urlparse(file_url)
    filename = (
        Path(parsed.path).name
        or f"download-{int(datetime.datetime.utcnow().timestamp())}"
    )

    date_folder = datetime.datetime.utcnow().strftime("%Y%m%d")
    out_dir = Path(dest_dir) / date_folder
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / filename

    with requests.get(file_url, stream=True, timeout=30) as r:
        r.raise_for_status()
        with open(out_path, "wb") as fh:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)

    logger.info("Saved file to %s", str(out_path))
    return str(out_path)


@task(name="save_metadata")
def save_metadata(files: List[str], job_name: str, dest_dir: str = "data") -> None:
    """Save a small metadata file describing the downloaded files.

    This is a simple local PoC. In production, write to a DB or object storage.
    """
    logger = get_run_logger()
    meta = {
        "job": job_name,
        "downloaded_at": datetime.datetime.utcnow().isoformat(),
        "files": files,
    }
    meta_path = (
        Path(dest_dir) / datetime.datetime.utcnow().strftime("%Y%m%d") / "metadata.json"
    )
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    import json

    with open(meta_path, "w", encoding="utf8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)

    logger.info("Saved metadata to %s", str(meta_path))


@flow(name="Universal Downloader", log_prints=True)
def universal_download_flow(config_dict: dict) -> List[str]:
    """Main flow: validates `config_dict` using `PipelineConfig` (Pydantic) and
    runs the download pipeline.

    Expected keys (via PipelineConfig in this project): at minimum provide
    - job_name
    - source_url
    - destination_path (optional; default 'data')
    - patterns (optional list of regex strings)
    - max_files (optional int)
    """
    logger = get_run_logger()
    try:
        config = PipelineConfig(**config_dict)
        logger.info("Config valid for job: %s", config.job_name)
    except Exception as e:
        logger.error("Invalid config: %s", e)
        raise

    base_url = config.source_url
    dest = getattr(config, "destination_path", "data") or "data"
    patterns: Optional[List[str]] = (
        config.source_params.get("patterns")
        if getattr(config, "source_params", None)
        else None
    )
    max_files = (
        config.source_params.get("max_files")
        if getattr(config, "source_params", None)
        else None
    )

    html = fetch_html(base_url)
    candidates = extract_links(html, base_url, patterns)

    # Optionally limit number of files
    if max_files and isinstance(max_files, int) and max_files > 0:
        candidates = candidates[:max_files]

    downloaded: List[str] = []
    for url in candidates:
        path = download_file(url, dest)
        downloaded.append(path)

    if downloaded:
        save_metadata(downloaded, config.job_name, dest)

    logger.info(
        "Job %s completed. %d files downloaded.", config.job_name, len(downloaded)
    )
    return downloaded


if __name__ == "__main__":
    # Example 1: FUNDEB 2026 page (POC)
    payload_fundeb = {
        "job_name": "fundeb_2026_examples",
        "environment": "dev",
        "source_type": "generic",
        "source_url": (
            "https://www.gov.br/fnde/pt-br/acesso-a-informacao/"
            "acoes-e-programas/financiamento/fundeb/2026"
        ),
        "source_params": {"max_files": 3},
        "destination_bucket": "local",
        "destination_path": "data",
    }

    # Example 2: Salário Educação consultas
    payload_salario = {
        "job_name": "salario_educacao_consultas",
        "environment": "dev",
        "source_type": "generic",
        "source_url": (
            "https://www.gov.br/fnde/pt-br/acesso-a-informacao/"
            "acoes-e-programas/financiamento/salario-educacao/consultas"
        ),
        "source_params": {"max_files": 3},
        "destination_bucket": "local",
        "destination_path": "data",
    }

    print("Running example FNDE download (fundeb)")
    universal_download_flow(payload_fundeb)
