"""Tarefas Prefect que usam os componentes de scraping.

Este arquivo adapta funções baixadas "baixas" (fetcher, parser, downloader)
para o modelo de execução do Prefect. Prefect organiza trabalho em "tasks"
e "flows" — cada task aqui é uma unidade de trabalho com tentativas (retries)
configuradas e logs.

Para quem não conhece Prefect:
- Um "task" é uma função executada por Prefect; ela pode ser reexecutada se
    falhar (retries) e tem logs/estado. Um "flow" compõe várias tasks em sequência.

As funções abaixo são pequenas adaptações que chamam os componentes do
core scraping (Buscar HTML, Extrair links, Baixar arquivo) e adicionam logs
e retry automático.
"""

from __future__ import annotations

import tempfile
from typing import List, Optional, Tuple

from prefect import get_run_logger, task

from data_platform.core.scraping.downloader import Downloader
from data_platform.core.scraping.fetcher import Fetcher
from data_platform.core.scraping.parser import extract_links_from_html
from data_platform.services.pdf_processor import process_pdf_and_upload


@task(name="fetch_html", retries=2, retry_delay_seconds=3)
def fetch_html_task(url: str, timeout: int = 15) -> str:
    logger = get_run_logger()
    logger.info("Fetching URL: %s", url)
    f = Fetcher(timeout=timeout)
    resp = f.get(url)
    resp.raise_for_status()
    logger.info("Fetched %s (status=%s)", url, resp.status_code)
    return resp.text


@task(name="extract_links", retries=0)
def extract_links_task(
    html: str, base_url: str, patterns: Optional[List[str]] = None
) -> List[Tuple[str, str]]:
    logger = get_run_logger()
    links = extract_links_from_html(html, base_url, patterns)
    logger.info("Extracted %d links from %s", len(links), base_url)
    return links


@task(name="download_file", retries=2, retry_delay_seconds=5)
def download_file_task(file_url: str, dest_dir: str = "data") -> str:
    logger = get_run_logger()
    d = Downloader()
    info = d.download(file_url, dest_dir)
    logger.info("Saved file %s (size=%s bytes)", info.get("path"), info.get("size"))
    return info.get("path") or ""


@task(name="stream_download", retries=2, retry_delay_seconds=5)
def stream_download_task(file_url: str, dest_dir: str = "data") -> dict:
    logger = get_run_logger()
    d = Downloader()
    info = d.download(file_url, dest_dir)
    logger.info("Downloaded %s sha256=%s", info.get("path"), info.get("sha256"))
    return info


@task(name="download_and_process", retries=1, retry_delay_seconds=3)
def download_and_process_task(
    file_url: str,
    *,
    bucket: str,
    prefix: str,
    dataset_name: str | None = None,
    uploader=None,
    staging_prefix: str | None = None,
    remove_local_pdf: bool = True,
) -> list:
    """Download file into a temporary directory, process PDF and upload, then cleanup.

    This task avoids persisting the downloaded file under the project's data folders.
    """
    logger = get_run_logger()
    d = Downloader()
    tmp = tempfile.TemporaryDirectory()
    try:
        info = d.download(file_url, tmp.name)
        logger.info(
            "Downloaded temp %s sha256=%s",
            info.get("path"),
            info.get("sha256"),
        )
        # Process and upload. The processor handles staging and parquet upload.
        # We will cleanup the temporary directory ourselves, so do not remove
        # the local PDF inside the processor.
        uploaded = process_pdf_and_upload(
            info,
            bucket=bucket,
            prefix=prefix,
            dataset_name=dataset_name,
            uploader=uploader,
            staging_prefix=staging_prefix,
            remove_local_pdf=False,
        )
    finally:
        try:
            tmp.cleanup()
        except Exception:
            pass
    return uploaded
