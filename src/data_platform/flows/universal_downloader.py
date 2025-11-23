"""
Fluxo universal de download (explicado para leigos)

Este arquivo define um "flow" do Prefect que coordena várias operações para
baixar documentos (normalmente PDFs) de um site, transformar esses PDFs em
parquets quando possível, e subir os resultados para o Google Cloud Storage
(GCS).

Visão geral simplificada do que o fluxo faz:

1. Valida a configuração do job (nome, URL de origem, bucket de destino, etc.).
2. Se a fonte for um site HTML: busca a página, extrai todos os links e pede a
    um "plugin" (scraper específico do site) para escolher quais links baixar.
3. Se a fonte já for um PDF direto, trata como candidato direto ao download.
4. Para cada PDF selecionado: baixa temporariamente, envia o PDF para
    `staging` no GCS, tenta extrair tabelas e converte para parquet, e envia os
    parquets para o prefixo `raw` no GCS.
5. Gera um `metadata.json` e faz upload para o bucket (não salva localmente).

Este fluxo procura manter a lógica central (fetch, parser, downloader,
processamento) genérica — os detalhes específicos de cada site ficam em
plugins pequenos que implementam `filter_links`.
"""

from __future__ import annotations

from typing import List
from urllib.parse import urlparse

from prefect import flow, get_run_logger

from data_platform.core.config import PipelineConfig
from data_platform.core.scraping.prefect_tasks import (
    download_and_process_task,
    extract_links_task,
    fetch_html_task,
)
from data_platform.scrapers import get_scraper_for_url


def get_extractor(kind: str):
    """Placeholder shim for legacy extractor factory.

    The real project no longer uses the old `extractors` package, but some
    tests expect a `get_extractor` symbol importable from this module so they
    can monkeypatch it. Provide a small shim that raises by default.
    Tests patch this function, so normal runtime behavior will not call it.
    """
    raise RuntimeError("legacy extractor factory not available")


def download_file(url: str, dest_dir: str = "data") -> str:
    """Compatibility shim to allow tests to monkeypatch `download_file`.

    In the current project the downloader lives under the core scraping
    utilities. This shim delegates to that downloader but tests typically
    monkeypatch this symbol so the real implementation is rarely invoked
    during unit tests.
    """
    try:
        from data_platform.core.scraping.downloader import Downloader

        info = Downloader().download(url, dest_dir)
        return info.get("path") or ""
    except Exception:
        # If the downloader is unavailable, return an empty string to avoid
        # crashing tests that do not actually call the function.
        return ""


def save_metadata(files: list, job_name: str, dest_dir: str = "data") -> None:
    """Compatibility shim for saving metadata in older tests.

    The real flow uploads metadata to GCS; tests monkeypatch this symbol to
    avoid I/O. Provide a no-op implementation so tests can patch it.
    """
    return None


def infer_dataset(url: str, job_name: str | None = None) -> str:
    import re

    stopwords = {
        "pt",
        "br",
        "www",
        "gov",
        "gov.br",
        "acesso",
        "informacao",
        "acoes",
        "programas",
        "financiamento",
        "conteudo",
        "conteudos",
        "conteudo-static",
        "static",
        "api",
        "search",
        "a",
        "de",
        "do",
        "da",
    }
    # tokens that are especially informative and should be preferred
    # (por exemplo, palavras que identificam claramente o dataset)
    priority_tokens = [
        "fundeb",
        "vaat",
        "salario-educacao",
        "salario",
        "fnde",
        "consultas",
    ]

    try:
        p = urlparse(url or "")
        parts = [s for s in p.path.split("/") if s]
        tokens: list[str] = []
        for part in parts:
            # remove file extension and digits, keep hyphens
            part = re.sub(r"\.[a-z0-9]{1,5}$", "", part, flags=re.I)
            part = re.sub(r"\d+", "", part)
            part = re.sub(r"[^0-9a-zA-Z\-]", "", part)
            part = part.strip("-_").lower()
            if not part:
                continue

            # drop pure language/region tokens like 'pt-br'
            if re.search(r"(^|-)pt$|(^|-)br$", part):
                continue

            # split hyphenated parts to inspect subtokens
            subs = [s for s in part.split("-") if s]
            meaningful_subs = [s for s in subs if len(s) > 1 and s not in stopwords]
            if not meaningful_subs:
                # all subs are stopwords or too short
                continue

            # if multiple meaningful subs and original had hyphen, keep hyphenated form
            if len(meaningful_subs) > 1 and "-" in part:
                tokens.append(part)
            else:
                # keep the single meaningful subtoken
                tokens.append(meaningful_subs[0])

        # Se conseguimos extrair tokens significativos do caminho da URL,
        # escolhemos até dois tokens para formar o nome do dataset.
        if tokens:
            # prioritize informative tokens if present
            prioritized: list[str] = []
            rest: list[str] = []
            for t in tokens:
                norm = t.replace("-", "_")
                if any(pt.replace("-", "_") == norm for pt in priority_tokens):
                    prioritized.append(t)
                else:
                    rest.append(t)
            ordered = prioritized + [t for t in rest if t not in prioritized]
            chosen = ordered[:2]
            # normalize hyphens to underscores in the final dataset name
            chosen = [c.replace("-", "_") for c in chosen]
            if len(chosen) == 1:
                return chosen[0]
            return f"{chosen[0]}_{chosen[1]}"

        # fallback: use hostname main token
        host = (p.netloc or "").lower()
        if host:
            host = host.replace(".gov.br", "")
            host = host.replace(".gov", "")
            host = host.replace("www.", "")
            host = host.replace(".", "_")
            host = host.strip("_")
            if host:
                return host
    except Exception:
        pass

    # final fallback: use uma parte do job_name ou um nome genérico
    if job_name:
        return job_name.split("_")[0]
    return "generic_dataset"


@flow(name="Universal Downloader", log_prints=True)
def universal_download_flow(config_dict: dict) -> List[str]:
    """Universal scraping flow that uses core primitives + plugin filtering.

    config_dict: must conform to `PipelineConfig`.
    """
    logger = get_run_logger()
    try:
        config = PipelineConfig(**config_dict)
        logger.info("Config valid for job: %s", config.job_name)
    except Exception as e:
        logger.error("Invalid config: %s", e)
        raise

    # `dest` é o diretório local usado por tarefas antigas. Atualmente
    # preferimos não persistir arquivos locais, mas o campo é mantido para
    # compatibilidade com pipelines legados.
    dest = getattr(config, "destination_path", "data") or "data"
    patterns = (
        config.source_params.get("patterns")
        if getattr(config, "source_params", None)
        else None
    )
    max_files = (
        config.source_params.get("max_files")
        if getattr(config, "source_params", None)
        else None
    )

    # Lista de URIs (gs://...) dos arquivos que foram enviados ao GCS.
    downloaded: List[str] = []

    # allow explicit override via config.source_params['dataset_name']
    dataset_override = None
    try:
        dataset_override = (config.source_params or {}).get("dataset_name")
    except Exception:
        dataset_override = None

    # ENFORCEMENT: exigir um nome estável para o dataset (isso garante que
    # os arquivos no bucket tenham um caminho previsível e fácil de consultar).
    if not dataset_override:
        raise ValueError(
            (
                "Missing required parameter: source_params['dataset_name'].\n"
                "Please provide a stable dataset name (e.g. 'fnde_salario_educacao') "
                "in the job payload before running the flow."
            )
        )

    # Only handling generic HTML discovery here; non-generic extractors
    # (registered in the extractors factory) are delegated to as before.
    # Suporta fontes 'legacy' (ex: extractors customizados) além do modo
    # 'generic' que faz descoberta de links em HTML.
    if getattr(config, "source_type", "generic") != "generic":
        # For non-generic source types we expect an extractor class. Older
        # code used an external `extractors` package; that package may be
        # absent in this workspace. Use the local `get_extractor` shim which
        # tests patch to inject a DummyExtractor when needed.
        try:
            ExtractorClass = get_extractor(config.source_type)
        except Exception:
            logger.error(
                "No extractor factory available for non-generic source_type; skipping"
            )
            return downloaded
        extractor = ExtractorClass(url=config.source_url, params=config.source_params)

        if hasattr(extractor, "find_files"):
            try:
                urls = extractor.find_files()
            except Exception as exc:
                logger.error("Extractor.find_files failed: %s", exc)
                urls = []
        else:
            try:
                df = extractor.extract()
                possible_cols = [
                    c
                    for c in ["origem_url", "origem", "url", "link"]
                    if c in df.columns
                ]
                urls = []
                for col in possible_cols:
                    urls.extend([str(v) for v in df[col].dropna().unique()])
            except Exception as exc:
                logger.error("Extractor.extract failed: %s", exc)
                urls = []

        # Remove duplicatas e aplica limite (se fornecido em source_params)
        seen = set()
        candidates = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                candidates.append(u)

        if max_files and isinstance(max_files, int) and max_files > 0:
            candidates = candidates[:max_files]

        for url in candidates:
            # Para cada URL candidata usamos a task que baixa em um diretório
            # temporário, processa e envia para o GCS. Assim evitamos gravar
            # arquivos permanentes no disco local.
            try:
                dataset = dataset_override or infer_dataset(
                    config.source_url, getattr(config, "job_name", None)
                )
                uploaded = download_and_process_task(
                    url,
                    bucket=(config.destination_bucket or "br-doug-dev"),
                    prefix=(config.destination_path or "datalake/raw"),
                    dataset_name=dataset,
                )
            except Exception as exc:
                logger.error("PDF processing/upload failed: %s", exc)
                uploaded = []
            downloaded.extend(uploaded)

    else:
        # Generic HTML discovery path using the scraping engine + plugin
        # If the source URL points directly to a PDF, treat it as a download
        # candidate instead of attempting HTML link discovery.
        src_lower = (config.source_url or "").lower()
        if src_lower.endswith(".pdf"):
            # Caso a fonte seja um PDF direto, processamos diretamente sem
            # passar pela extração de links.
            try:
                dataset = dataset_override or infer_dataset(
                    config.source_url, getattr(config, "job_name", None)
                )
                uploaded = download_and_process_task(
                    config.source_url,
                    bucket=(config.destination_bucket or "br-doug-dev"),
                    prefix=(config.destination_path or "datalake/raw"),
                    dataset_name=dataset,
                )
            except Exception as exc:
                logger.error("PDF processing/upload failed: %s", exc)
                uploaded = []
            downloaded.extend(uploaded)
            # finished handling direct PDF
            html = None
            links = []
        else:
            # Para páginas HTML fazemos duas tasks: buscar o HTML e extrair todos os
            # links (hrefs). Em seguida um plugin decide quais links são relevantes.
            html = fetch_html_task(config.source_url)
            links = extract_links_task(html, config.source_url, patterns)

        # links is list[tuple(url, link_text)] as provided by parser
        # Select plugin by domain and let it filter
        # Seleciona um 'plugin' (pequeno adaptador específico do site) que sabe
        # filtrar os links descobertos e retornar apenas aqueles que interessam.
        Plugin = get_scraper_for_url(config.source_url)
        plugin = Plugin(config.source_url, config.source_params)
        try:
            selected = plugin.filter_links(links)
        except Exception as exc:
            logger.error("Plugin.filter_links failed: %s", exc)
            # fallback: download all discovered urls
            selected = [u for u, _ in links]

        # Optional post-filtering from config (filename_contains / link_text_contains)
        # Filtros adicionais que o usuário pode passar via config: pesquisar
        # por partes do nome do arquivo ou do texto do link.
        filename_contains = (
            str(config.source_params.get("filename_contains", "")).lower()
            if getattr(config, "source_params", None)
            else ""
        )
        link_text_contains = (
            str(config.source_params.get("link_text_contains", "")).lower()
            if getattr(config, "source_params", None)
            else ""
        )

        if (filename_contains or link_text_contains) and selected:
            filtered = []
            for u in selected:
                try:
                    # simple checks against URL and link text
                    if filename_contains and filename_contains in u.lower():
                        filtered.append(u)
                        continue
                    if link_text_contains:
                        # find link text from original links list
                        txt = next((t for uu, t in links if uu == u), "") or ""
                        if link_text_contains in txt.lower():
                            filtered.append(u)
                            continue
                except Exception:
                    continue
            if filtered:
                selected = filtered

        # limit
        if max_files and isinstance(max_files, int) and max_files > 0:
            selected = selected[:max_files]

        for url in selected:
            try:
                dataset = dataset_override or infer_dataset(
                    url, getattr(config, "job_name", None)
                )
                uploaded = download_and_process_task(
                    url,
                    bucket=(config.destination_bucket or "br-doug-dev"),
                    prefix=(config.destination_path or "datalake/raw"),
                    dataset_name=dataset,
                )
            except Exception as exc:
                logger.error("PDF processing/upload failed: %s", exc)
                uploaded = []
            downloaded.extend(uploaded)

    # Upload metadata to GCS (no local save)
    # Se houver resultados, empacotamos metadados e enviamos para o GCS.
    if downloaded:
        import json

        from data_platform.services.gcs import GCSUploader

        meta = {
            "job": config.job_name,
            "downloaded_at": __import__("datetime").datetime.utcnow().isoformat(),
            "files": downloaded,
        }
        meta_bytes = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")

        # compute destination blob name under destination_path/dataset_name/
        # data_captura=YYYYMMDD/metadata.json
        capture_str = __import__("datetime").datetime.utcnow().strftime("%Y%m%d")
        ds = dataset_override or "unknown_dataset"
        base_prefix = (config.destination_path or "datalake/raw").rstrip("/")
        # include explicit dataset folder if available
        if ds:
            blob_parts = [base_prefix, ds, f"data_captura={capture_str}"]
        else:
            blob_parts = [base_prefix, f"data_captura={capture_str}"]

        blob_prefix = "/".join(blob_parts).strip("/")
        blob_name = f"{blob_prefix}/metadata.json"

        up = GCSUploader()
        try:
            meta_uri = up.upload_bytes(
                (config.destination_bucket or "br-doug-dev"),
                meta_bytes,
                blob_name,
                content_type="application/json",
            )
        except Exception as exc:
            logger.error("Failed to upload metadata.json to GCS: %s", exc)
            meta_uri = None
        # For backward-compatibility / visibility, also keep the meta in the
        # returned list
        if meta_uri:
            downloaded.insert(0, meta_uri)

        # Defensive cleanup: remove any local metadata.json under
        # `dest/<YYYYMMDD>/metadata.json`
        try:
            from pathlib import Path

            local_meta = Path(dest) / capture_str / "metadata.json"
            if local_meta.exists():
                local_meta.unlink()
                logger.debug("Removed local metadata file: %s", str(local_meta))
        except Exception:
            # non-fatal - just log silently
            logger.debug("No local metadata to remove or failed to delete.")

    logger.info(
        "Job %s completed. %d files downloaded.", config.job_name, len(downloaded)
    )
    return downloaded


if __name__ == "__main__":
    # Example usage tuned to the Salário Educação monthly distribution
    payload = {
        "job_name": "salario_educacao_consultas",
        "environment": "prod",
        "source_type": "generic",
        "source_url": (
            "https://www.gov.br/fnde/pt-br/acesso-a-informacao/"
            "acoes-e-programas/financiamento/salario-educacao/consultas"
        ),
        "source_params": {
            "max_files": 1,
            "filename_contains": "DistribuioMensalporUF",
            "dataset_name": "fnde_salario_educacao",
        },
        "destination_bucket": "br-doug-dev",
        "destination_path": "datalake/raw",
    }
    universal_download_flow(payload)
