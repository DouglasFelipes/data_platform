"""
Serviços para processar PDFs e enviar resultados para o armazenamento (GCS).

Explicação para leigos:

- Muitos PDFs contêm tabelas (por exemplo, planilhas). Este módulo abre o PDF,
    tenta extrair tabelas e converte cada tabela em um arquivo parquet (formato
    colunar eficiente usado em pipelines de dados).
- Antes de gerar parquets, fazemos upload do PDF original para um local de
    "staging" (para auditoria e reprocessamento). Depois, os parquets vão para
    um prefixo `raw` no bucket.
- Ferramentas usadas:
    - `pdfplumber`: biblioteca que entende o conteúdo do PDF e tenta extrair
        tabelas por página.
    - `pandas`: representa tabelas em memória (DataFrame).
    - `pyarrow`/`pandas.to_parquet`: escreve parquet no disco.
    - `google-cloud-storage` (via `GCSUploader`): envia arquivos para GCS.

Segurança e desempenho:
- Extração de tabelas pode falhar em PDFs mal formatados; nesse caso o código
    faz fallback e, se nenhuma tabela for encontrada, apenas sobe o PDF original
    sob `raw` para que alguém possa revisar manualmente.

Este arquivo fornece funções reutilizáveis para o fluxo principal.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime
from typing import List, Optional


def _ensure_dependencies():
    try:
        # import via importlib to avoid linter warnings about unused imports
        import importlib

        importlib.import_module("pandas")
        importlib.import_module("pdfplumber")
    except Exception as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "Missing dependencies for PDF processing. Install with: \n"
            "pip install pandas pdfplumber pyarrow"
        ) from exc


def extract_tables_from_pdf(path: str) -> List:
    _ensure_dependencies()
    import pandas as pd
    import pdfplumber

    # Abre o PDF e tenta extrair tabelas por página. O formato retornado por
    # pdfplumber é uma lista de linhas (cada linha é lista de células). Tentamos
    # interpretar a primeira linha como cabeçalho; se falhar, construímos o DataFrame
    # sem cabeçalho explícito.
    dfs: List = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            try:
                tables = page.extract_tables()
            except Exception:
                tables = None
            if not tables:
                continue
            for table in tables:
                # table is list[list[str]]; first row may be header
                try:
                    df = pd.DataFrame(table[1:], columns=table[0])
                except Exception:
                    df = pd.DataFrame(table)
                dfs.append(df)
    return dfs


def write_dfs_to_parquet(dfs: List, out_dir: str, base_name: str) -> List[str]:

    os.makedirs(out_dir, exist_ok=True)
    paths: List[str] = []
    for i, df in enumerate(dfs):
        fname = (
            f"{base_name}__table{i}.parquet" if len(dfs) > 1 else f"{base_name}.parquet"
        )
        out_path = os.path.join(out_dir, fname)
        # prefer pyarrow if available
        try:
            df.to_parquet(out_path, engine="pyarrow", index=False)
        except Exception:
            # fallback to default (may still require pyarrow)
            df.to_parquet(out_path, index=False)
        paths.append(out_path)
    return paths


def process_pdf_and_upload(
    info: dict,
    *,
    bucket: str,
    prefix: str,
    dataset_name: str | None = None,
    uploader=None,
    staging_prefix: str | None = None,
    remove_local_pdf: bool = True,
) -> List[str]:
    """Process a downloaded PDF and upload resulting parquet(s) to GCS.

    - `info` is the dict returned by the downloader (must contain `path` and `url`).
    - `bucket` is the GCS bucket name.
    - `prefix` is the prefix inside the bucket (e.g. `datalake/raw`).
        - `dataset_name` optional name for a subfolder under `prefix`.
            Ex: `fundeb` or `fnde`.
        - `uploader` optional GCSUploader instance (if not provided, one will be
            created).

    Returns list of `gs://` URIs uploaded.
    """
    _ensure_dependencies()
    # Caminho do PDF local (fornecido pelo downloader)
    pdf_path = info.get("path")
    if not pdf_path or not os.path.exists(pdf_path):
        # Se o PDF não estiver presente localmente, não temos como extrair tabelas
        # nem subir o arquivo. Paramos com erro para que o job fique visível.
        raise RuntimeError(f"PDF not found at {pdf_path}")

    # determine capture date
    captured = datetime.utcnow()
    capture_str = captured.strftime("%Y%m%d")
    year = captured.strftime("%Y")
    month = captured.strftime("%m")

    # try to infer year from filename if present
    # Tentamos extrair o ano do nome do arquivo (muitas vezes os relatórios
    # trazem o ano no nome). Isso ajuda a criar partições lógicas no destino.
    import re

    m = re.search(r"(20\d{2})", os.path.basename(pdf_path))
    if m:
        year = m.group(1)

    # Se o usuário informou um `dataset_name` (ex: 'fundeb'), incluímos esse
    # diretório dentro do prefixo (ex: 'datalake/raw/fundeb').
    if dataset_name:
        prefix = os.path.join(prefix, dataset_name)

    # compute staging prefix (where to upload original PDF)
    sp = staging_prefix or prefix
    if "/raw" in sp:
        sp = sp.replace("/raw", "/staging")
    elif sp.endswith("raw"):
        sp = sp[:-3] + "staging"
    else:
        sp = sp.rstrip("/") + "/staging"
    sp = sp.rstrip("/")

    # ensure uploader instance
    # Cria (ou usa) o uploader para o Google Cloud Storage.
    from data_platform.services.gcs import GCSUploader

    up = uploader or GCSUploader()

    # Primeiro fazemos upload do PDF original para o local de staging. Isso é
    # útil para auditoria: se a extração falhar, ainda teremos o PDF original
    # armazenado para inspeção humana.
    staging_parts = [
        sp,
        f"data_captura={capture_str}",
        f"year={year}",
        os.path.basename(pdf_path),
    ]
    staging_blob = os.path.join(*staging_parts)
    staging_blob = staging_blob.replace("\\", "/")
    staging_uri: Optional[str] = None
    try:
        staging_uri = up.upload_file(bucket, pdf_path, staging_blob)
    except Exception:
        # se o upload do staging falhar, é melhor parar e deixar o erro visível
        raise

    # where to write parquet files locally before upload
    # Criamos um diretório temporário para gravar os arquivos parquet antes do
    # upload. Usamos um diretório temporário para não poluir o workspace local.
    with tempfile.TemporaryDirectory() as tmp:
        dfs = extract_tables_from_pdf(pdf_path)
        uploaded: List[str] = []
        if dfs:
            # Se encontrarmos tabelas, convertemos cada uma em um parquet e
            # enviamos para o destino final (prefix/data_captura=.../year=.../month=...)
            base_name = os.path.splitext(os.path.basename(pdf_path))[0]
            out_paths = write_dfs_to_parquet(dfs, tmp, base_name)
            for p in out_paths:
                # cria um nome de blob com partições temporais para facilitar
                # downstream (ex.: partições por ano/mês)
                blob_name = os.path.join(
                    prefix,
                    f"data_captura={capture_str}",
                    f"year={year}",
                    f"month={month}",
                    os.path.basename(p),
                )
                blob_name = blob_name.replace("\\", "/")
                uri = up.upload_file(bucket, p, blob_name)
                uploaded.append(uri)
        else:
            # Se não houver tabelas detectadas, subimos o PDF original também
            # para o prefixo `raw` para que seja possível reprocessar manualmente.
            blob_name = os.path.join(
                prefix,
                f"data_captura={capture_str}",
                f"year={year}",
                os.path.basename(pdf_path),
            )
            blob_name = blob_name.replace("\\", "/")
            uri = up.upload_file(bucket, pdf_path, blob_name)
            uploaded.append(uri)

    # Retornamos primeiro a URI do PDF em staging (para facilitar auditoria) e
    # em seguida as URIs dos parquets (ou do PDF no raw se não houver tabelas).
    result = [staging_uri] + uploaded if staging_uri else uploaded

    # Opcional: remover o PDF local após o upload (padrão True). Isso evita
    # acumular arquivos no disco do executor. Se falhar, não interrompemos o
    # processamento — apenas mantemos o arquivo local.
    if remove_local_pdf:
        try:
            os.remove(pdf_path)
        except Exception:
            # não fatal
            pass

    return result
