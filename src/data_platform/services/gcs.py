"""
Simple GCS helper utilities.

Este módulo fornece utilitários simples para enviar arquivos ao
Google Cloud Storage (GCS). Ele usa apenas uma parte pequena
da biblioteca oficial `google.cloud.storage`, mas com mensagens
de erro mais claras caso haja problemas de dependência ou credenciais.
"""

from __future__ import annotations

import io
from typing import Optional

# Permite usar hints de tipos antes de as classes existirem,
# deixando o código mais moderno e compatível.


# Optional permite que parâmetros aceitem valores ou None.


# io é usado para criar "arquivos virtuais" em memória usando bytes.


class GCSUploader:
    """
    Classe responsável por fazer upload para o Google Cloud Storage.

    Ela encapsula a lógica da biblioteca oficial, deixando o resto
    do pipeline mais simples e com menos repetição.
    """

    def __init__(self, project: Optional[str] = None):
        """
        Inicializa o cliente de conexão com o GCS.

        - project: opcionalmente define qual projeto GCP usar.
        """
        try:
            from google.cloud import storage

            # Tenta importar a biblioteca oficial do GCP.
            # Se ela não estiver instalada, cai no except.
        except Exception as exc:
            # Se não conseguir importar, lança erro amigável explicando
            # exatamente o que instalar.
            raise RuntimeError(
                "google-cloud-storage is required to upload to GCS. "
                "Install it with `pip install google-cloud-storage`"
            ) from exc

        # Cria o cliente GCS.
        # É ele que permite acessar buckets, blobs e fazer upload.
        self.client = storage.Client(project=project)

    def upload_file(
        self, bucket_name: str, source_file_path: str, dest_blob_name: str
    ) -> str:
        """
        Envia um arquivo local para o bucket do GCS.

        - bucket_name: nome do bucket (ex: "meu-bucket").
        - source_file_path: caminho local do arquivo.
        - dest_blob_name: caminho destino dentro do bucket.

        Retorna a URI final "gs://bucket/arquivo".
        """

        # Seleciona o bucket desejado.
        bucket = self.client.bucket(bucket_name)

        # Define o "blob" (arquivo destino) com o nome desejado no bucket.
        blob = bucket.blob(dest_blob_name)

        try:
            # Envia o arquivo do disco para o GCS.
            blob.upload_from_filename(source_file_path)
        except Exception as exc:
            # Se der erro (credenciais, permissão, caminho errado etc.),
            # gera uma mensagem clara para facilitar o debug.
            msg = (
                f"Failed to upload {source_file_path} to "
                f"gs://{bucket_name}/{dest_blob_name}: {exc}"
            )
            raise RuntimeError(msg)

        # Retorna a URI de onde o arquivo foi salvo no GCS.
        return f"gs://{bucket_name}/{dest_blob_name}"

    def upload_bytes(
        self,
        bucket_name: str,
        data: bytes,
        dest_blob_name: str,
        content_type: Optional[str] = None,
    ) -> str:
        """
        Faz upload de dados armazenados como bytes em memória.

        Ideal para:
        - PDFs gerados dinamicamente
        - CSV criados em runtime
        - logs
        - HTML raspado
        - arquivos que não precisam ser salvos localmente

        - data: bytes do arquivo
        - content_type: ex: "application/pdf"
        """

        # Seleciona o bucket.
        bucket = self.client.bucket(bucket_name)

        # Define o blob destino.
        blob = bucket.blob(dest_blob_name)

        try:
            # Envia os bytes usando um "arquivo virtual" em memória.
            blob.upload_from_file(io.BytesIO(data), content_type=content_type)
        except Exception as exc:
            # Mensagem clara de erro em caso de falha.
            msg = (
                f"Failed to upload bytes to gs://{bucket_name}/{dest_blob_name}: {exc}"
            )
            raise RuntimeError(msg)

        # Retorna a URI final.
        return f"gs://{bucket_name}/{dest_blob_name}"
