"""Detect resource type from URL/response headers/content.

Provides a small ResourceType enum and `detect_resource_type` helper.
"""

from __future__ import annotations

# Importa a classe Enum, que serve para criar uma lista de valores fixos, tipo um menu.
from enum import Enum

# Permite dizer que um parâmetro pode ser opcional (pode ser string ou pode ser “nada”).
from typing import Optional


# Isso cria uma lista de tipos possíveis de arquivos que o scraper pode encontrar.
class ResourceType(str, Enum):
    HTML = "html"
    PDF = "pdf"
    CSV = "csv"
    XLSX = "xlsx"
    JSON = "json"
    ZIP = "zip"
    API = "api"
    UNKNOWN = "unknown"


# Começa uma função que tenta descobrir a extensão do arquivo olhando pro texto da URL.
def _ext_from_url(url: str) -> Optional[str]:
    if ".pdf" in url.lower():
        return "pdf"
    if ".csv" in url.lower():
        return "csv"
    if ".xlsx" in url.lower() or ".xls" in url.lower():
        return "xlsx"
    if ".zip" in url.lower():
        return "zip"
    # Se não encontrou nenhuma extensão conhecida, devolve “nada”.
    return None


def detect_resource_type(url: str, content_type: Optional[str] = None) -> ResourceType:
    """Detect resource type by URL and optional Content-Type header.
    Heuristics: content_type preferred, then URL extension.
    Se o servidor disse o tipo de arquivo, usa essa informação primeiro.
    """
    if content_type:
        c = content_type.lower()
        if "text/html" in c:
            return ResourceType.HTML
        if "application/pdf" in c:
            return ResourceType.PDF
        if "text/csv" in c or "application/csv" in c:
            return ResourceType.CSV
        if "application/json" in c:
            return ResourceType.JSON
        if "zip" in c:
            return ResourceType.ZIP
        if "spreadsheet" in c or "excel" in c:
            return ResourceType.XLSX

    ext = _ext_from_url(url)
    if ext == "pdf":
        return ResourceType.PDF
    if ext == "csv":
        return ResourceType.CSV
    if ext == "xlsx":
        return ResourceType.XLSX
    if ext == "zip":
        return ResourceType.ZIP

    return ResourceType.UNKNOWN
