import unicodedata
from typing import Any, List, Optional
from urllib.parse import urljoin

import fitz  # PyMuPDF
import pandas as pd
import requests  # type: ignore
from bs4 import BeautifulSoup

from data_platform.core.interfaces import BaseExtractor


class PdfExtractor(BaseExtractor):
    """Generic PDF extractor base.

    This class contains generic PDF download and parsing helpers. It does
    not implement site-specific scraping logic. Subclasses should implement
    `find_files()` or pass a `pdf_url` in `params` to locate the desired PDF.
    """

    def extract(self) -> pd.DataFrame:
        print(f"🔎 [PDF] Iniciando processamento na URL/base: {self.url}")

        # 1. Determine the PDF URL to process.
        pdf_url: Optional[str] = None
        # Priority: explicit `pdf_url` in params
        if isinstance(self.params, dict) and "pdf_url" in self.params:
            pdf_url = str(self.params["pdf_url"])
        # Next: subclasses can implement `find_files()` to discover candidate PDFs
        elif hasattr(self, "find_files"):
            urls = self.find_files()
            pdf_url = urls[0] if urls else None

        if not pdf_url:
            print("⚠️ Nenhum PDF encontrado ou fornecido nos parâmetros.")
            return pd.DataFrame()

        # 2. Baixa o PDF em memória
        pdf_content = self._download_pdf(pdf_url)

        # 3. Processa o PDF (Parsing)
        df = self._parse_pdf(pdf_content)

        # Adiciona metadados úteis
        if not df.empty:
            # Keep any provided 'ano' param as metadata if present
            if isinstance(self.params, dict) and "ano" in self.params:
                df["ano_referencia"] = self.params.get("ano")
            df["origem_url"] = pdf_url

        return df

    # --- Métodos Privados (Helpers da Classe) ---

    def _download_pdf(self, url: str) -> bytes:
        """Baixa o binário do PDF."""
        print("   ⬇️ Baixando PDF...")
        if url.endswith("/view"):
            url = url.replace("/view", "")
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.content

    def _parse_pdf(self, pdf_bytes: bytes) -> pd.DataFrame:
        """Lógica do PyMuPDF (Fitz)."""
        print("   📄 Processando PDF (OCR/Text)...")
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        all_data = []
        header = None

        for i, page in enumerate(doc):
            # Usando sua lógica de extração de tabelas
            tables = page.find_tables()
            if tables and tables.tables:
                rows = tables.tables[0].extract()
                if i == 0:
                    header = self._norm_row(rows[0])
                    data_rows = rows[1:]
                else:
                    data_rows = rows

                all_data.extend(data_rows)

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame(all_data)
        if header:
            if len(header) == df.shape[1]:
                df.columns = header

        return df

    def _norm_text(self, s: Any) -> str:
        if not isinstance(s, str):
            return str(s) if s else ""
        s = unicodedata.normalize("NFD", s)
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
        return s.lower().strip()

    def _norm_row(self, row: List[Any]) -> List[str]:
        return [self._norm_text(c) for c in row]

    def find_files(self) -> List[str]:
        """Find candidate file URLs on the page using generic heuristics.

        This is a general-purpose fallback. Specific extractors can override
        or rely on `self.params` to narrow results.
        """
        try:
            resp = requests.get(self.url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            candidates: List[str] = []
            for a in soup.find_all("a", href=True):
                raw = a["href"]
                href = str(raw).strip()
                href_l = href.lower()
                if href_l.endswith(".pdf"):
                    full = href if href.startswith("http") else urljoin(self.url, href)
                    candidates.append(full)

            seen = set()
            uniq: List[str] = []
            for u in candidates:
                if u not in seen:
                    seen.add(u)
                    uniq.append(u)

            return uniq
        except Exception:
            return []
