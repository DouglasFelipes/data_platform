import unicodedata
from datetime import datetime
from typing import Any, List, Optional

import fitz  # PyMuPDF
import pandas as pd
import requests  # type: ignore
from bs4 import BeautifulSoup

from data_platform.core.interfaces import BaseExtractor


class FndePdfExtractor(BaseExtractor):
    """
    Estratégia concreta para extrair dados de PDF do FNDE.
    """

    def extract(self) -> pd.DataFrame:
        print(f"🔎 [FNDE] Iniciando estratégia na URL: {self.url}")

        # 1. Recupera parâmetros
        ano = self.params.get("ano", datetime.now().year)
        meses_alvo = self.params.get("meses_alvo", [])

        # 2. Faz o Scraping para achar o link do PDF
        pdf_url = self._scrape_link(ano, meses_alvo)

        if not pdf_url:
            print("⚠️ Nenhum PDF encontrado para os parâmetros informados.")
            return pd.DataFrame()

        # 3. Baixa o PDF em memória
        pdf_content = self._download_pdf(pdf_url)

        # 4. Processa o PDF (Parsing)
        df = self._parse_pdf(pdf_content)

        # Adiciona metadados úteis
        if not df.empty:
            df["ano_referencia"] = ano
            df["origem_url"] = pdf_url

        return df

    # --- Métodos Privados (Helpers da Classe) ---

    def _scrape_link(self, ano: int, meses_alvo: list) -> Optional[str]:
        """Varre o site do Gov.br e retorna o link do PDF mais recente."""
        print("   🕵️ Scraping buscando dados de {}...".format(ano))
        try:
            resp = requests.get(self.url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # (Sua lógica simplificada de scraping aqui)
            # Procura blocos com o ano e links de distribuição
            for bloco in soup.find_all("strong"):
                if bloco.get_text(strip=True) == str(ano):
                    ul = bloco.find_next("ul")
                    if ul:
                        for a in ul.find_all("a", href=True):
                            href_raw = a["href"]
                            # BeautifulSoup can return AttributeValueList for some
                            # attributes; normalizamos para `str` para evitar erros
                            # de tipagem e garantir que `lower()` exista.
                            href = str(href_raw)
                            # Retorna o primeiro que encontrar (PoC)
                            href_l = href.lower()
                            distrib_kw = "distribuicao-mensal"
                            pdf_kw = "pdf"
                            has_pdf = distrib_kw in href_l
                            has_pdf = has_pdf or pdf_kw in href_l
                            if has_pdf:
                                print("   🎯 PDF Encontrado: {}".format(href))
                                return href
            return None
        except Exception as e:
            print(f"   ❌ Erro no scraping: {e}")
            raise e

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
                    header = self._norm_row(
                        rows[0]
                    )  # Assume primeira linha como header
                    data_rows = rows[1:]
                else:
                    data_rows = rows

                all_data.extend(data_rows)

        if not all_data:
            return pd.DataFrame()

        # Cria DF (ajuste conforme a necessidade real das colunas)
        df = pd.DataFrame(all_data)
        # Se tiver header identificado, define. Senão, deixa numérico.
        if header:
            # Ajuste simples para evitar erro de tamanho
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
