from typing import List

from data_platform.extractors.pdf_extractor import PdfExtractor
from data_platform.extractors.scraping_extractor import ScrapingExtractor


class FndeSalarioExtractor(ScrapingExtractor):
    """Extractor specialized for 'Salário-Educação' distribution PDFs.

    Uses the generic scraping base to discover candidate PDF links and
    then delegates parsing of the selected PDF to `PdfExtractor`.
    """

    def __init__(self, url: str, params: dict | None = None):
        params = params or {}
        params.setdefault("filename_contains", "distribuicao")
        params.setdefault("link_text_contains", "distribuicao")
        super().__init__(url=url, params=params)

    def find_files(self) -> List[str]:
        html = self.fetch_html()
        if not html:
            return []

        patterns = [r"distribuicao.*\.pdf$", r"mensal.*\.pdf$", r"\.pdf$"]
        candidates = self.extract_links(html, patterns=patterns)
        return candidates

    def extract(self):
        urls = self.find_files()
        if not urls:
            import pandas as pd

            return pd.DataFrame()
        pdf_url = urls[0]
        parser = PdfExtractor(url=pdf_url, params={})
        return parser.extract()
