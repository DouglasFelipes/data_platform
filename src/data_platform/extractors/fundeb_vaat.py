from typing import List

from data_platform.extractors.pdf_extractor import PdfExtractor
from data_platform.extractors.scraping_extractor import ScrapingExtractor


class FundebVaatExtractor(ScrapingExtractor):
    """Extractor specialized for FUNDEB VAAT lists.

    Examples: Listapreliminar / Lista definitiva PDFs. Reuses the generic PDF
    scraping logic but applies default filters tuned to VAAT filenames and
    link texts.
    """

    def __init__(self, url: str, params: dict | None = None):
        params = params or {}
        # Provide sensible defaults for VAAT lists; user params override these.
        params.setdefault("filename_contains", "vaat")
        params.setdefault("link_text_contains", "lista")
        super().__init__(url=url, params=params)

    # Optionally expose a typed wrapper
    def find_files(self) -> List[str]:
        # Use ScrapingExtractor.find_files with default PDF pattern, but also
        # filter by defaults useful for VAAT lists.
        html = self.fetch_html()
        if not html:
            return []

        # patterns that capture PDFs referenced with VAAT/lista hints
        patterns = [
            r"vaat.*\.pdf$",
            r"lista.*\.pdf$",
            r"listapreliminar.*\.pdf$",
            r"\.pdf$",
        ]
        candidates = self.extract_links(html, patterns=patterns)
        return candidates

    def extract(self):
        # find the first VAAT PDF and delegate to the generic PDF parser
        urls = self.find_files()
        if not urls:
            import pandas as pd

            return pd.DataFrame()
        # Delegate parsing to the generic PDF parser by passing pdf_url in params
        pdf_url = urls[0]
        parser = PdfExtractor(url=pdf_url, params={})
        return parser.extract()
