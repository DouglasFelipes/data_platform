from typing import List

from .fnde_pdf import FndePdfExtractor


class FundebVaatExtractor(FndePdfExtractor):
    """Extractor specialized for FUNDEB VAAT lists.

    Examples: Listapreliminar / Lista definitiva PDFs. Reuses the FNDE PDF
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
        return super().find_files()
