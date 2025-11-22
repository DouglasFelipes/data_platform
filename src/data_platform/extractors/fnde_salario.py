from typing import List

from .fnde_pdf import FndePdfExtractor


class FndeSalarioExtractor(FndePdfExtractor):
    """Extractor specialized for 'Salário-Educação' distribution PDFs.

    It sets default filters that try to match 'distribuicao' / 'mensal' in
    filenames or link text commonly used on the FNDE pages.
    """

    def __init__(self, url: str, params: dict | None = None):
        params = params or {}
        params.setdefault("filename_contains", "distribuicao")
        params.setdefault("link_text_contains", "distribuicao")
        super().__init__(url=url, params=params)

    def find_files(self) -> List[str]:
        return super().find_files()
