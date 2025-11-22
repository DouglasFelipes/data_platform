import pandas as pd

from data_platform.extractors.fnde_salario import FndeSalarioExtractor
from data_platform.extractors.fundeb_vaat import FundebVaatExtractor
from data_platform.extractors.pdf_extractor import PdfExtractor


class DummyResponse:
    def __init__(self, text: str):
        self.text = text


def test_fundeb_vaat_find_files(monkeypatch):
    html = """
    <html>
      <body>
        <a href="/files/lista_vaat_2025.pdf">Lista VAAT</a>
        <a href="https://other.org/irrelevant.pdf">Other</a>
      </body>
    </html>
    """

    # stub fetch_html to avoid real HTTP calls
    monkeypatch.setattr(
        FundebVaatExtractor, "fetch_html", lambda self, timeout=15: html
    )

    ext = FundebVaatExtractor(url="https://example.org/base", params=None)
    urls = ext.find_files()
    assert any("lista_vaat_2025.pdf" in u for u in urls)


def test_fundeb_vaat_extract_delegates_to_pdf(monkeypatch):
    html = '<a href="/files/lista_vaat_2025.pdf">Lista VAAT</a>'
    monkeypatch.setattr(
        FundebVaatExtractor, "fetch_html", lambda self, timeout=15: html
    )

    # monkeypatch PdfExtractor.extract to avoid pdf parsing
    dummy_df = pd.DataFrame({"a": [1, 2]})

    monkeypatch.setattr(PdfExtractor, "extract", lambda self: dummy_df)

    ext = FundebVaatExtractor(url="https://example.org/base", params=None)
    df = ext.extract()
    pd.testing.assert_frame_equal(df, dummy_df)


def test_fnde_salario_find_files(monkeypatch):
    html = '<a href="/downloads/distribuicao_mensal.pdf">Distribuicao</a>'
    monkeypatch.setattr(
        FndeSalarioExtractor, "fetch_html", lambda self, timeout=15: html
    )

    ext = FndeSalarioExtractor(url="https://example.org/salario", params=None)
    urls = ext.find_files()
    assert any("distribuicao_mensal.pdf" in u for u in urls)


def test_fnde_salario_extract_delegates_to_pdf(monkeypatch):
    html = '<a href="/downloads/distribuicao_mensal.pdf">Distribuicao</a>'
    monkeypatch.setattr(
        FndeSalarioExtractor, "fetch_html", lambda self, timeout=15: html
    )

    dummy_df = pd.DataFrame({"x": [10]})
    monkeypatch.setattr(PdfExtractor, "extract", lambda self: dummy_df)

    ext = FndeSalarioExtractor(url="https://example.org/salario", params=None)
    df = ext.extract()
    pd.testing.assert_frame_equal(df, dummy_df)
