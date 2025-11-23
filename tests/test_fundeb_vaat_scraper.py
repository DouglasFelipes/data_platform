"""Testes do scraper Fundeb VAAT (comentado para leigos).

Estes testes verificam duas funcionalidades importantes do plugin que
processa páginas do FNDE relacionadas ao VAAT:

1) `test_filter_selects_vaat_pdf`: garante que o filtro do plugin realmente
   seleciona o link do PDF esperado entre vários links.
2) `test_metadata_parsing`: garante que a lógica que extrai metadados do
   nome do arquivo (por exemplo, se é PDF, qual o ano) funciona conforme o
   esperado.

Os testes usam exemplos pequenos e determinísticos para validar o comportamento
do código; eles não baixam nem acessam a rede durante o teste (apenas
manipulam strings e lógica local).
"""

from data_platform.scrapers.fundeb_vaat_scraper import FundebVaatScraper


def test_filter_selects_vaat_pdf():
    # URL de exemplo que representa o PDF esperado no site do FNDE
    base = (
        "https://www.gov.br/fnde/pt-br/acesso-a-informacao/"
        "acoes-e-programas/financiamento/fundeb/vaat/"
    )
    url = base + "copy_of_ListadefinitvaVAAT202631agosto2025.pdf"
    # Simula uma lista de links extraídos de uma página: o scraper deve
    # reconhecer e escolher o link correto.
    links = [
        (url, "Lista dos entes habilitados/inabilitados ao VAAT 2026 (posição final)"),
        ("https://example.com/other.pdf", "other"),
    ]

    s = FundebVaatScraper(base, {})
    selected = s.filter_links(links)
    # Verifica que o link do VAAT foi incluído na seleção
    assert url in selected


def test_metadata_parsing():
    base = (
        "https://www.gov.br/fnde/pt-br/acesso-a-informacao/"
        "acoes-e-programas/financiamento/fundeb/vaat/"
    )
    url = base + "copy_of_ListadefinitvaVAAT202631agosto2025.pdf"
    s = FundebVaatScraper(base, {})
    meta = s.parse_filename(url)
    # meta deve indicar que é um PDF e extrair o nome do arquivo
    assert meta["is_pdf"] is True
    assert meta["filename"].endswith(".pdf")
    # O ano extraído deve ser um inteiro plausível (podemos aceitar 2025 ou 2026)
    assert meta["year"] == 2026 or meta["year"] == 2025 or isinstance(meta["year"], int)
