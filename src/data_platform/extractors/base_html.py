import requests  # type: ignore
from bs4 import BeautifulSoup

from data_platform.core.interfaces import BaseExtractor


class BaseHtmlExtractor(BaseExtractor):
    """
    Classe Pai para Scraping HTML.
    Já traz ferramentas prontas: User-Agent, Soup, Tratamento de Erro.
    """

    def get_soup(self, url: str):
        """Método utilitário que todo scraper filho vai usar."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ...",
            "Accept-Language": "pt-BR,pt;q=0.9",
        }
        print(f"🌐 [Scraper] Acessando: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"❌ Erro de conexão: {e}")
            return None

    # O método extract() continua abstrato. O filho É OBRIGADO a implementar.
