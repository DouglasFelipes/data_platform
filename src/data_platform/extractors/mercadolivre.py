import pandas as pd

from .base_html import BaseHtmlExtractor


class MercadoLivreExtractor(BaseHtmlExtractor):
    """
    Especialista em raspar o Mercado Livre.
    Sabe que o preço fica na classe '.ui-search-price'.
    """

    def extract(self) -> pd.DataFrame:
        termo = self.params.get("busca", "iphone")
        # Monta a URL específica desse site
        url_busca = f"https://lista.mercadolivre.com.br/{termo}"

        soup = self.get_soup(url_busca)
        if not soup:
            return pd.DataFrame()

        produtos = []
        # Aqui entra a "sujeira" específica deste site
        for item in soup.select(".ui-search-layout__item"):
            try:
                titulo = item.select_one(".ui-search-item__title").text.strip()
                # Logica complexa de extração de preço...
                preco = item.select_one(".price-tag-fraction").text

                produtos.append({"produto": titulo, "preco": preco, "origem": "ML"})
            except Exception:
                continue  # Pula se falhar um item

        print(f"✅ [ML Scraper] {len(produtos)} itens encontrados.")
        return pd.DataFrame(produtos)
