import pandas as pd
from .base_html import BaseHtmlExtractor

class G1NewsExtractor(BaseHtmlExtractor):
    """
    Especialista em raspar o G1.
    Sabe que o título é '.feed-post-body-title'.
    """
    
    def extract(self) -> pd.DataFrame:
        soup = self.get_soup("https://g1.globo.com/")
        if not soup: return pd.DataFrame()

        noticias = []
        for post in soup.select(".feed-post-body"):
            titulo = post.select_one(".feed-post-link").text.strip()
            resumo = post.select_one(".feed-post-body-resumo").text.strip() if post.select_one(".feed-post-body-resumo") else ""
            
            noticias.append({"titulo": titulo, "resumo": resumo, "origem": "G1"})

        return pd.DataFrame(noticias)
