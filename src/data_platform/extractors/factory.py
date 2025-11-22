from data_platform.extractors.base_api import RestApiExtractor
from data_platform.extractors.fnde_pdf import FndePdfExtractor
from data_platform.extractors.fnde_salario import FndeSalarioExtractor
from data_platform.extractors.fundeb_vaat import FundebVaatExtractor
from data_platform.extractors.g1_news import G1NewsExtractor
from data_platform.extractors.mercadolivre import MercadoLivreExtractor


def get_extractor(extractor_type: str):
    """
    Factory Pattern: Decide dinamicamente qual classe instanciar.
    O Flow não precisa saber quais extratores existem.
    """
    extractors_map = {
        "fnde_pdf": FndePdfExtractor,
        "fnde_salario": FndeSalarioExtractor,
        "fundeb_vaat": FundebVaatExtractor,
        "rest_api": RestApiExtractor,
        "scraper_ml": MercadoLivreExtractor,
        "scraper_g1": G1NewsExtractor,
    }

    extractor_class = extractors_map.get(extractor_type)

    if not extractor_class:
        raise ValueError(f"❌ Extrator '{extractor_type}' não registrado na Factory.")

    return extractor_class
