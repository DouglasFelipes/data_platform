from data_platform.extractors.base_api import RestApiExtractor
from data_platform.extractors.fnde_salario import FndeSalarioExtractor
from data_platform.extractors.fundeb_vaat import FundebVaatExtractor


def get_extractor(extractor_type: str):
    """
    Factory Pattern: Decide dinamicamente qual classe instanciar.
    O Flow não precisa saber quais extratores existem.
    """
    extractors_map = {
        "rest_api": RestApiExtractor,
        "fnde_salario": FndeSalarioExtractor,
        "fundeb_vaat": FundebVaatExtractor,
    }

    extractor_class = extractors_map.get(extractor_type)

    if not extractor_class:
        raise ValueError(f"❌ Extrator '{extractor_type}' não registrado na Factory.")

    return extractor_class
