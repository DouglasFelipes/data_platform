from abc import ABC, abstractmethod

import pandas as pd


class BaseExtractor(ABC):
    """
    Interface (Contrato) que todo Extrator deve seguir.

    Isso garante que o Flow não precise mudar quando surgir uma nova fonte.
    """

    def __init__(self, url: str, params: dict):
        self.url = url
        self.params = params

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Deve executar a lógica de extração e retornar um DataFrame Pandas limpo.
        Se não houver dados, retorna um DataFrame vazio.
        """
        pass

    @abstractmethod
    def find_files(self) -> list[str]:
        """Discover candidate file URLs for this extractor.

        Implementations should return a list of absolute URLs (may be empty).
        This method is used by higher-level flows and by generic PDF parsers
        that can be delegated to when a specific file URL is required.
        """
        raise NotImplementedError()
