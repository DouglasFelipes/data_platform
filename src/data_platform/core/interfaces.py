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
