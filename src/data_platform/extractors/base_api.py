import requests
import pandas as pd
from data_platform.core.interfaces import BaseExtractor

class RestApiExtractor(BaseExtractor):
    """
    Extrator Genérico para APIs REST.
    Já implementa a lógica repetitiva de conexão HTTP.
    """
    
    def extract(self) -> pd.DataFrame:
        """
        Implementação padrão: faz um GET e transforma JSON em DataFrame.
        Pode ser sobrescrita por classes filhas se a API for complexa.
        """
        print(f"🌐 [API Extractor] Conectando em: {self.url}")
        
        try:
            # Lógica genérica de chamada
            response = requests.get(self.url, params=self.params)
            response.raise_for_status()
            
            data = response.json()
            
            # Tratamento genérico de retorno (Lista ou Chave 'data')
            if isinstance(data, dict):
                # Muitas APIs retornam { "results": [...] }
                for key in ["results", "data", "items"]:
                    if key in data:
                        data = data[key]
                        break
            
            # Se virou lista, vira DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
                print(f"✅ [API Extractor] {len(df)} registros obtidos.")
                return df
            
            # Se for um único objeto
            return pd.DataFrame([data])

        except Exception as e:
            print(f"❌ [API Extractor] Erro na requisição: {e}")
            return pd.DataFrame()
