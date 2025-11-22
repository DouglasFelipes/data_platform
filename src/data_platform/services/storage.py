from prefect import task
import pandas as pd
from datetime import datetime
import os

@task(name="save_to_storage", retries=3, retry_delay_seconds=5)
def save_dataframe(df: pd.DataFrame, bucket: str, path: str, format: str = "parquet"):
    """
    Task Genérica de Load.
    Salva um DataFrame em um Bucket (Simulado localmente ou GCS).
    """
    if df.empty:
        print("⚠️ DataFrame vazio. Nada a salvar.")
        return None

    # Simulação de salvamento em GCS (usando caminho local para a PoC)
    # Num cenário real, usaria google.cloud.storage
    full_path = f"{path}/data.{format}"
    
    print(f"💾 [Storage Service] Iniciando salvamento em: gs://{bucket}/{full_path}")
    
    # Aqui entraria a lógica real do GCS. Para PoC, apenas printamos.
    # df.to_parquet(f"gs://{bucket}/{full_path}") 
    
    print(f"✅ [Storage Service] Sucesso! {len(df)} linhas salvas.")
    return full_path
