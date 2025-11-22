import asyncio
from inspect import iscoroutinefunction
from typing import Any, cast

import pandas as pd
from prefect import flow, task

from data_platform.core.config import PipelineConfig
from data_platform.extractors.factory import get_extractor
from data_platform.services.storage import save_dataframe


@task(name="execute_extraction")
def execute_extraction_strategy(config: PipelineConfig):
    """
    Task que encapsula a estratégia de extração.
    """
    # 1. Pede para a fábrica a ferramenta certa (FNDE, API, SQL, etc)
    ExtractorClass = get_extractor(config.source_type)

    # 2. Inicializa a ferramenta
    extractor = ExtractorClass(url=config.source_url, params=config.source_params)

    # 3. Executa sem saber o que tem dentro
    return extractor.extract()


@flow(name="Universal Ingestion Pipeline", log_prints=True)
def run_ingestion_pipeline(config_dict: dict):
    """
    Flow Mestre.
    Recebe um dicionário (JSON), valida o contrato e executa a pipeline.
    """
    print("-" * 30)
    print("🚀 Iniciando Pipeline Universal")
    print("-" * 30)

    # 1. Validação de Contrato (Pydantic)
    # Se falhar aqui, o flow para imediatamente com erro claro.
    try:
        config = PipelineConfig(**config_dict)
        print(f"✅ Configuração válida para: {config.job_name}")
    except Exception as e:
        print(f"❌ Configuração inválida: {e}")
        raise e

    # 2. Extração (Polimorfismo)
    # execute_extraction_strategy é uma Task do Prefect; o objeto `.fn`
    # contém a função subjacente. Em algumas versões essa função pode ser
    # assíncrona — então precisamos tratá-la corretamente para não gerar
    # um coroutine não-awaitado (mypy/Runtime error).
    func = execute_extraction_strategy.fn
    result: Any
    if iscoroutinefunction(func):
        # Executa a coroutine de forma síncrona neste processo (PoC).
        result = asyncio.run(func(config))
    else:
        result = func(config)

    df = cast(pd.DataFrame, result)

    # 3. Carga (Reutilizável)
    save_dataframe(df=df, bucket=config.destination_bucket, path=config.raw_path)


# ==========================================
# EXECUÇÃO LOCAL (Para testes)
# ==========================================
if __name__ == "__main__":
    # --- TESTE: Mercado Livre Scraper ---
    payload_ml = {
        "job_name": "pesquisa_iphone_ml",
        "environment": "dev",
        # O Tipo que registramos na factory
        "source_type": "scraper_ml",
        # URL Base (O extrator vai montar a URL final de busca,
        # mas o Pydantic exige este campo, então passamos a home)
        "source_url": "https://www.mercadolivre.com.br",
        # Parâmetros que o MercadoLivreExtractor espera
        "source_params": {"busca": "iphone 15 pro max"},
        "destination_bucket": "br-doug-dev",
        "destination_path": "datalake",
    }

    print("🛒 Iniciando Raspagem do Mercado Livre...")
    run_ingestion_pipeline(payload_ml)
