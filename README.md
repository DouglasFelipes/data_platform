# Data Platform — Scraping & Ingestão (Prefect)

## Resumo

Projeto de ingestão e scraping genérico: um motor central (fetcher, parser, downloader, normalizer) + plugins pequenos por site (scrapers). PDFs são baixados, enviados para `staging` no GCS e tabelas extraídas viram arquivos Parquet enviados para `raw`.

## Arquitetura (resumida)

- Core primitives: `data_platform/core/scraping/*` — Fetcher, Parser, Downloader e tasks Prefect.
- Serviços: `data_platform/services/*` — `gcs.py` (uploader), `pdf_processor.py` (extração + parquet).
- Flows: `data_platform/flows/universal_downloader.py` — flow universal que orquestra o pipeline.
- Scrapers: `data_platform/scrapers/*` — plugins por site, implementam `filter_links(links)`.

## Pré-requisitos

- Python 3.10+ (o ambiente do projeto usa 3.10)
- Virtualenv / venv preferido. Exemplo:

```bash
python -m venv prefect-env
source prefect-env/bin/activate
pip install -r requirements.txt
```

## Credenciais GCS

Configure a variável de ambiente `GOOGLE_APPLICATION_CREDENTIALS` com o caminho do JSON da service account que tem permissão de escrita no bucket.

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-prefect.json
```

## Como rodar o fluxo (local)

O projeto inclui um fluxo de exemplo. Você pode executá-lo diretamente:

```bash
# rodar o exemplo do fluxo universal
python -m data_platform.flows.universal_downloader
```

No payload de exemplo (veja `__main__` no arquivo) está configurado para baixar um PDF do site do FNDE e processar 1 arquivo.

## Como adicionar um novo scraper (plugin)

1. Crie um arquivo em `src/data_platform/scrapers/` com uma classe que herde de `BaseScraper` ou que implemente a mesma interface: um construtor `(base_url, params)` e `filter_links(links)` que recebe `[(url, link_text), ...]` e retorna `[url, ...]`.
2. Registre seu scraper no pacote `data_platform.scrapers.__init__` (atualmente a seleção é feita por `get_scraper_for_url` por heurística de domínio/paths).
3. Teste localmente com o fluxo universal passando `source_params` contendo `dataset_name` (obrigatório) e filtros opcionais como `filename_contains`.

Exemplo mínimo de `source_params`:

```json
{
  "max_files": 5,
  "dataset_name": "meu_dataset",
  "filename_contains": "relatorio"
}
```

## Política de armazenamento

- O PDF original é enviado para `gs://<bucket>/<prefix>/staging/<dataset>/data_captura=YYYYMMDD/...` para auditoria.
- Os Parquets (tabelas extraídas) são enviados para `gs://<bucket>/<prefix>/raw/<dataset>/data_captura=YYYYMMDD/year=YYYY/month=MM/...`.
- O pipeline evita preservar arquivos PDF localmente (usa diretórios temporários) e remove artefatos locais por padrão.

## Testes

Rodar a suíte de testes:

```bash
pytest -q
```

## Notas finais

- O `dataset_name` é obrigatório no payload do job para garantir consistência de nomes de datasets no bucket.
- Se quiser que eu force o prefixo de staging para sempre `datalake/staging`, ou adicionar um parâmetro `keep_local_copy` por job, posso ajustar.

---

Atualizado em: 22 de novembro de 2025
