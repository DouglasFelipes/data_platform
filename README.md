# Data Platform

Projeto de exemplo para ingestão e processamento de dados com Prefect.

## Estrutura

- `data_platform/` : pacote principal com módulos de extração, serviços e fluxos.
- `jobs/` : scripts auxiliares (ex.: `job_pyspark.py`).
- `prefect-env/` : ambiente virtual usado localmente (não comitar normalmente).
- `tests/` : testes com `pytest`.

## Rápido começo

1. Ative o ambiente virtual (se existir):

```bash
source prefect-env/bin/activate
```

2. Instale dependências (opcional):

```bash
pip install -r requirements.txt
```

3. Rode o fluxo de ingestão:

```bash
python -m data_platform.flows.ingestion_flow
```

## Testes

Rode os testes com:

```bash
pytest -q
```

## Contribuindo

Veja `CONTRIBUTING.md` para orientações básicas.

## Boas práticas

- Evite nomes de arquivo que conflitem com pacotes externos (ex.: `pandas.py`).
- Use `pyproject.toml`/`requirements.txt` para gerenciar dependências.
- Adicione `pre-commit` para formatar e checar o código automaticamente.
