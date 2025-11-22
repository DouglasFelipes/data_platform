PYTHON := python

.PHONY: install test lint format run

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	pytest -q

lint:
	flake8 .

format:
	black .

run:
	PYTHONPATH=src $(PYTHON) -m data_platform.flows.ingestion_flow
