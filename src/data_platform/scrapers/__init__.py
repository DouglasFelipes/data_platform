"""Registry and helper to select scraper plugin by domain.

Simplest form: map known domains to a Scraper class. Falls back to
`BaseScraper` which performs no filtering (returns all links).
"""

from typing import Type
from urllib.parse import urlparse

from .base_scraper import BaseScraper
from .fundeb_vaat_scraper import FundebVaatScraper
from .salario_educacao_scraper import SalarioEducacaoScraper

_REGISTRY: dict[str, Type[BaseScraper]] = {
    "www.gov.br": SalarioEducacaoScraper,
}


def get_scraper_for_url(url: str) -> Type[BaseScraper]:
    domain = urlparse(url).netloc.lower()
    path = urlparse(url).path.lower()
    # naive: match exact domain first, else try suffix
    if domain in _REGISTRY:
        # domain-level mapping present; allow path-based overrides
        # prefer a Fundeb/VAAT-specific plugin when path indicates VAAT
        if "vaat" in path or "/fundeb/" in path:
            return FundebVaatScraper
        return _REGISTRY[domain]
    for key in _REGISTRY:
        if domain.endswith(key):
            return _REGISTRY[key]
    # if domain didn't match, still allow VAAT path heuristic as a last resort
    if "vaat" in path or "/fundeb/" in path:
        return FundebVaatScraper
    return BaseScraper


__all__ = ["get_scraper_for_url", "BaseScraper"]
