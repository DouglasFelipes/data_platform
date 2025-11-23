"""
Downloader (explicação para leigos)

Este arquivo contém um pequeno componente responsável por baixar arquivos
da internet (por exemplo, PDFs). A ideia principal é:

- baixar o arquivo em pedaços (stream), para não ocupar muita memória;
- salvar o arquivo em uma pasta organizada por data
    (ex: `data/20251122/arquivo.pdf`);
- calcular um hash (SHA-256) durante o download para garantir integridade;
- devolver um dicionário com informações úteis sobre o download.
    Exemplos: caminho, tamanho, hash.

Comentários simples:
- "stream": significa ler o arquivo em blocos pequenos em vez de carregar tudo
    na memória — importante para arquivos grandes.
- "hash": é um resumo único do conteúdo. Se o arquivo mudar, o hash muda.
    Útil para checar se o download foi completo/íntegro.

O código abaixo é escrito para ser pequeno, testável e fácil de adaptar.
"""

from __future__ import annotations

# datetime: pega data/tempo atual
import datetime

# hashlib: cria hash do arquivo, para verificar integridade
import hashlib

# Path: trabalhar com caminhos de arquivos de forma limpa
from pathlib import Path

# Dict/Optional: definir o tipo do dicionário retornado
from typing import Dict, Optional

# Importa o objeto que realmente faz o download via HTTP.
from data_platform.core.scraping.fetcher import Fetcher


# Define um "objeto" responsável por baixar arquivos e devolver informações
# sobre o download.
class Downloader:
    """Classe responsável por baixar um único arquivo e devolver metadados.

    Para um leigo:
    - Você chama `Downloader().download(url, dest_dir)` e ele baixa o arquivo.
    - O método retorna um dicionário com informações (onde o arquivo ficou,
      quantos bytes, o hash SHA-256 e o código de resposta HTTP).

    A classe recebe opcionalmente um `Fetcher` (que encapsula as requisições HTTP).
    Isso facilita testes: podemos injetar um `Fetcher` falso que devolve respostas
    controladas.
    """

    def __init__(self, fetcher: Fetcher | None = None):
        # se nenhum fetcher for passado, criamos um padrão
        self.fetcher = fetcher or Fetcher()

    # Método para extrair nome do arquivo a partir da URL.
    # Explicação resumida:
    # 1) urlparse(url) divide a URL em partes (protocolo, domínio, caminho).
    # 2) Path(p.path).name pega o último pedaço do caminho
    #    (normalmente o nome do arquivo).
    # 3) Se não houver nome claro, geramos um nome único com timestamp.
    def _filename_from_url(self, url: str) -> str:
        """Tenta extrair um nome de arquivo da URL.

        Exemplos:
        - https://exemplo/arquivos/relatorio.pdf -> retorna 'relatorio.pdf'
        - https://exemplo/download?id=123 -> não tem nome claro, então retornamos
          um nome gerado com o timestamp para evitar colisões.
        """
        from urllib.parse import urlparse

        p = urlparse(url)
        name = Path(p.path).name
        # se não conseguir extrair um nome, cria um nome único baseado no tempo
        return name or f"download-{int(datetime.datetime.utcnow().timestamp())}"

    # Essa função executa o download real e devolve metadados.
    def download(self, url: str, dest_dir: str = "data") -> Dict[str, Optional[str]]:
        """Faz o download em modo 'stream' e salva em `dest_dir/YYYYMMDD/`.

        Passo a passo (explicado):
        1. Abre a conexão em modo stream via `fetcher.stream_get(url)` — assim
           podemos iterar por blocos (`chunks`) do arquivo.
        2. Verificamos se o servidor respondeu ok com `raise_for_status()` —
           se houver erro (404, 500, etc.) a função levanta exceção.
        3. Calculamos `filename` a partir da URL e criamos a pasta `dest_dir/YYYYMMDD`.
        4. Abrimos o arquivo local para escrita binária e gravamos os chunks à medida
           que chegam; ao mesmo tempo atualizamos o `hasher` (SHA-256) e somamos o
           total de bytes baixados.
        5. Ao final retornamos um dicionário com os metadados do download.

        Observações para leigos:
        - O arquivo é salvo em disco local nesta função — em outra parte do
          pipeline podemos escolher mover para um bucket remoto (GCS) e remover o
          arquivo local posteriormente.
        - Usamos chunks de 8KB para equilibrar IO e uso de memória.
        """
        # pede ao fetcher que abra a resposta em modo stream
        resp = self.fetcher.stream_get(url)
        resp.raise_for_status()

        # determina um nome e cria a pasta base com a data atual
        filename = self._filename_from_url(url)
        date_folder = datetime.datetime.utcnow().strftime("%Y%m%d")
        out_dir = Path(dest_dir) / date_folder
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / filename

        # prepara um objeto para calcular o hash SHA-256 enquanto escrevemos
        hasher = hashlib.sha256()
        total = 0
        with resp as r:
            # escrevemos em binário para suportar qualquer tipo de arquivo
            with open(out_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    fh.write(chunk)
                    hasher.update(chunk)
                    total += len(chunk)

        # devolve informações que serão usadas por outras partes do pipeline
        return {
            "path": str(out_path),
            "url": url,
            "sha256": hasher.hexdigest(),
            "size": str(total),
            "status_code": str(resp.status_code),
        }
