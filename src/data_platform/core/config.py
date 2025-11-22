from pydantic import BaseModel, Field, field_validator
from typing import Dict, Any
from datetime import datetime

class PipelineConfig(BaseModel):
    """
    Contrato de Configuração Universal.
    Define tudo que é necessário para rodar uma ingestão.
    """
    job_name: str
    environment: str = Field(default="dev", pattern="^(dev|staging|prod)$")
    source_type: str  # ex: "fnde_pdf", "rest_api"
    
    # Configurações de Origem
    source_url: str
    source_params: Dict[str, Any] = Field(default_factory=dict)
    
    # Configurações de Destino
    destination_bucket: str
    destination_path: str
    
    # Metadados de execução (injetados automaticamente se não passados)
    execution_date: str = Field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    @property
    def raw_path(self) -> str:
        """Gera o caminho padrão para a camada Raw: raw/job_name/data_captura=YYYY-MM-DD"""
        return f"{self.destination_path}/raw/{self.job_name}/data_captura={self.execution_date}"

    @field_validator('job_name')
    def job_name_must_be_slug(cls, v):
        if ' ' in v:
            raise ValueError('job_name não deve conter espaços')
        return v.lower()
