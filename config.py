from pydantic_settings import BaseSettings
from typing import Literal
from functools import lru_cache


class Settings(BaseSettings):
    # Storage - Azure Blob wih SAS authentication
    storage_type: Literal["azure"] = "azure"
    azure_blob_name: str = ""
    sas_url: str = ""
    sas_token: str = ""
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
