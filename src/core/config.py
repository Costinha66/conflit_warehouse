from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATA_ROOT: str = "./"
    START_YEAR: int = 2022
    END_YEAR: int = 2023
    REGION_FILTER: str | None = None
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
