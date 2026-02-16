from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")

    # HTTP
    host: str = "0.0.0.0"
    port: int = 8080
    public_base_url: str = Field(default="https://parser.tunecrm.su", validation_alias="PUBLIC_BASE_URL")

    # Storage
    db_url: str = Field(default="sqlite:////data/app.db", validation_alias="DB_URL")

    # CLIProxyAPI (OpenAI-compatible)
    cliproxy_base_url: str = Field(default="http://cliapiproxy:8317/v1", validation_alias="CLIPROXY_BASE_URL")
    cliproxy_api_key: str = Field(default="", validation_alias="CLIPROXY_API_KEY")

    # Telegram bot notifications
    tg_bot_token: str = Field(default="", validation_alias="TG_BOT_TOKEN")
    tg_chat_id: str = Field(default="", validation_alias="TG_CHAT_ID")

    # LLM routing / retries
    llm_max_attempts: int = 3
    llm_timeout_s: float = 15.0
    message_total_timeout_s: float = 45.0

    # If true, never use models whose owned_by == "openai" (Codex accounts are there too).
    exclude_openai_owned_models: bool = True

    # Web UI auth (HTTP Basic). If user is empty, auth is disabled.
    auth_user: str = Field(default="", validation_alias="APP_AUTH_USER")
    auth_pass: str = Field(default="", validation_alias="APP_AUTH_PASS")


settings = Settings()
