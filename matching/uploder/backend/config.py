from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    signed_url_bucket: str
    signed_url_expires_in: int
    job_name: str
    job_queue: str
    job_definition: str
    user_pool_id: str
    ses_source_email_address: str

    model_config = SettingsConfigDict(env_file=".env")
