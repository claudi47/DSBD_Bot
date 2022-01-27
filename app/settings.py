from pydantic import BaseSettings


class BrokerSettings(BaseSettings):
    broker: str
    schema_registry: str


broker_settings = BrokerSettings()
