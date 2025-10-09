from typing import Optional

from azure.keyvault.secrets import SecretClient

from demo.secret_fetchers.secret_fetcher import SecretFetcher


class KVSecretFetcher(SecretFetcher):
    def __init__(self, secret_client: SecretClient):
        self.client = secret_client

    def fetch_str(self, secret_name: str, secret_version: Optional[str] = None) -> str:
        secret = self.client.get_secret(secret_name, secret_version)
        return secret.value or ""

    def fetch_bytes(self, secret_name: str, secret_version: Optional[str] = None) -> bytes:
        secret_value = self.fetch_str(secret_name, secret_version)
        return self.to_bytes(secret_value)

    @staticmethod
    def to_bytes(secret: str) -> bytes:
        return secret.encode("utf-8")

    @staticmethod
    def to_str(secret: bytes) -> str:
        return secret.decode("utf-8")
