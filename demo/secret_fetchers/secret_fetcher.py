from abc import ABC, abstractmethod
from typing import Optional


class SecretFetcher(ABC):
    @abstractmethod
    def fetch_str(self, secret_name: str, secret_version: Optional[str] = None) -> str:
        raise NotImplementedError()

    @abstractmethod
    def fetch_bytes(self, secret_name: str, secret_version: Optional[str] = None) -> bytes:
        raise NotImplementedError()

    @staticmethod
    def to_bytes(secret: str) -> bytes:
        raise NotImplementedError()

    @staticmethod
    def to_str(secret: bytes) -> str:
        raise NotImplementedError()
