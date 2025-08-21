from dataclasses import dataclass
from typing import Optional


@dataclass
class StorageAccountMetadata:
    connection_str: Optional[str] = None
    storage_account_name: Optional[str] = None
    container: Optional[str] = None
    client_id: Optional[str] = None
    tenant_id: Optional[str] = None
