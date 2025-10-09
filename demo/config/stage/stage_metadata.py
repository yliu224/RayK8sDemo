from dataclasses import dataclass
from typing import Optional

from demo.config.stage.kv_metadata import KeyVaultMetadata
from demo.config.stage.storage_account_metadata import StorageAccountMetadata


@dataclass
class StageMetadata:
    stage: str
    source_storage_account: Optional[StorageAccountMetadata] = None
    dest_storage_account: Optional[StorageAccountMetadata] = None
    key_vault: Optional[KeyVaultMetadata] = None
    project: Optional[str] = None
    default_client_id: Optional[str] = None
    default_tenant_id: Optional[str] = None
