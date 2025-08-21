from dataclasses import dataclass
from typing import Optional

from demo.config.stage.storage_account_metadata import StorageAccountMetadata


@dataclass
class StageMetadata:
    stage: str
    source_storage_account: Optional[StorageAccountMetadata] = None
    dest_storage_account: Optional[StorageAccountMetadata] = None
    project: Optional[str] = None
    token: Optional[str] = None
