from dataclasses import dataclass
from typing import Optional


@dataclass
class KeyVaultMetadata:
    vault_url: str
    client_id: Optional[str] = None
    tenant_id: Optional[str] = None
    dx_api_token_secret_name: Optional[str] = None
