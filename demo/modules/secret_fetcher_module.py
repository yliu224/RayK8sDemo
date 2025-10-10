import logging

from azure.identity import AzureCliCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from injector import Module, provider, singleton

from demo.config.stage.stage_metadata import StageMetadata
from demo.modules.constants import DxApiToken
from demo.secret_fetchers.kv_secret_fetcher import KVSecretFetcher
from demo.secret_fetchers.secret_fetcher import SecretFetcher

LOG = logging.getLogger(__name__)


class SecretFetcherModule(Module):
    def __init__(self, stage_metadata: StageMetadata):
        self.__key_vault_metadata = stage_metadata.key_vault
        self.__default_client_id = stage_metadata.default_client_id
        self.__default_tenant_id = stage_metadata.default_tenant_id

    @singleton
    @provider
    def provide_dx_token_fetcher(self, secret_fetcher: SecretFetcher) -> DxApiToken:
        assert secret_fetcher is not None
        assert self.__key_vault_metadata is not None
        assert self.__key_vault_metadata.dx_api_token_secret_name is not None
        LOG.info("Fetching DNA Nexus API token from Key Vault secret ")
        return DxApiToken(secret_fetcher.fetch_str(self.__key_vault_metadata.dx_api_token_secret_name))

    @singleton
    @provider
    def provide_kv_secret_fetcher(self) -> SecretFetcher:
        assert self.__key_vault_metadata is not None
        client_id = self.__key_vault_metadata.client_id or self.__default_client_id
        tenant_id = self.__key_vault_metadata.tenant_id or self.__default_tenant_id
        if client_id is not None and tenant_id is not None:
            LOG.info(
                f"Using ManagedIdentityCredential with client_id {client_id} and "
                f"tenant_id {tenant_id} to access Key Vault"
            )
            secret_client = SecretClient(
                vault_url=self.__key_vault_metadata.vault_url,
                credential=ManagedIdentityCredential(client_id=client_id, tenant_id=tenant_id),
            )
        else:
            LOG.info("Using AzureCliCredential to access Key Vault")
            secret_client = SecretClient(
                vault_url=self.__key_vault_metadata.vault_url, credential=AzureCliCredential()
            )
        return KVSecretFetcher(secret_client)
