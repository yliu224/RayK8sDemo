import logging
from typing import cast

from azure.core.credentials import TokenCredential
from azure.identity import AzureCliCredential, ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
from injector import Module, provider

from demo.config.stage.stage_metadata import StageMetadata
from demo.config.stage.storage_account_metadata import StorageAccountMetadata
from demo.file_system.azure_storage_file_system import AzureStorageFileSystem
from demo.file_system.dna_nexus_file_system import DNANexusFileSystem
from demo.modules.constants import (
    DISPATCH,
    EMBASSY,
    LANDING,
    NOTIFICATION,
    DestinationFileSystem,
    DxApiToken,
    SourceFileSystem,
)

LOG = logging.getLogger(__name__)


class FileSystemModule(Module):

    def __init__(self, stage_metadata: StageMetadata):
        self.__source_storage_account_metadata = stage_metadata.source_storage_account
        self.__project = stage_metadata.project
        self.__dest_storage_account_metadata = stage_metadata.dest_storage_account
        self.__stage = stage_metadata.stage
        self.__default_client_id = stage_metadata.default_client_id
        self.__default_tenant_id = stage_metadata.default_tenant_id

    @provider
    def provide_source_file_system(self, token: DxApiToken) -> SourceFileSystem:
        if self.__stage == LANDING:
            assert self.__project is not None
            assert token is not None
            LOG.info(f"Token is {token},{type(token)}")
            return cast(SourceFileSystem, DNANexusFileSystem(self.__project, token))

        if self.__stage in [DISPATCH, EMBASSY, NOTIFICATION]:
            assert self.__source_storage_account_metadata is not None
            assert self.__source_storage_account_metadata.container is not None
            return cast(
                SourceFileSystem,
                AzureStorageFileSystem(
                    self.__provide_storage_account(self.__source_storage_account_metadata),
                    self.__source_storage_account_metadata.container,
                ),
            )

        raise ValueError(f"Unknown stage {self.__stage} when creating SourceFileSystem")

    @provider
    def provide_destination_file_system(self, token: DxApiToken) -> DestinationFileSystem:
        if self.__stage == NOTIFICATION:
            assert self.__project is not None
            assert token is not None
            return cast(DestinationFileSystem, DNANexusFileSystem(self.__project, token))

        if self.__stage in [DISPATCH, EMBASSY, LANDING]:
            assert self.__dest_storage_account_metadata is not None
            assert self.__dest_storage_account_metadata.container is not None
            return cast(
                DestinationFileSystem,
                AzureStorageFileSystem(
                    self.__provide_storage_account(self.__dest_storage_account_metadata),
                    self.__dest_storage_account_metadata.container,
                ),
            )
        raise ValueError(f"Unknown stage {self.__stage} when creating DestinationFileSystem")

    def __provide_storage_account(self, metadata: StorageAccountMetadata) -> BlobServiceClient:
        if metadata:
            if metadata.connection_str:
                LOG.info("Loading storage account from connection string")
                return BlobServiceClient.from_connection_string(metadata.connection_str)
            client_id = metadata.client_id or self.__default_client_id
            tenant_id = metadata.tenant_id or self.__default_tenant_id
            if metadata.storage_account_name and client_id and tenant_id:
                LOG.info("Loading storage account from managed identity")
                return BlobServiceClient(
                    account_url=f"https://{metadata.storage_account_name}.blob.core.windows.net",
                    credential=cast(
                        TokenCredential,
                        ManagedIdentityCredential(client_id=metadata.client_id, tenant_id=metadata.tenant_id),
                    ),
                )
            if metadata.storage_account_name:
                LOG.info("Loading storage account from AzureCliCredential")
                return BlobServiceClient(
                    account_url=f"https://{metadata.storage_account_name}.blob.core.windows.net",
                    credential=cast(TokenCredential, AzureCliCredential()),
                )
        raise ValueError("Please provide metadata to init storage account")
