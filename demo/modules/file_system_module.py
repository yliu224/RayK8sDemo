from typing import cast

from azure.core.credentials import TokenCredential
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
from injector import Module, provider, singleton

from demo.carrier.dna_carrier import DestinationFileSystem, SourceFileSystem
from demo.config.stage.stage_metadata import StageMetadata
from demo.config.stage.storage_account_metadata import StorageAccountMetadata
from demo.file_system.azure_storage_file_system import AzureStorageFileSystem
from demo.file_system.dna_nexus_file_system import DNANexusFileSystem


class FileSystemModule(Module):
    LANDING = "landing"
    DISPATCH = "dispatch"
    EMBASSY = "embassy"
    NOTIFICATION = "notification"

    def __init__(self, stage_metadata: StageMetadata):
        self.__source_storage_account_metadata = stage_metadata.source_storage_account
        self.__project = stage_metadata.project
        self.__token = stage_metadata.token
        self.__dest_storage_account_metadata = stage_metadata.dest_storage_account
        self.__stage = stage_metadata.stage

    @singleton
    @provider
    def provide_source_file_system(self) -> SourceFileSystem:
        if self.__stage == self.LANDING:
            assert self.__project is not None
            assert self.__token is not None
            return cast(SourceFileSystem, DNANexusFileSystem(self.__project, self.__token))

        if self.__stage in [self.DISPATCH, self.EMBASSY, self.NOTIFICATION]:
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

    @singleton
    @provider
    def provide_destination_file_system(self) -> DestinationFileSystem:
        if self.__stage == self.NOTIFICATION:
            assert self.__project is not None
            assert self.__token is not None
            return cast(DestinationFileSystem, DNANexusFileSystem(self.__project, self.__token))

        if self.__stage in [self.DISPATCH, self.EMBASSY, self.LANDING]:
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

    @staticmethod
    def __provide_storage_account(metadata: StorageAccountMetadata) -> BlobServiceClient:
        if metadata:
            if metadata.connection_str:
                return BlobServiceClient.from_connection_string(metadata.connection_str)
            if metadata.storage_account_name and metadata.client_id and metadata.tenant_id:
                return BlobServiceClient(
                    account_url=f"https://{metadata.storage_account_name}.blob.core.windows.net",
                    credential=cast(
                        TokenCredential,
                        ManagedIdentityCredential(client_id=metadata.client_id, tenant_id=metadata.tenant_id),
                    ),
                )
        raise ValueError("Please provide metadata to init storage account")
