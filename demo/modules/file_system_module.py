from typing import Optional, cast

from azure.storage.blob import BlobServiceClient
from injector import Module, inject, provider, singleton

from demo.carrier.dna_carrier import DestinationFileSystem, SourceFileSystem
from demo.file_system.azure_storage_file_system import AzureStorageFileSystem
from demo.file_system.dna_nexus_file_system import DNANexusFileSystem


class FileSystemModule(Module):
    LANDING = "landing"
    DISPATCH = "dispatch"
    EMBASSY = "embassy"

    def __init__(
        self,
        stage: str,
        source_connection_str: Optional[str] = None,
        destination_connection_str: Optional[str] = None,
        project: Optional[str] = None,
        token: Optional[str] = None,
        source_container: Optional[str] = None,
        destination_container: Optional[str] = None,
    ):
        self.__source_connection_str = source_connection_str
        self.__destination_connection_str = destination_connection_str
        self.__project = project
        self.__token = token
        self.__source_container = source_container
        self.__destination_container = destination_container
        self.__stage = stage

    @singleton
    @provider
    @inject
    def provide_source_file_system(self) -> SourceFileSystem:
        if self.__stage == self.LANDING:
            assert self.__project is not None
            assert self.__token is not None
            return cast(SourceFileSystem, DNANexusFileSystem(self.__project, self.__token))

        assert self.__source_container is not None
        assert self.__source_connection_str is not None
        return cast(
            SourceFileSystem,
            AzureStorageFileSystem(
                BlobServiceClient.from_connection_string(self.__source_connection_str), self.__source_container
            ),
        )

    @singleton
    @provider
    def provide_destination_file_system(self) -> DestinationFileSystem:
        assert self.__destination_container is not None
        assert self.__destination_connection_str is not None
        return cast(
            DestinationFileSystem,
            AzureStorageFileSystem(
                BlobServiceClient.from_connection_string(self.__destination_connection_str),
                self.__destination_container,
            ),
        )
