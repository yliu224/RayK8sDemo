from typing import Optional, cast

from azure.storage.blob import BlobServiceClient
from injector import Module, provider, singleton

from demo.file_system.azure_storage_file_system import BlobClient, Container
from demo.file_system.dna_nexus_file_system import Project, Token


class FileSystemModule(Module):
    def __init__(self, connection_str: Optional[str], project: Project, token: Token, container: Container):
        self.__connection_str = connection_str
        self.__project = project
        self.__token = token
        self.__container = container

    @singleton
    @provider
    def provide_azure_storage_client(self) -> BlobClient:
        if self.__connection_str:
            return cast(BlobClient, BlobServiceClient.from_connection_string(self.__connection_str))
        raise RuntimeError("Please provide value to generate storage client instance")

    @provider
    def provide_container(self) -> Container:
        return self.__container

    @provider
    def provide_project(self) -> Project:
        return self.__project

    @provider
    def provide_token(self) -> Token:
        return self.__token
