from typing import Tuple, cast

from azure.storage.blob import BlobServiceClient

from demo.file_system.azure_storage_file_system import AzureStorageFileSystem
from demo.modules.constants import DestinationFileSystem, SourceFileSystem


class TestDNACarrier:
    def provide_source_and_dest_fs(
        self, source_container: str, dest_container: str, connection_str: str
    ) -> Tuple[SourceFileSystem, DestinationFileSystem]:
        source_fs = cast(
            SourceFileSystem,
            AzureStorageFileSystem(BlobServiceClient.from_connection_string(connection_str), source_container),
        )
        dest_fs = cast(
            DestinationFileSystem,
            AzureStorageFileSystem(BlobServiceClient.from_connection_string(connection_str), dest_container),
        )
        return source_fs, dest_fs
