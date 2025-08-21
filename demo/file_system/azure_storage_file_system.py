import os
from typing import List

from azure.storage.blob import BlobProperties, BlobServiceClient

from demo.file_system.file_info import FileInfo
from demo.file_system.file_system import FileSystem


class AzureStorageFileSystem(FileSystem):

    def __init__(self, blob_client: BlobServiceClient, container: str):
        super().__init__()
        self.__blob_service_client = blob_client
        self.__container_client = self.__blob_service_client.get_container_client(container)

    def list_folder(self, folder_path: str, recursive: bool = True) -> List[FileInfo]:
        """List blobs under a folder path in Azure Blob Storage"""
        results: List[FileInfo] = []

        if not folder_path.endswith("/"):
            folder_path += "/"

        # use walk_blobs if recursive, else list_blobs with delimiter
        blob_list = (
            self.__container_client.walk_blobs(name_starts_with=folder_path)
            if recursive
            else self.__container_client.list_blobs(name_starts_with=folder_path)
        )

        for blob in blob_list:
            assert isinstance(blob, BlobProperties)
            file_name = os.path.basename(blob.name)
            folder = os.path.dirname(blob.name)
            results.append(FileInfo(file_name=file_name, file_size=blob.size, folder=folder, file_id=blob.etag))
        return results

    def download_file(self, file_info: FileInfo, destination: str) -> bool:
        """Download a blob to local path"""

        blob_client = self.__container_client.get_blob_client(file_info.file_path)
        os.makedirs(destination, exist_ok=True)
        with open(os.path.join(destination, file_info.file_name), "wb") as f:
            stream = blob_client.download_blob()
            # Stream in chunks instead of readall()
            for chunk in stream.chunks():
                f.write(chunk)
        return True

    def upload_file(self, source_path: str, destination_path: str) -> bool:
        """Upload a local file to Azure Blob Storage"""
        blob_client = self.__container_client.get_blob_client(destination_path)
        with open(source_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)
        return True

    def upload_files(self, source_folder: str, destination_path: str) -> bool:
        if destination_path.endswith("/"):
            destination_path = destination_path[:-1]
        result = True
        file_names = [f for f in os.listdir(source_folder) if os.path.isfile(os.path.join(source_folder, f))]
        for f in file_names:
            result &= self.upload_file(os.path.join(source_folder, f), f"{destination_path}/{f}")
        return result

    def get_file_system_name(self) -> str:
        return f"{self.__container_client.container_name}@{self.__container_client.account_name}"
