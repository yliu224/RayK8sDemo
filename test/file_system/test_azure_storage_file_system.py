import logging
from test.conftest import file_checker

import pytest
from azure.storage.blob import BlobServiceClient

from demo.file_system.azure_storage_file_system import AzureStorageFileSystem

INPUT_FILE_PATH = "test_fs.txt"
TEXT = "hello world"
OUTPUT_FOLDER = "test_output/fs"
CONTAINER_NAME = "fstest"

LOG = logging.getLogger(__name__)


@pytest.mark.parametrize("azurite_context", [[CONTAINER_NAME]], indirect=True)
@pytest.mark.usefixtures("azurite_context")
@pytest.mark.parametrize("test_data_context", [(INPUT_FILE_PATH, TEXT, OUTPUT_FOLDER)], indirect=True)
@pytest.mark.usefixtures("test_data_context")
class TestAzureStorageFileSystem:

    def test_auzre_storage_file_system(self, azurite: str) -> None:
        fs = AzureStorageFileSystem(BlobServiceClient.from_connection_string(azurite), CONTAINER_NAME)

        # Test upload
        assert len(fs.list_folder("/")) == 0
        assert fs.upload_file(INPUT_FILE_PATH, "test_data.txt") is True
        assert len(fs.list_folder("/")) == 1
        assert fs.upload_file(INPUT_FILE_PATH, "/test_data.txt") is True
        assert len(fs.list_folder("/")) == 1
        assert fs.upload_file(INPUT_FILE_PATH, "/test/test_data.txt") is True
        assert len(fs.list_folder("/")) == 2
        assert fs.upload_file(INPUT_FILE_PATH, "test/test_data.txt") is True
        assert len(fs.list_folder("/")) == 2
        assert fs.upload_file(INPUT_FILE_PATH, "/test/test/test_data.txt") is True

        # Test list
        assert len(fs.list_folder("")) == 3
        assert len(fs.list_folder("/")) == 3
        assert len(fs.list_folder("/test")) == 2
        assert len(fs.list_folder("/test/test")) == 1
        assert len(fs.list_folder("/test", False)) == 1

        # Test download
        expected = [
            {
                "file_name": "test_data.txt",
                "file_size": 11,
                "folder": "test/test",
                "file_id": "xxxxxxxx",
                "file_path": "test/test/test_data.txt",
            },
            {
                "file_name": "test_data.txt",
                "file_size": 11,
                "folder": "test",
                "file_id": "xxxxxxxx",
                "file_path": "test/test_data.txt",
            },
            {
                "file_name": "test_data.txt",
                "file_size": 11,
                "folder": "",
                "file_id": "xxxxxxxx",
                "file_path": "test_data.txt",
            },
        ]
        file_checker(fs.list_folder("/"), expected, TEXT, fs, OUTPUT_FOLDER)
