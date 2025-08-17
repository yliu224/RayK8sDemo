from abc import ABC, abstractmethod
from typing import List

from demo.file_system.file_info import FileInfo


class FileSystem(ABC):
    """Abstract interface for file system operations."""

    @abstractmethod
    def list_folder(self, folder_path: str, recursive: bool = True) -> List[FileInfo]:
        """
        List contents of a folder.

        Args:
            folder_path (str): Path to the folder to list. Empty string for base directory.
            recursive (bool): Controlling if the look-up will be recursive or not

        Returns:
            List[FileInfo]: List of dictionaries containing file/folder information.
                Each dict should contain: name, path, type, size (for files), modified
        """

    @abstractmethod
    def download_file(self, file_path: str, destination: str) -> bool:
        """
        Download a file from the file system to a destination.

        Args:
            file_path (str): Path to the source file.
            destination (str): Destination path for the downloaded file.

        Returns:
            bool: True if successful, False otherwise.
        """

    @abstractmethod
    def upload_file(self, source_path: str, destination_path: str = "") -> bool:
        """
        Upload a file to the file system.

        Args:
            source_path (str): Path to the source file to upload.
            destination_path (str): Destination path in the file system.
                                  If empty, uses the source filename.

        Returns:
            bool: True if successful, False otherwise.
        """
