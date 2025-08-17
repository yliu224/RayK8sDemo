from typing import List

import dxpy

from demo.file_system.file_info import FileInfo
from demo.file_system.file_system import FileSystem


class DNANexusFileSystem(FileSystem):

    def __init__(self, token: str):
        super().__init__()
        auth_token = {"auth_token_type": "Bearer", "auth_token": token}
        dxpy.set_security_context(auth_token)

    def list_folder(self, folder_path: str, recursive: bool = True) -> List[FileInfo]:
        return []

    def download_file(self, file_path: str, destination: str) -> bool:
        return True

    def upload_file(self, source_path: str, destination_path: str = "") -> bool:
        return True
