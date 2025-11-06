import os
from typing import Any, Iterator, Tuple

import dxpy
from dxpy.bindings.dxfile_functions import download_dxfile

from demo.file_system.file_info import FileInfo
from demo.file_system.file_system import FileSystem
from demo.modules.constants import DxApiToken


class DNANexusFileSystem(FileSystem):
    def __init__(self, project: str, token: DxApiToken):
        super().__init__()
        self.__auth_token = {"auth_token_type": "Bearer", "auth_token": token}
        dxpy.set_security_context(self.__auth_token)
        self.__project_id = dxpy.find_one_project(level="VIEW", name=project, name_mode="regexp")["id"]
        self.__project_name = project

    def list_folder(self, folder_path: str, recursive: bool = True) -> Iterator[Tuple[int, FileInfo]]:
        # This API will automatically handle pagination, the init page size is 100
        # and it multiplies by 2 for each subsequent. The max page size is 1000
        objs = dxpy.find_data_objects(
            classname="file",
            state="closed",
            describe={"fields": {"id": True, "name": True, "folder": True, "parts": True}},
            project=self.__project_id,
            folder=folder_path,
            recurse=recursive,
        )
        for idx, obj in enumerate(objs):
            yield idx, self.__extract_file_info(obj["describe"])

    def download_file(self, file_info: FileInfo, destination: str) -> bool:
        dxpy.set_security_context(self.__auth_token)
        os.makedirs(destination, exist_ok=True)
        download_dxfile(
            dxid=file_info.file_id,
            filename=f"{destination}/{file_info.file_name}",
            project=self.__project_id,
        )
        return True

    def upload_file(self, source_path: str, destination_path: str) -> bool:
        dxpy.set_security_context(self.__auth_token)
        folder = os.path.dirname(destination_path)
        filename = os.path.basename(destination_path)
        with open(source_path, "rb") as source_file:
            dxpy.upload_local_file(file=source_file, project=self.__project_id, folder=folder, name=filename)
        return True

    def upload_files(self, source_folder: str, destination_folder: str) -> bool:
        success = True
        for root, _, files in os.walk(source_folder):
            for name in files:
                abs_path = os.path.abspath(os.path.join(root, name))
                success &= self.upload_file(abs_path, os.path.join(destination_folder, name))
        return success

    def get_file_system_name(self) -> str:
        return self.__project_name

    @staticmethod
    def __extract_file_info(f_details: Any) -> FileInfo:
        # Symlinks do not contain parts
        total_size = 0
        parts = f_details["parts"]
        for v in parts.values():
            total_size += v["size"]

        return FileInfo(f_details["name"], total_size, f_details["folder"], f_details["id"])
