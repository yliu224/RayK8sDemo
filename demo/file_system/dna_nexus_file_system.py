import os
from typing import Any, List

import dxpy
from dxpy.bindings.dxfile_functions import download_dxfile

from demo.file_system.file_info import FileInfo
from demo.file_system.file_system import FileSystem


class DNANexusFileSystem(FileSystem):
    def __init__(self, project: str, token: str):
        super().__init__()
        auth_token = {"auth_token_type": "Bearer", "auth_token": token}
        dxpy.set_security_context(auth_token)
        self.__project_id = dxpy.find_one_project(level="VIEW", name=project, name_mode="regexp")["id"]
        self.__project_name = project

    def list_folder(self, folder_path: str, recursive: bool = True) -> List[FileInfo]:
        """
        TODO: Need a sample project with over 1k files and test the find_data_objects() pagination feature
        TODO: For delta processing, we can filter things by tags/visibility and datetime
        :param folder_path:
        :param recursive:
        :return:
        """
        objs = dxpy.find_data_objects(
            classname="file",
            first_page_size=1000,
            state="closed",
            describe={"fields": {"id": True, "name": True, "folder": True, "parts": True}},
            project=self.__project_id,
            folder=folder_path,
            recurse=recursive,
        )
        return [self.__extract_file_info(obj["describe"]) for obj in list(objs)]

    def download_file(self, file_info: FileInfo, destination: str) -> bool:
        os.makedirs(destination, exist_ok=True)
        download_dxfile(
            dxid=file_info.file_id,
            filename=f"{destination}/{file_info.file_name}",
            project=self.__project_id,
            show_progress=True,
        )
        return True

    def upload_file(self, source_path: str, destination_path: str) -> bool:
        raise NotImplementedError("We don't support DNA Nexus upload for now")

    def upload_files(self, source_folder: str, destination_path: str) -> bool:
        raise NotImplementedError("We don't support DNA Nexus upload for now")

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
