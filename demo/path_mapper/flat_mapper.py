from demo.file_system.file_info import FileInfo
from demo.path_mapper.path_mapper import PathMapper


class FlatMapper(PathMapper):
    def map(self, file_info: FileInfo, dest_folder: str) -> str:
        return f"{dest_folder.rstrip("/")}/{file_info.file_name}"
