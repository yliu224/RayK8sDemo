from demo.file_system.file_info import FileInfo
from demo.path_mapper.path_mapper import PathMapper


class PersonIdMapper(PathMapper):
    def map(self, file_info: FileInfo, dest_folder: str) -> str:
        person_id = self.extract_person_id(file_info)
        return f"{dest_folder}/{person_id}/{file_info.file_name}"

    def extract_person_id(self, _file_info: FileInfo) -> str:
        # TODO: Add logic to extract person ID from file_info
        return "p1"
