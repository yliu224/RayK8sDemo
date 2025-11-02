from abc import ABC, abstractmethod

from demo.file_system.file_info import FileInfo


class PathMapper(ABC):
    @abstractmethod
    def map(self, file_info: FileInfo, dest_folder: str) -> str:
        raise NotImplementedError()
