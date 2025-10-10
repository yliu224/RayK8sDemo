import uuid
from abc import ABC, abstractmethod

from demo.modules.constants import DestinationFileSystem, SourceFileSystem


class DNACarrier(ABC):
    def __init__(self, source: SourceFileSystem, dest: DestinationFileSystem):
        """
        Interface for moving data from a source to a destination.

        Implementations can define how 'source' and 'dest' are interpreted
        (local paths, URLs, object stores, etc.).
        """
        super().__init__()
        self.__source = source
        self.__dest = dest
        self.__source_name = self.__source.get_file_system_name()
        self.__dest_name = self.__dest.get_file_system_name()

    @abstractmethod
    def move_folder(
        self,
        source_folder: str,
        dest_folder: str,
        recursive: bool = True,
        # TODO: Add filter interface if necessary
    ) -> None:
        """
        Move data from 'source' to 'dest'.

        Parameters
        ----------
        source_folder : str
            The source location.
        dest_folder : str
            The destination location.
        recursive : bool
            Controlling if the look-up will be recursive or not
        """
        raise NotImplementedError()

    @property
    def source(self) -> SourceFileSystem:
        return self.__source

    @property
    def dest(self) -> DestinationFileSystem:
        return self.__dest

    @property
    def source_name(self) -> str:
        return self.__source_name

    @property
    def dest_name(self) -> str:
        return self.__dest_name

    @staticmethod
    def generate_tmp_folder() -> str:
        return f"/tmp/{uuid.uuid4().hex}"
