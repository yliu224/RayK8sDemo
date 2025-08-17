from dataclasses import dataclass, field


@dataclass
class FileInfo:
    # private fields
    __file_name: str = field(repr=False)
    __file_size: int = field(repr=False)
    __file_path: str = field(repr=False)

    # file_name property
    @property
    def file_name(self) -> str:
        return self.__file_name

    # file_size property
    @property
    def file_size(self) -> int:
        return self.__file_size

    @file_size.setter
    def file_size(self, value: int) -> None:
        if value < 0:
            raise ValueError("file_size must be non-negative")
        self.__file_size = value

    # file_path property
    @property
    def file_path(self) -> str:
        return self.__file_path
