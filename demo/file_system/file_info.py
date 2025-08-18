from dataclasses import dataclass, field


@dataclass(frozen=True)
class FileInfo:
    file_name: str
    file_size: int
    folder: str
    file_id: str
    file_path: str = field(init=False)

    def __post_init__(self) -> None:
        # Compute file_path after the object is created
        object.__setattr__(self, "file_path", f"{self.folder}/{self.file_name}")
