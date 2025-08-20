import json
from dataclasses import asdict, dataclass, field


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

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> "FileInfo":
        data = json.loads(json_str)
        # remove computed field if present
        data.pop("file_path", None)
        return cls(**data)
