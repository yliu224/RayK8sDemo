from dataclasses import dataclass
from typing import Optional


@dataclass
class StageMetadata:
    stage: str
    source_connection_str: Optional[str] = None
    source_container: Optional[str] = None
    destination_connection_str: Optional[str] = None
    destination_container: Optional[str] = None
    project: Optional[str] = None
    token: Optional[str] = None
