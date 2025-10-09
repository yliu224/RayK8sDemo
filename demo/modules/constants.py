from typing import NewType

from demo.file_system.file_system import FileSystem

SourceFileSystem = NewType("SourceFileSystem", FileSystem)
DestinationFileSystem = NewType("DestinationFileSystem", FileSystem)

DxApiToken = NewType("DxApiToken", str)

LANDING = "landing"
DISPATCH = "dispatch"
EMBASSY = "embassy"
NOTIFICATION = "notification"
