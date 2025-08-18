import logging
import shutil
import uuid

from injector import inject

from demo.carrier.dna_carrier import DestinationFileSystem, DNACarrier, SourceFileSystem

LOG = logging.getLogger(__name__)


class DNALocalCarrier(DNACarrier):

    @inject
    def __init__(self, source: SourceFileSystem, dest: DestinationFileSystem):
        super().__init__()
        self.__source = source
        self.__dest = dest
        self.__source_name = self.__source.get_file_system_name()
        self.__dest_name = self.__dest.get_file_system_name()
        self.__tmp_folder = f"/tmp/{uuid.uuid4().hex}"

    def move_folder(self, source_folder: str, dest_folder: str, recursive: bool = True) -> None:
        files = self.__source.list_folder(source_folder, recursive)
        LOG.info(f"Found {len(files)} files from {self.__source_name}:{source_folder}")
        self.__source.download_files(files, self.__tmp_folder)
        LOG.info(f"Download {len(files)} files from {self.__source_name}:{source_folder} to {self.__tmp_folder}")
        self.__dest.upload_files(self.__tmp_folder, dest_folder)
        LOG.info(f"Upload {len(files)} files from {self.__tmp_folder} to {self.__dest_name}:{dest_folder}")

        shutil.rmtree(self.__tmp_folder)
        LOG.info(f"Cleaned up {self.__tmp_folder}")
