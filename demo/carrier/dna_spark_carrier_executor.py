import logging
import os.path
import shutil

from demo.carrier.dna_carrier import DNACarrier
from demo.file_system.file_info import FileInfo
from demo.modules.constants import DestinationFileSystem, SourceFileSystem
from demo.path_mapper.path_mapper import PathMapper

LOG = logging.getLogger(__name__)


class DNASparkCarrierExecutor:
    def __init__(self, source: SourceFileSystem, dest: DestinationFileSystem, mapper: PathMapper, dest_folder: str):
        self.__source = source
        self.__dest = dest
        self.__dest_folder = dest_folder
        self.__mapper = mapper

    def spark_download(self, file_info: FileInfo) -> str:
        tmp_folder = DNACarrier.generate_tmp_folder()
        try:
            self.__source.download_file(file_info, tmp_folder)
            self.__dest.upload_file(
                os.path.join(tmp_folder, file_info.file_name), self.__mapper.map(file_info, self.__dest_folder)
            )
            return "True"
        except Exception as e:
            LOG.exception(e)
            return str(e)
        finally:
            shutil.rmtree(tmp_folder)
            LOG.info(f"Cleaned up {tmp_folder}")
