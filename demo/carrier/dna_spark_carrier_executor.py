import logging
import os.path
import shutil

from pyspark.sql.types import Row

from demo.carrier.dna_carrier import DNACarrier
from demo.file_system.file_info import FileInfo
from demo.modules.constants import DestinationFileSystem, SourceFileSystem

LOG = logging.getLogger(__name__)


class DNASparkCarrierExecutor:
    def __init__(self, source: SourceFileSystem, dest: DestinationFileSystem, dest_folder: str):
        self.source = source
        self.dest = dest
        self.dest_folder = dest_folder

    def spark_download(self, row: Row) -> Row:
        tmp_folder = DNACarrier.generate_tmp_folder()
        try:
            file_info = FileInfo.from_json(row.FileInfo)
            self.source.download_file(file_info, tmp_folder)
            self.dest.upload_file(
                os.path.join(tmp_folder, file_info.file_name), os.path.join(self.dest_folder, file_info.file_name)
            )
            return Row(Message="True")
        except Exception as e:
            LOG.exception(e)
            return Row(Message=e)
        finally:
            shutil.rmtree(tmp_folder)
            LOG.info(f"Cleaned up {tmp_folder}")
