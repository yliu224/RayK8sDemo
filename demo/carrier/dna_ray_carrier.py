import logging
import os.path
import shutil
import socket

import ray

from demo.carrier.dna_carrier import DNACarrier
from demo.file_system.file_info import FileInfo

LOG = logging.getLogger(__name__)


class DNARayCarrier(DNACarrier):

    def move_folder(self, source_folder: str, dest_folder: str, recursive: bool = True) -> None:
        files = self.source.list_folder(source_folder, recursive)
        LOG.info(f"Found {len(files)} files from {self.source_name}:{source_folder}")
        ray.init()
        remote_move_list = [self.remote_move.remote(self, f, dest_folder) for f in files]
        results = ray.get(remote_move_list)
        total = len(results)
        success = results.count(True)
        LOG.info(
            f"{success}/{total} successfully moved from {self.source_name}:{source_folder} "
            f"to {self.dest_name}:{dest_folder}"
        )
        ray.shutdown()

    @ray.remote
    def remote_move(self, source_file: FileInfo, dest_folder: str) -> bool:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger("azure.core").setLevel(logging.WARNING)
        tmp_folder = self.generate_tmp_folder()
        try:
            host_name = socket.gethostname()
            tmp_file_path = os.path.join(tmp_folder, source_file.file_name)
            dest_file_path = self.mapper.map(source_file, dest_folder)

            self.source.download_file(source_file, tmp_folder)
            LOG.info(f"Download {self.source_name}:{source_file.file_path} to {host_name}:{tmp_file_path}")

            self.dest.upload_file(tmp_file_path, dest_file_path)
            LOG.info(f"Upload {host_name}:{tmp_file_path} to {self.dest_name}:{dest_file_path}/{source_file.file_name}")
            return True
        except Exception as e:
            LOG.exception(e)
            return False
        finally:
            shutil.rmtree(tmp_folder)
            LOG.info(f"Cleaned up {tmp_folder}")
