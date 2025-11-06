import logging
import os.path
import shutil
import socket

import ray

from demo.carrier.dna_carrier import DNACarrier
from demo.file_system.file_info import FileInfo

LOG = logging.getLogger(__name__)


class DNARayCarrier(DNACarrier):
    MAX_ACTIVE_TASKS = 1000  # TODO: Move this to config file maybe?
    PROGRESS_LOG_INTERVAL = 100  # TODO: Move this to config file maybe?

    def move_folder(self, source_folder: str, dest_folder: str, recursive: bool = True) -> None:
        files = self.source.list_folder(source_folder, recursive)
        LOG.info(f"Starts moving files from {self.source_name}:{source_folder}")
        ray.init()

        pending = []
        success = 0
        total = 0

        for i, file in files:
            pending.append(self.remote_move.remote(self, file, dest_folder))
            total = i
            if total % self.PROGRESS_LOG_INTERVAL == 0 and total > 0:
                LOG.info(f"Submitted {total} files so far...")
            if len(pending) >= self.MAX_ACTIVE_TASKS:
                done, pending = ray.wait(pending, num_returns=1)
                success += ray.get(done).count(True)

        success += ray.get(pending).count(True)
        LOG.info(
            f"{success}/{total+1} successfully moved from {self.source_name}:{source_folder} "
            f"to {self.dest_name}:{dest_folder}"
        )

    @ray.remote
    def remote_move(self, source_file: FileInfo, dest_folder: str) -> bool:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger("azure").setLevel(logging.WARNING)
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
            LOG.debug(f"Cleaned up {tmp_folder}")
