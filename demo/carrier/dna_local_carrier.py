import logging
import os
import shutil

from demo.carrier.dna_carrier import DNACarrier

LOG = logging.getLogger(__name__)


class DNALocalCarrier(DNACarrier):

    def move_folder(self, source_folder: str, dest_folder: str, recursive: bool = True) -> None:
        tmp_folder = self.generate_tmp_folder()
        files = self.source.list_folder(source_folder, recursive)
        LOG.info(f"Found {len(files)} files from {self.source_name}:{source_folder}")

        for f in files:
            self.source.download_file(f, tmp_folder)
            LOG.info(f"Download file from {self.source_name}:{source_folder} to {tmp_folder}")
            self.dest.upload_file(os.path.join(tmp_folder, f.file_name), self.mapper.map(f, dest_folder))
            LOG.info(f"Upload file from {tmp_folder} to {self.dest_name}:{dest_folder}")
        shutil.rmtree(tmp_folder)
        LOG.info(f"Cleaned up {tmp_folder}")
