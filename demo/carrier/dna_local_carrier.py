import logging
import shutil

from demo.carrier.dna_carrier import DNACarrier

LOG = logging.getLogger(__name__)


class DNALocalCarrier(DNACarrier):

    def move_folder(self, source_folder: str, dest_folder: str, recursive: bool = True) -> None:
        files = self.source.list_folder(source_folder, recursive)
        LOG.info(f"Found {len(files)} files from {self.source_name}:{source_folder}")
        self.source.download_files(files, self.tmp_folder)
        LOG.info(f"Download {len(files)} files from {self.source_name}:{source_folder} to {self.tmp_folder}")
        self.dest.upload_files(self.tmp_folder, dest_folder)
        LOG.info(f"Upload {len(files)} files from {self.tmp_folder} to {self.dest_name}:{dest_folder}")

        shutil.rmtree(self.tmp_folder)
        LOG.info(f"Cleaned up {self.tmp_folder}")
