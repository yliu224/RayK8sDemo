import logging
import os
import shutil

from demo.carrier.dna_carrier import DNACarrier

LOG = logging.getLogger(__name__)


class DNALocalCarrier(DNACarrier):

    def move_folder(self, source_folder: str, dest_folder: str, recursive: bool = True) -> None:
        tmp_folder = self.generate_tmp_folder()
        files = self.source.list_folder(source_folder, recursive)
        for idx, f in files:
            self.source.download_file(f, tmp_folder)
            LOG.info(f"Download file No.{idx} from {self.source_name}:{f.file_path} to {tmp_folder}")
            self.dest.upload_file(os.path.join(tmp_folder, f.file_name), self.mapper.map(f, dest_folder))
            LOG.info(f"Upload file No.{idx} from {tmp_folder} to {self.dest_name}:{self.mapper.map(f, dest_folder)}")
        shutil.rmtree(tmp_folder)
        LOG.info(f"Cleaned up {tmp_folder}")
