import argparse
import logging

import dataconf
from injector import Injector
from pyhocon import ConfigFactory

from demo.carrier.dna_carrier import DNACarrier
from demo.config.stage.stage_metadata import StageMetadata
from demo.modules.carrier_module import CarrierModule
from demo.modules.file_system_module import FileSystemModule

logging.basicConfig(level=logging.INFO)
logging.getLogger("azure.core").setLevel(logging.WARNING)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", type=str)
    parser.add_argument("--mode", type=str)
    parser.add_argument("--source_folder", type=str)
    parser.add_argument("--dest_folder", type=str)
    args = parser.parse_args()

    conf = ConfigFactory.parse_file(f"/Users/dean/PycharmProjects/RayK8sDemo/demo/config/stage/{args.stage}/local.conf")
    stage_metadata = dataconf.dict(conf[str(args.mode).lower()], StageMetadata)

    injector = Injector(
        [
            FileSystemModule(stage_metadata),
            CarrierModule(mode=str(args.mode).lower()),
        ]
    )
    carrier = injector.get(DNACarrier)
    carrier.move_folder(args.source_folder, args.dest_folder)


if __name__ == "__main__":
    main()
