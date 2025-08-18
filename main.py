import logging

from injector import Injector

from demo.carrier.dna_ray_carrier import DNARayCarrier
from demo.modules.carrier_module import CarrierModule
from demo.modules.file_system_module import FileSystemModule

logging.basicConfig(level=logging.INFO)
logging.getLogger("azure.core").setLevel(logging.WARNING)


def main() -> None:
    injector = Injector(
        [
            FileSystemModule(
                stage=FileSystemModule.LANDING,
            ),
            CarrierModule(mode=CarrierModule.RAY),
        ]
    )
    carrier = injector.get(DNARayCarrier)
    carrier.move_folder("/resources", "test/ray")


if __name__ == "__main__":
    main()
