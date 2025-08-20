import logging

from injector import Injector

from demo.carrier.dna_carrier import DNACarrier
from demo.carrier.dna_ray_carrier import DNARayCarrier
from demo.carrier.dna_spark_carrier import DNASparkCarrier
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
            CarrierModule(mode=CarrierModule.SPARK),
        ]
    )
    carrier = injector.get(DNACarrier)
    carrier.move_folder("/resources", "test/spark")


if __name__ == "__main__":
    main()
