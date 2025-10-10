from injector import Module, provider, singleton

from demo.carrier.dna_carrier import DNACarrier
from demo.carrier.dna_local_carrier import DNALocalCarrier
from demo.carrier.dna_ray_carrier import DNARayCarrier
from demo.carrier.dna_spark_carrier import DNASparkCarrier
from demo.modules.constants import DestinationFileSystem, SourceFileSystem


class CarrierModule(Module):
    SPARK = "spark"
    RAY = "ray"
    LOCAL = "local"

    def __init__(self, mode: str):
        self.__mode = mode

    @singleton
    @provider
    def provide_carrier(self, source: SourceFileSystem, dest: DestinationFileSystem) -> DNACarrier:
        if self.__mode == self.LOCAL:
            return DNALocalCarrier(source, dest)
        if self.__mode == self.SPARK:
            return DNASparkCarrier(source, dest)
        if self.__mode == self.RAY:
            return DNARayCarrier(source, dest)
        raise RuntimeError(f"{self.__mode} is not a valid mode")
