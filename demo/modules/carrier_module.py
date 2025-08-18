from injector import Module, inject, provider, singleton

from demo.carrier.dna_carrier import DestinationFileSystem, DNACarrier, SourceFileSystem
from demo.carrier.dna_local_carrier import DNALocalCarrier


class CarrierModule(Module):
    SPARK = "spark"
    RAY = "ray"
    LOCAL = "local"

    def __init__(self, mode: str):
        self.__mode = mode

    @singleton
    @provider
    @inject
    def provide_carrier(self, source: SourceFileSystem, dest: DestinationFileSystem) -> DNACarrier:
        if self.__mode == self.LOCAL:
            return DNALocalCarrier(source, dest)
        raise RuntimeError(f"{self.__mode} is not a valid mode")
