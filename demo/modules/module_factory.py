from argparse import Namespace
from typing import List

from injector import Module

from demo.config.stage.stage_metadata import StageMetadata
from demo.modules.carrier_module import CarrierModule
from demo.modules.constants import LANDING
from demo.modules.file_system_module import FileSystemModule
from demo.modules.secret_fetcher_module import SecretFetcherModule


class ModuleFactory:
    @staticmethod
    def create_modules(stage_metadata: StageMetadata, args: Namespace) -> List[Module]:
        modules = [
            FileSystemModule(stage_metadata),
            CarrierModule(mode=str(args.mode).lower()),
        ]
        if stage_metadata.stage == LANDING:
            modules.append(SecretFetcherModule(stage_metadata))

        return modules
