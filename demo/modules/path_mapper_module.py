from injector import Module, provider, singleton

from demo.modules.constants import DISPATCH, EMBASSY, LANDING, NOTIFICATION
from demo.path_mapper.hierarchy_mapper import HierarchyMapper
from demo.path_mapper.path_mapper import PathMapper
from demo.path_mapper.person_id_mapper import PersonIdMapper


class PathMapperModlue(Module):
    def __init__(self, stage: str):
        self.__stage = stage

    @singleton
    @provider
    def provide_path_mapper(self) -> PathMapper:
        if self.__stage in [LANDING, DISPATCH, NOTIFICATION]:
            return HierarchyMapper()
        if self.__stage in [EMBASSY]:
            return PersonIdMapper()
        raise RuntimeError(f"{self.__stage} is not a valid path mapping stage")
