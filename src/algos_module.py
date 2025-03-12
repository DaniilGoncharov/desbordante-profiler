import pandas as pd
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import desbordante



class AlgorithmInterface(ABC):

    @abstractmethod
    def load_data(self, data: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def execute(self) -> None:
        pass

    @abstractmethod
    def get_results(self) -> Any:
        pass

    def run(self, data: pd.DataFrame) -> Any:
        self.load_data(data)
        self.execute()
        return self.get_results()


class FDAlgorithm(AlgorithmInterface):

    FD_ALGORITHMS = {
        "hyfd": desbordante.fd.algorithms.HyFD,
        "fd_mine": desbordante.fd.algorithms.FdMine,
        "dfd": desbordante.fd.algorithms.DFD,
        "default": desbordante.fd.algorithms.Default
    }

    def __init__(self, algo_name: str, parameters: Dict[str, Any] = None):
        self.algo_name = algo_name.lower()
        self.parameters = parameters or {}
        algo_class = self.FD_ALGORITHMS.get(self.algo_name)
        if not algo_class:
            raise ValueError(f"Unknown FD algorithm: {self.algo_name}")
        self.instance = algo_class()

    def load_data(self, data: pd.DataFrame) -> None:
        self.instance.load_data(table=data, **self.parameters)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> List[str]:
        fds = self.instance.get_fds()
        return fds


class AFDAlgorithm(AlgorithmInterface):

    AFD_ALGO_MAP = {
        "pyro": desbordante.afd.algorithms.Pyro,
        "tane": desbordante.afd.algorithms.Tane,
        "default": desbordante.afd.algorithms.Default
    }

    def __init__(self, algo_name: str, parameters: Dict[str, Any] = None):
        self.algo_name = algo_name.lower()
        self.parameters = parameters or {}
        algo_class = self.AFD_ALGO_MAP.get(self.algo_name)
        if not algo_class:
            raise ValueError(f"Unknown AFD algorithm: {self.algo_name}")
        self.instance = algo_class()

    def load_data(self, data: pd.DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> List[str]:
        fds = self.instance.get_fds()
        return [str(fd) for fd in fds]


class CFDAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Dict[str, Any] = None):
        self.parameters = parameters or {}
        self.instance = desbordante.cfd.algorithms.Default()

    def load_data(self, data: pd.DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> List[str]:
        cfds = self.instance.get_cfds()
        return cfds


class INDAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Dict[str, Any] = None):
        self.parameters = parameters or {}
        self.instance = desbordante.ind.algorithms.Default()

    def load_data(self, data: Any) -> None:
        self.instance.load_data(tables=[data])

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> List[str]:
        inds = self.instance.get_inds()
        return inds


class UCCAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Dict[str, Any] = None):
        self.parameters = parameters or {}
        self.instance = desbordante.ucc.algorithms.Default()

    def load_data(self, data: pd.DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> List[str]:
        uccs = self.instance.get_uccs()
        return uccs


class DDAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Dict[str, Any] = None):
        self.parameters = parameters or {}
        self.instance = desbordante.dd.algorithms.Split()

    def load_data(self, data: pd.DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> List[str]:
        dds = self.instance.get_dds()
        return dds


class ARAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Dict[str, Any] = None):
        self.parameters = parameters or {}
        self.instance = desbordante.ar.algorithms.Default()

    def load_data(self, data: pd.DataFrame) -> None:
        self.instance.load_data(table=data, **self.parameters)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> List[str]:
        ars = self.instance.get_ars()
        return ars


class ODAlgorithm(AlgorithmInterface):

    def __init__(self, algo_name: str = "fastod", parameters: Dict[str, Any] = None):
        self.parameters = parameters or {}
        self.algo_name = algo_name
        if algo_name.lower() == "fastod":
            self.instance = desbordante.od.algorithms.Fastod()
        elif algo_name.lower() == "order":
            self.instance = desbordante.od.algorithms.Order()
        else:
            raise ValueError(f"Unsupported OD algorithm: {algo_name}")

    def load_data(self, data: pd.DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> List[str]:
        if self.algo_name.lower() == "fastod":
            ods = self.instance.get_asc_ods() + self.instance.get_desc_ods() + self.instance.get_simple_ods()
        elif self.algo_name.lower() == "order":
            ods = self.instance.get_list_ods()
        return ods



def create_algorithm(family: str, algo_name: str, parameters: Dict[str, Any]) -> AlgorithmInterface:

    family_lower = family.lower()
    match family_lower:
        case "fd":
            return FDAlgorithm(algo_name, parameters)
        case "afd":
            return AFDAlgorithm(algo_name, parameters)
        case "cfd":
            return CFDAlgorithm(parameters)
        case "ind":
            return INDAlgorithm(parameters)
        case "ucc":
            return UCCAlgorithm(parameters)
        case "dd":
            return DDAlgorithm(parameters)
        case "ar":
            return ARAlgorithm(parameters)
        case "od":
            return ODAlgorithm(algo_name, parameters)
        case _:
            raise ValueError(f"Unsupported family: {family_lower}")
