import sys
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pandas import DataFrame
import desbordante

from desbordante_profiler_package.core.enums import AlgorithmFamily, Algorithm, AlgorithmParameter

logger = logging.getLogger(__name__)

MINING_FAMILIES = [AlgorithmFamily.fd, AlgorithmFamily.afd, AlgorithmFamily.ind, AlgorithmFamily.aind,
                   AlgorithmFamily.ucc, AlgorithmFamily.aucc, AlgorithmFamily.cfd, AlgorithmFamily.od,
                   AlgorithmFamily.ar, AlgorithmFamily.dd, AlgorithmFamily.nar, AlgorithmFamily.dc,
                   AlgorithmFamily.ac, AlgorithmFamily.sfd, AlgorithmFamily.md]

DEFAULT_ALGORITHMS = {
    AlgorithmFamily.fd: Algorithm.hyfd,
    AlgorithmFamily.afd: Algorithm.pyro,
    AlgorithmFamily.ind: Algorithm.spider,
    AlgorithmFamily.aind: Algorithm.spider,
    AlgorithmFamily.ucc: Algorithm.hpivalid,
    AlgorithmFamily.aucc: Algorithm.pyroucc,
    AlgorithmFamily.od: Algorithm.fastod,
    AlgorithmFamily.ar: Algorithm.apriori,
    AlgorithmFamily.dd: Algorithm.split,
    AlgorithmFamily.cfd: Algorithm.fd_first,
    AlgorithmFamily.nar: Algorithm.des,
    AlgorithmFamily.dc: Algorithm.fastadc,
    AlgorithmFamily.ac: Algorithm.acalgorithm,
    AlgorithmFamily.sfd: Algorithm.sfdalgorithm,
    AlgorithmFamily.md: Algorithm.hymd
}

class AlgorithmInterface(ABC):
    """Abstract base class for mining algorithms."""

    @abstractmethod
    def load_data(self, data: DataFrame) -> None:
        """Loads data into the algorithm instance."""
        pass

    @abstractmethod
    def execute(self) -> None:
        """Executes the mining algorithm."""
        pass

    @abstractmethod
    def get_results(self) -> Dict[str, List[Any]]:
        """Retrieves the results from the algorithm execution."""
        pass

    def run(self, data: DataFrame) -> Dict[str, List[Any]]:
        """Runs the entire process: load data, execute, and get results."""
        self.load_data(data)
        self.execute()
        return self.get_results()


class FDAlgorithm(AlgorithmInterface):

    FD_ALGO_MAP = {
        Algorithm.hyfd: desbordante.fd.algorithms.HyFD,
        Algorithm.dfd: desbordante.fd.algorithms.DFD,
        Algorithm.aid: desbordante.fd.algorithms.Aid,
        Algorithm.depminer: desbordante.fd.algorithms.Depminer,
        Algorithm.eulerfd: desbordante.fd.algorithms.EulerFD,
        Algorithm.fastfds: desbordante.fd.algorithms.FastFDs,
        Algorithm.fdep: desbordante.fd.algorithms.FDep,
        Algorithm.fun: desbordante.fd.algorithms.FUN,
        Algorithm.pfdtane: desbordante.fd.algorithms.PFDTane,
        Algorithm.pyro: desbordante.fd.algorithms.Pyro,
        Algorithm.tane: desbordante.fd.algorithms.Tane,
        Algorithm.default: desbordante.fd.algorithms.Default
    }

    def __init__(self, algo_name: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.algo_name = algo_name.lower()
        self.parameters = parameters or {}
        algo_class = self.FD_ALGO_MAP.get(self.algo_name)
        if not algo_class:
            raise ValueError(f"Unknown FD algorithm: {self.algo_name}")
        self.instance = algo_class()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data, **self.parameters)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "FD": self.instance.get_fds()
        }
        return result


class AFDAlgorithm(AlgorithmInterface):

    AFD_ALGO_MAP = {
        Algorithm.pyro: desbordante.afd.algorithms.Pyro,
        Algorithm.tane: desbordante.afd.algorithms.Tane,
        Algorithm.default: desbordante.afd.algorithms.Default
    }

    def __init__(self, algo_name: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.algo_name = algo_name.lower()
        self.parameters = parameters or {}
        algo_class = self.AFD_ALGO_MAP.get(self.algo_name)
        if not algo_class:
            raise ValueError(f"Unknown AFD algorithm: {self.algo_name}")
        self.instance = algo_class()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "AFD": self.instance.get_fds()
        }
        return result


class CFDAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Dict[str, Any] = None) -> None:
        self.parameters = parameters or {}
        self.instance = desbordante.cfd.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "CFD": self.instance.get_cfds()
        }
        return result


class INDAlgorithm(AlgorithmInterface):

    IND_ALGO_MAP = {
        Algorithm.spider: desbordante.ind.algorithms.Spider,
        Algorithm.faida: desbordante.ind.algorithms.Faida,
        Algorithm.default: desbordante.ind.algorithms.Default
    }

    def __init__(self, algo_name: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.algo_name = algo_name.lower()
        self.parameters = parameters or {}
        algo_class = self.IND_ALGO_MAP.get(self.algo_name)
        if not algo_class:
            raise ValueError(f"Unknown IND algorithm: {self.algo_name}")
        self.instance = algo_class()

    def load_data(self, data: Any) -> None:
        self.instance.load_data(tables=[data])

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "IND": self.instance.get_inds()
        }
        return result

class AINDAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.parameters = parameters or {}
        self.instance = desbordante.ind.algorithms.Spider()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(tables=[data])

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "AIND": self.instance.get_inds()
        }
        return result


class UCCAlgorithm(AlgorithmInterface):

    UCC_ALGO_MAP = {
        Algorithm.pyroucc: desbordante.ucc.algorithms.PyroUCC,
        Algorithm.hyucc: desbordante.ucc.algorithms.HyUCC,
        Algorithm.hpivalid: desbordante.ucc.algorithms.HPIValid,
        Algorithm.default: desbordante.ucc.algorithms.Default
    }

    def __init__(self, algo_name: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.algo_name = algo_name.lower()
        self.parameters = parameters or {}
        algo_class = self.UCC_ALGO_MAP.get(self.algo_name)
        if not algo_class:
            raise ValueError(f"Unknown UCC algorithm: {self.algo_name}")
        self.instance = algo_class()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "UCC": self.instance.get_uccs()
        }
        return result

class AUCCAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.parameters = parameters or {}
        self.instance = desbordante.ucc.algorithms.PyroUCC()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "AUCC": self.instance.get_uccs()
        }
        return result

class DDAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.parameters = parameters or {}
        self.instance = desbordante.dd.algorithms.Split()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "DD": self.instance.get_dds()
        }
        return result


class ARAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.parameters = parameters or {}
        self.instance = desbordante.ar.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data, **self.parameters)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "AR": self.instance.get_ars()
        }
        return result


class ODAlgorithm(AlgorithmInterface):

    OD_ALGO_MAP = {
        Algorithm.fastod: desbordante.od.algorithms.Fastod,
        Algorithm.order: desbordante.od.algorithms.Order,
        Algorithm.default: desbordante.od.algorithms.Default
    }

    def __init__(self, algo_name: str, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.algo_name = algo_name.lower()
        self.parameters = parameters or {}
        algo_class = self.OD_ALGO_MAP.get(self.algo_name)
        if not algo_class:
            raise ValueError(f"Unknown OD algorithm: {self.algo_name}")
        self.instance = algo_class()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {}
        if self.algo_name.lower() == Algorithm.fastod:
            result = {
                "ASC_OD": self.instance.get_asc_ods(),
                "DESC_OD": self.instance.get_desc_ods(),
                "SIMPLE_OD": self.instance.get_simple_ods()
            }
        elif self.algo_name.lower() == Algorithm.order:
            result = {
                "OD": self.instance.get_list_ods()
            }
        return result


class NARAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.parameters = parameters or {}
        self.instance = desbordante.nar.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "NAR": self.instance.get_nars()
        }
        return result

class DCAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.parameters = parameters or {}
        self.instance = desbordante.dc.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "DC": self.instance.get_dcs()
        }
        return result

class ACAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.parameters = parameters or {}
        self.instance = desbordante.ac.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "AC_Ranges": self.instance.get_ac_ranges(),
            "AC_Exceptions": self.instance.get_ac_exceptions()
        }
        return result

class SFDAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.parameters = parameters or {}
        self.instance = desbordante.sfd.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "FD": self.instance.get_fds(),
            "Correlations": self.instance.get_correlations()
        }
        return result

class MDAlgorithm(AlgorithmInterface):

    def __init__(self, parameters: Optional[Dict[str, Any]] = None) -> None:
        self.parameters = parameters or {}
        self.instance = desbordante.md.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(left_table=data)

    def execute(self) -> None:
        self.instance.execute(**self.parameters)

    def get_results(self) -> Dict[str, List[Any]]:
        result = {
            "MD": self.instance.get_mds()
        }
        return result

def create_mining_algorithm(
    family: str,
    algo_name: str,
    parameters: Dict[str, Any]
) -> AlgorithmInterface:
    """Factory function to create a mining algorithm instance."""
    family_lower = family.lower()
    match family_lower:
        case AlgorithmFamily.fd:
            return FDAlgorithm(algo_name, parameters)
        case AlgorithmFamily.afd:
            return AFDAlgorithm(algo_name, parameters)
        case AlgorithmFamily.cfd:
            return CFDAlgorithm(parameters)
        case AlgorithmFamily.ind:
            return INDAlgorithm(algo_name, parameters)
        case AlgorithmFamily.aind:
            return AINDAlgorithm(parameters)
        case AlgorithmFamily.ucc:
            return UCCAlgorithm(algo_name, parameters)
        case AlgorithmFamily.aucc:
            return AUCCAlgorithm(parameters)
        case AlgorithmFamily.dd:
            return DDAlgorithm(parameters)
        case AlgorithmFamily.ar:
            return ARAlgorithm(parameters)
        case AlgorithmFamily.od:
            return ODAlgorithm(algo_name, parameters)
        case AlgorithmFamily.nar:
            return NARAlgorithm(parameters)
        case AlgorithmFamily.dc:
            return DCAlgorithm(parameters)
        case AlgorithmFamily.ac:
            return ACAlgorithm(parameters)
        case AlgorithmFamily.sfd:
            return SFDAlgorithm(parameters)
        case AlgorithmFamily.md:
            return MDAlgorithm(parameters)
        case _:
            logger.error(f"Unsupported mining algorithm family: {family_lower}")
            sys.exit(1)

def get_algorithm_name_by_family(family: AlgorithmFamily) -> Algorithm:
    """Returns the default algorithm name for a given algorithm family."""
    algorithm_name = DEFAULT_ALGORITHMS[family]
    return algorithm_name


def get_family_by_algorithm(algorithm: Algorithm, params: Dict[str, Any]) -> Optional[AlgorithmFamily]:
    """Returns the algorithm family based on the algorithm name and parameters."""
    match algorithm:
        case Algorithm.split:
            family_name = AlgorithmFamily.dd
        case Algorithm.apriori:
            family_name = AlgorithmFamily.ar
        case algorithm if algorithm in (Algorithm.fastod, Algorithm.order):
            family_name = AlgorithmFamily.od
        case Algorithm.fd_first:
            family_name = AlgorithmFamily.cfd
        case Algorithm.pyroucc:
            if params.get(AlgorithmParameter.error, 0):
                family_name = AlgorithmFamily.aucc
            else:
                family_name = AlgorithmFamily.ucc
        case algorithm if algorithm in (Algorithm.hpivalid, Algorithm.hyucc):
            family_name = AlgorithmFamily.ucc
        case Algorithm.spider:
            if params.get(AlgorithmParameter.error, 0):
                family_name = AlgorithmFamily.aind
            else:
                family_name = AlgorithmFamily.ind
        case Algorithm.faida:
            family_name = AlgorithmFamily.ind
        case algorithm if algorithm in (Algorithm.pyro, Algorithm.tane):
            if params.get(AlgorithmParameter.error, 0):
                family_name = AlgorithmFamily.afd
            else:
                family_name = AlgorithmFamily.fd
        case algorithm if algorithm in (Algorithm.hyfd, Algorithm.dfd, Algorithm.aid, Algorithm.depminer,
                                        Algorithm.eulerfd, Algorithm.fastfds, Algorithm.fdep, Algorithm.fun,
                                        Algorithm.pfdtane, Algorithm.pyro, Algorithm.tane):
            family_name = AlgorithmFamily.fd
        case Algorithm.des:
            family_name = AlgorithmFamily.nar
        case Algorithm.fastadc:
            family_name = AlgorithmFamily.dc
        case Algorithm.acalgorithm:
            family_name = AlgorithmFamily.ac
        case Algorithm.sfdalgorithm:
            family_name = AlgorithmFamily.sfd
        case Algorithm.hymd:
            family_name = AlgorithmFamily.md
        case _:
            logger.warning(f"Unsupported mining algorithm: {algorithm}")
            family_name = None
    return family_name
