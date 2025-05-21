import desbordante
from pandas import DataFrame
from abc import ABC, abstractmethod
from typing import Any, List, Dict


from desbordante_profiler_package.core.enums import AlgorithmFamily

VERIFICATION_FAMILIES = [AlgorithmFamily.fd, AlgorithmFamily.afd, AlgorithmFamily.ucc, AlgorithmFamily.aucc,
                         AlgorithmFamily.dc, AlgorithmFamily.ind, AlgorithmFamily.aind]

class VerificationAlgorithmInterface(ABC):
    """Abstract base class for verification algorithms."""

    @abstractmethod
    def load_data(self, data: DataFrame) -> None:
        """Loads data into the verification algorithm instance."""
        pass

    @abstractmethod
    def get_broken_primitives(self, primitives_list: List[Any]) -> List[Dict[str, Any]]:
        """Identifies which of the given primitives are not holding in the loaded data."""
        pass

    def run(self, data: DataFrame, primitives_list: List[Any]) -> List[Dict[str, Any]]:
        """Runs the full verification: load data and get broken primitives."""
        self.load_data(data)
        return self.get_broken_primitives(primitives_list)

class VerificationApproximateAlgorithmInterface(ABC):
    """Abstract base class for approximate verification algorithms."""

    @abstractmethod
    def load_data(self, data: DataFrame) -> None:
        """Loads data into the verification algorithm instance."""
        pass

    @abstractmethod
    def get_broken_primitives(self, primitives_list: List[Any], error: float) -> List[Dict[str, Any]]:
        """Identifies which of the given approximate primitives are broken beyond an error threshold."""
        pass

    def run(self, data: DataFrame, primitives_list: List[Any], error: float) -> List[Dict[str, Any]]:
        """Runs the full approximate verification: load data and get broken primitives."""
        self.load_data(data)
        return self.get_broken_primitives(primitives_list, error)



class FDVerificationAlgorithm(VerificationAlgorithmInterface):

    def __init__(self):
        self.instance = desbordante.fd_verification.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def get_broken_primitives(self, fd_list):
        broken = []

        for fd in fd_list:
            lhs = list(fd.lhs_indices)
            rhs = [fd.rhs_index]
            if len(lhs) == 0:
                continue # [] -> [*] is unsupported by fd_verification
            self.instance.execute(lhs_indices=lhs, rhs_indices=rhs)
            if self.instance.fd_holds():
                continue
            else:
                broken.append({
                    "Broken FD": fd.to_long_string(),
                    "Number of error clusters": self.instance.get_num_error_clusters(),
                    "Number of error rows": self.instance.get_num_error_rows()
                })
        return broken

class AFDVerificationAlgorithm(VerificationApproximateAlgorithmInterface):

    def __init__(self):
        self.instance = desbordante.fd_verification.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def get_broken_primitives(self, fd_list, error):
        broken = []

        for fd in fd_list:
            lhs = list(fd.lhs_indices)
            rhs = [fd.rhs_index]
            if len(lhs) == 0:
                continue # [] -> [*] is unsupported by fd_verification
            self.instance.execute(lhs_indices=lhs, rhs_indices=rhs)
            if self.instance.get_error() <= error:
                continue
            else:
                broken.append({
                    "Broken AFD": fd.to_long_string(),
                    "Number of error clusters": self.instance.get_num_error_clusters(),
                    "Number of error rows": self.instance.get_num_error_rows()
                })
        return broken

class DCVerificationAlgorithm(VerificationAlgorithmInterface):

    def __init__(self):
        self.instance = desbordante.dc_verification.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def get_broken_primitives(self, dc_list):
        broken = []

        for dc in dc_list:
            self.instance.execute(denial_constraint=str(dc), do_collect_violations=True)
            if self.instance.dc_holds():
                continue
            else:
                broken.append({
                    "Broken DC": str(dc),
                    "Violations": self.instance.get_violations()
                })
        return broken

class UCCVerificationAlgorithm(VerificationAlgorithmInterface):

    def __init__(self):
        self.instance = desbordante.ucc_verification.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def get_broken_primitives(self, ucc_list):
        broken = []

        for ucc in ucc_list:
            self.instance.execute(ucc_indices=ucc.indices)
            if self.instance.ucc_holds():
                continue
            else:
                broken.append({
                    "Broken UCC": ucc.to_long_string(),
                    "Number of clusters violating UCC": self.instance.get_num_clusters_violating_ucc(),
                    "Clusters violating UCC": self.instance.get_clusters_violating_ucc(),
                    "Number of rows violating UCC": self.instance.get_num_rows_violating_ucc()
                })
        return broken

class AUCCVerificationAlgorithm(VerificationApproximateAlgorithmInterface):

    def __init__(self):
        self.instance = desbordante.ucc_verification.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=data)

    def get_broken_primitives(self, ucc_list, error):
        broken = []

        for ucc in ucc_list:
            self.instance.execute(ucc_indices=ucc.indices)
            if self.instance.get_error() <= error:
                continue
            else:
                broken.append({
                    "Broken AUCC": ucc.to_long_string(),
                    "Number of clusters violating AUCC": self.instance.get_num_clusters_violating_ucc(),
                    "Clusters violating AUCC": self.instance.get_clusters_violating_ucc(),
                    "Number of rows violating AUCC": self.instance.get_num_rows_violating_ucc()
                })
        return broken

class INDVerificationAlgorithm(VerificationAlgorithmInterface):

    def __init__(self):
        self.instance = desbordante.ind_verification.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=[data])

    def get_broken_primitives(self, ind_list):
        broken = []

        for ind in ind_list:
            self.instance.execute(lhs_indices=list(ind.get_lhs), rhs_indices=list(ind.get_rhs))
            if self.instance.ind_holds():
                continue
            else:
                broken.append({
                    "Broken IND": ind.to_long_string(),
                    "Number of clusters violating IND": self.instance.get_violating_clusters_count(),
                    "Clusters violating IND": self.instance.get_violating_clusters(),
                    "Number of rows violating IND": self.instance.get_violating_rows_count()
                })
        return broken

class AINDVerificationAlgorithm(VerificationApproximateAlgorithmInterface):

    def __init__(self):
        self.instance = desbordante.ind_verification.algorithms.Default()

    def load_data(self, data: DataFrame) -> None:
        self.instance.load_data(table=[data])

    def get_broken_primitives(self, ind_list, error):
        broken = []

        for ind in ind_list:
            self.instance.execute(lhs_indices=list(ind.get_lhs), rhs_indices=list(ind.get_rhs))
            if self.instance.get_error() <= error:
                continue
            else:
                broken.append({
                    "Broken AIND": ind.to_long_string(),
                    "Number of clusters violating AIND": self.instance.get_violating_clusters_count(),
                    "Clusters violating AIND": self.instance.get_violating_clusters(),
                    "Number of rows violating AIND": self.instance.get_violating_rows_count()
                })
        return broken

def create_verification_algorithm(family: str):
    family_lower = family.lower()
    match family_lower:
        case "fd":
            return FDVerificationAlgorithm()
        case "afd":
            return AFDVerificationAlgorithm()
        case "dc":
            return DCVerificationAlgorithm()
        case "ucc":
            return UCCVerificationAlgorithm()
        case "aucc":
            return AUCCVerificationAlgorithm()
        case "ind":
            return INDVerificationAlgorithm()
        case "aind":
            return AINDVerificationAlgorithm()
        case _:
            raise ValueError(f"Unsupported family for verification algorithms: {family_lower}")
