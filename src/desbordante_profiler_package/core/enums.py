from enum import StrEnum, auto

class AlgorithmFamily(StrEnum):
    fd = auto()
    cfd = auto()
    ar = auto()
    afd = auto()
    sfd = auto()
    od = auto()
    ind = auto()
    aind = auto()
    dd = auto()
    ucc = auto()
    aucc = auto()
    md = auto()
    nar = auto()
    dc = auto()
    ac = auto()

class Algorithm(StrEnum):
    default = auto()
    split = auto()
    apriori = auto()
    fastod = auto()
    order = auto()
    fd_first = auto()
    pyroucc = auto()
    hpivalid = auto()
    hyucc = auto()
    spider = auto()
    faida = auto()
    pyro = auto()
    tane = auto()
    hyfd = auto()
    dfd = auto()
    aid = auto()
    depminer = auto()
    eulerfd = auto()
    fastfds = auto()
    fdep = auto()
    fun = auto()
    pfdtane = auto()
    des = auto()
    fastadc = auto()
    acalgorithm = auto()
    sfdalgorithm = auto()
    hymd = auto()

class ProfileParameter(StrEnum):
    name = auto()
    global_settings = auto()
    rows = auto()
    columns = auto()
    global_timeout = auto()
    tasks = auto()
    family = auto()
    algorithm = auto()
    parameters = auto()
    timeout = auto()

class AlgorithmParameter(StrEnum):
    error = auto()
    threads = auto()
    
class DictionaryField(StrEnum):
    runs = auto()
    run_id = auto()
    task_id = auto()
    algorithm = auto()
    algorithm_family = auto()
    params = auto()
    rows = auto()
    cols = auto()
    timestamp_start = auto()
    result = auto()
    data_hash = auto()
    timestamp_end = auto()
    execution_time = auto()
    result_path = auto()
    instances = auto()
    error_type = auto()
    rules_decision = auto()
    baseline_instances = auto()
    target_instances = auto()
    comparison = auto()

class TaskStatus(StrEnum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values):
        return name # to make this class case-sensitive

    Success = auto()
    Failure = auto()
    NotFinished = auto()
    MemoryError = auto()
    Error = auto()
    NotStarted = auto()
    StartingFailure = auto()
    Running = auto()
    Timeout = auto()
    GlobalTimeout = auto()
    Killed = auto()
    Cancelled = auto()

class Strategy(StrEnum):
    auto_decision = auto()
    ask = auto()
    timeout_grow = auto()
    shrink_search = auto()
    single_run = auto()

class RulesField(StrEnum):
    task = auto()
    action = auto()
    retry_params = auto()
    
class RulesAction(StrEnum):
    retry = auto()
    skip = auto()
    prune = auto()
    
class RulesRetryParameter(StrEnum):
    new_timeout = auto()
    new_dataframe = auto()
    
# rules