import sys
import pandas
import logging
import warnings
import hashlib

from typing import Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_HASH_ALGORITHM = "SHA256"
DEFAULT_BLOCK_SIZE = 65536 # 64 KB

def get_dataframe_and_hash(
    filepath: str | Path,
    delimiter: str,
    has_header: bool,
    rows: Optional[int],
    cols: Optional[int]
) -> Tuple[pandas.DataFrame, Optional[str]]:
    """Loads CSV data into a pandas DataFrame and calculates its hash."""
    logger.info(f"Loading CSV from {filepath}")
    header = 0 if has_header else None
    try:
        with warnings.catch_warnings(record=True) as w_list:
            warnings.simplefilter('always')
            initial_df = pandas.read_csv(filepath, sep=delimiter, header=header)

            for warning in w_list:
                logger.warning(f"Warning while loading CSV file: {warning.message}")

            logger.info(f"Successfully loaded CSV with {initial_df.shape[0]} rows and {initial_df.shape[1]} columns.")
    except Exception as e:
        logger.error(f"Failed to load CSV file {filepath}: {e}")
        sys.exit(1)

    df_hash = calculate_data_hash(filepath)

    n_rows = initial_df.shape[0] if rows is None else min(rows, initial_df.shape[0])
    n_cols = initial_df.shape[1] if cols is None else min(cols, initial_df.shape[1])
    sliced_df = initial_df.iloc[:n_rows, :n_cols]
    return sliced_df, df_hash


def calculate_data_hash(
    filepath: str | Path,
    hash_algorithm: str = DEFAULT_HASH_ALGORITHM,
    block_size: int = DEFAULT_BLOCK_SIZE
) -> Optional[str]:
    """Calculates the hash of a file."""
    hasher = hashlib.new(hash_algorithm)
    try:
        with open(filepath, 'rb') as f:
            while True:
                block = f.read(block_size)
                if not block:
                    break
                hasher.update(block)
    except Exception as e:
        logger.warning(f"Error calculating data hash: {e}")
        return None
    return hasher.hexdigest()