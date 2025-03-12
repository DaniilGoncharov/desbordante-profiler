import pandas as pd
import logging
import warnings

logger = logging.getLogger(__name__)


class CSVDataSource:
    """Handles loading CSV data with configurable options."""

    def __init__(self, file_path: str, delimiter: str = ',', has_header: bool = True):
        self.file_path = file_path
        self.delimiter = delimiter
        self.has_header = has_header

    def load_data(self) -> pd.DataFrame:
        """Loads CSV data into a pandas DataFrame."""
        logger.info(f"Loading CSV from {self.file_path}")
        header = 0 if self.has_header else None

        try:
            with warnings.catch_warnings(record=True) as w_list:
                warnings.simplefilter('always')
                df = pd.read_csv(self.file_path, sep=self.delimiter, header=header)

                for warning in w_list:
                    logger.warning(f"Warning while loading CSV file: {warning.message}")

                logger.info(f"Successfully loaded CSV with {df.shape[0]} rows and {df.shape[1]} columns.")
                return df
        except Exception as e:
            logger.error(f"Error loading CSV file: {e}")
            raise RuntimeError(f"Failed to load CSV file {self.file_path}: {e}")