from abc import ABC, abstractmethod
import pandas as pd

from common import constants
from common.data_ingestion.data_filters import TSRFilters
from common.config.tsr_attribute_mappings import PRODUCT_TAXONOMY


class DataReader(ABC):
    """
    Abstract base class for data readers.
    Provides a method to read CSV data efficiently.
    """

    def __init__(self, skiprow=0, skipfooter=0, report_type=None, asset_class=None,
                 dtype=None, regime=None, logger=None, nrows=None):
        """
        Initializes the DataReader with common parameters.

        Parameters:
            skiprow (int): Number of rows to skip at the start of the file.
            skipfooter (int): Number of rows to skip at the end of the file.
            report_type (str): Type of report (e.g., 'tsr', 'msr', 'derivone', 'gleif').
            asset_class (str): Asset class (e.g., 'EQ', 'FX', 'CR', etc.).
            dtype (dict): Dictionary of column data types.
            regime (str): Regulatory regime.
            logger: Logger instance for logging messages.
            nrows (int, optional): Number of rows to read.
        """
        self.regime = regime
        self.asset_class = asset_class
        self.skiprow = skiprow
        self.skipfooter = skipfooter
        self.report_type = report_type
        self.dtype = dtype
        self.logger = logger
        self.nrows = nrows  # Number of rows to read

    @abstractmethod
    def get_report(self, file_paths, usecols=None, nrows=None):
        """
        Abstract method to get the report data.
        """
        pass

    def read_csv_data(self, file_paths, dtype=None, usecols=None, nrows=None):
        """
        Reads the data from CSV files in chunks for memory efficiency.
        """
        # Use the class attribute 'self.nrows' if 'nrows' is not provided
        if nrows is None:
            nrows = self.nrows

        if isinstance(file_paths, str):
            file_paths = [file_paths]

        if all(isinstance(i, str) for i in file_paths):
            data_frames = []

            # Set default chunksize for reading in chunks
            default_chunksize = 500000

            # Adjust chunksize if nrows is specified and less than default_chunksize
            if nrows is not None and nrows < default_chunksize:
                chunksize = nrows
            else:
                chunksize = default_chunksize

            # Create a translation table to replace non-printable ASCII characters with '_'
            non_printable_chars = ''.join(map(chr, range(0, 32))) + chr(127)
            translation_table = str.maketrans({char: '_' for char in non_printable_chars})

            total_rows_read = 0  # Keep track of the total number of rows read

            for file in file_paths:
                reader = pd.read_csv(
                    file,
                    skiprows=self.skiprow,
                    skipfooter=self.skipfooter,
                    usecols=usecols,
                    low_memory=False,
                    dtype=dtype,
                    encoding='utf-8',
                    on_bad_lines='skip',
                    index_col=False,
                    encoding_errors='strict',
                    chunksize=chunksize,
                    engine='python' if self.skipfooter > 0 else 'c',
                )

                for i, chunk in enumerate(reader):
                    # Replace control characters in column names with '_', only once
                    if i == 0 and not data_frames:
                        chunk.columns = chunk.columns.str.translate(translation_table)
                        columns = chunk.columns  # Store the processed column names
                    else:
                        chunk.columns = columns  # Ensure consistent column names

                    # If nrows is specified, limit the total rows read
                    if nrows is not None:
                        remaining_rows = nrows - total_rows_read
                        if remaining_rows <= 0:
                            break  # Stop if we've read enough rows
                        if len(chunk) > remaining_rows:
                            chunk = chunk.iloc[:remaining_rows]  # Trim the chunk
                    total_rows_read += len(chunk)

                    # Replace exact matches of 'nan' with empty strings
                    chunk.replace(to_replace='nan', value='', inplace=True)
                    data_frames.append(chunk)

                    # Break if we've read enough rows
                    if nrows is not None and total_rows_read >= nrows:
                        break  # Stop reading further chunks

            # Combine all data_frames into a single DataFrame
            df_final = pd.concat(data_frames, ignore_index=True)
            return df_final

        raise ValueError("'file_paths' should be a string or a list of strings")

    # Additional methods to read data from other sources (e.g., Excel, databases) can be added here.


class DerivOneDataReader(DataReader):
    """
    DataReader subclass for reading DerivOne data.
    """

    def __init__(self, skiprow=0, skipfooter=0, report_type=None, asset_class=None,
                 dtype=None, regime=None, logger=None, nrows=None):
        """
        Initializes the DerivOneDataReader with specific parameters.
        """
        super().__init__(skiprow=skiprow, skipfooter=skipfooter, report_type=report_type,
                         asset_class=asset_class, dtype=dtype, regime=regime, logger=logger, nrows=nrows)
        # self.nrows = 1000

    def get_report(self, file_paths, usecols=None, nrows=None):
        """
        Reads DerivOne data from the specified file paths.
        """
        data = self.read_csv_data(file_paths, dtype=self.dtype, usecols=usecols, nrows=nrows)
        return data


class TSRDataReader(DataReader):
    """
    DataReader subclass for reading TSR data.
    """

    def __init__(self, skiprow=0, skipfooter=0, report_type=None, asset_class=None,
                 dtype=None, regime=None, logger=None, nrows=None):
        """
        Initializes the TSRDataReader with specific parameters.
        """
        super().__init__(skiprow=skiprow, skipfooter=skipfooter, report_type=report_type,
                         asset_class=asset_class, dtype=dtype, regime=regime, logger=logger, nrows=nrows)

    def get_report(self, file_paths, usecols=None, nrows=None):
        """
        Reads TSR data from the specified file paths and applies filters if necessary.
        """
        data = self.read_csv_data(file_paths, dtype=self.dtype, usecols=usecols, nrows=nrows)

        # Apply TSRFilters if needed
        if self.asset_class in [
            constants.EQUITY_DERIVATIVES,
            constants.EQUITY_SWAPS,
            constants.FOREIGN_EXCHANGE_CASH,
            constants.FOREIGN_EXCHANGE_OPTIONS,
        ]:
            product_taxonomy = PRODUCT_TAXONOMY.get(self.regime)
            tsr_filter = TSRFilters(
                data=data,
                asset_class=self.asset_class,
                regime=self.regime,
                logger=self.logger,
                product_id_col=product_taxonomy,
            )
            data = tsr_filter.data
        return data


class MSRDataReader(DataReader):
    """
    DataReader subclass for reading MSR data.
    """

    def __init__(self, skiprow=0, skipfooter=0, report_type=None, asset_class=None,
                 dtype=None, regime=None, logger=None, nrows=None):
        """
        Initializes the MSRDataReader with specific parameters.
        """
        super().__init__(skiprow=skiprow, skipfooter=skipfooter, report_type=report_type,
                         asset_class=asset_class, dtype=dtype, regime=regime, logger=logger, nrows=nrows)

    def get_report(self, file_paths, usecols=None, nrows=None):
        """
        Reads MSR data from the specified file paths.
        """
        data = self.read_csv_data(file_paths, dtype=self.dtype, usecols=usecols, nrows=nrows)
        return data


class GLEIFDataReader(DataReader):
    """
    DataReader subclass for reading GLEIF data.
    """

    def __init__(self, skiprow=0, skipfooter=0, report_type=None, asset_class=None,
                 dtype=None, regime=None, logger=None, nrows=None):
        """
        Initializes the GLEIFDataReader with specific parameters.
        """
        super().__init__(skiprow=skiprow, skipfooter=skipfooter, report_type=report_type,
                         asset_class=asset_class, dtype=dtype, regime=regime, logger=logger, nrows=nrows)
        # Define the columns to read
        self.usecols = [
            'LEI',
            'Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.1',
            'Entity.LegalName',
        ]
        # self.nrows = 500

    def get_report(self, file_paths, usecols=None, nrows=None):
        """
        Reads GLEIF data from the specified file paths.
        """
        # Use predefined usecols for GLEIF data
        data = self.read_csv_data(file_paths, dtype=self.dtype, usecols=self.usecols, nrows=self.nrows)

        # Drop duplicates based on LEI
        data.drop_duplicates(subset=['LEI'], inplace=True)

        # Create a new column 'Entity Name' with preference for Transliterated name
        data['Entity Name'] = data[
            'Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.1'
        ]

        # Replace NaN or empty strings in 'Entity Name' with values from 'Entity.LegalName'
        data['Entity Name'] = data['Entity Name'].mask(
            data['Entity Name'].eq('')
        ).fillna(data['Entity.LegalName'])

        return data
