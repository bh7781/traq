"""
Factory class to instantiate and return the appropriate data reader object.
"""

from common.data_ingestion.data_reader import DerivOneDataReader
from common.data_ingestion.data_reader import TSRDataReader
from common.data_ingestion.data_reader import MSRDataReader
from common.data_ingestion.data_reader import GLEIFDataReader


class DataFactory:
    @staticmethod
    def get_data_reader(skiprow, skipfooter, report_type, asset_class=None, dtype=None, regime=None, logger=None):
        """
        Factory method to instantiate and return the appropriate data reader object.
        """
        if report_type.lower() == 'derivone':
            return DerivOneDataReader(skiprow, skipfooter, report_type, asset_class, dtype)
        elif report_type.lower() == 'tsr':
            return TSRDataReader(skiprow, skipfooter, report_type, asset_class, dtype, regime, logger)
        elif report_type.lower() == 'msr':
            return MSRDataReader(skiprow, skipfooter, report_type, asset_class, dtype, regime, logger)
        elif report_type.lower() == 'gleif':
            return GLEIFDataReader(skiprow, skipfooter, report_type, asset_class, dtype)
        else:
            raise ValueError(f"Invalid report type: {report_type}. Must be one of 'DerivOne', 'TSR', 'GLEIF'.")
