from common.data_ingestion.data_factory import DataFactory


class DataProcessor:
    """
    Main class for reading & processing the data.
    """

    def __init__(self, report_type, skiprow=0, skipfooter=0, asset_class=None, dtype=None, regime=None, logger=None):
        """
        Initializes a DataProcessor with the specified parameters.
        """
        self.report_type = report_type
        self.asset_class = asset_class
        self.skiprow = skiprow
        self.skipfooter = skipfooter
        self.dtype = dtype
        self.regime = regime
        self.logger = logger
        self.data_reader = DataFactory.get_data_reader(self.skiprow, self.skipfooter, self.report_type,
                                                       self.asset_class, self.dtype,
                                                       regime=self.regime, logger=self.logger)

    def process_data(self, file_paths):
        """
        Process the data from the provided file paths using the data reader.
        """
        return self.data_reader.get_report(file_paths)
