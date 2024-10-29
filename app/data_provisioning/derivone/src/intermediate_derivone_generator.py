import pandas as pd
import argparse
import sys
import os

from common.data_ingestion.data_processor import DataProcessor
from common.config.logger_config import get_logger
from common.scripts.derivone_deduplicator import DerivOneDeduplicator


class IntermediateDerivOneGenerator:
    """
    Class to generate intermediate DerivOne data by reading and processing DerivOne files.
    """

    def __init__(self, asset_class, environment, report_date, use_case_name='default', skiprow=0, skipfooter=0, dtype=None):
        """
        Initialize the IntermediateDerivOneGenerator.
        """

        self.asset_class = asset_class
        self.environment = environment
        self.report_date = report_date
        self.use_case_name = use_case_name
        self.skiprow = skiprow
        self.skipfooter = skipfooter
        self.dtype = dtype

        # Initialize logger
        self.logger = get_logger(name=__name__, env=self.environment, date=self.report_date, use_case_name=self.use_case_name)

        # Initialize DataProcessor for DerivOne
        self.data_processor = DataProcessor(report_type='derivone', skiprow=self.skiprow, skipfooter=self.skipfooter,
                                            asset_class=self.asset_class, dtype=self.dtype, logger=self.logger)

    def read_derivone_data(self, file_paths, usecols=None, nrows=None, deduplicate=True):
        """
        Read DerivOne data from the specified file paths and optionally deduplicate records.
        """
        try:
            self.logger.info(f"Reading DerivOne data from {file_paths}")

            # Process the data using DataProcessor
            data = self.data_processor.process_data(
                file_paths=file_paths
            )

            self.logger.info(f"Successfully read {len(data)} rows from DerivOne file(s)")

            # Deduplicate if requested
            if deduplicate:
                self.logger.info("Starting deduplication process")
                deduplicator = DerivOneDeduplicator(
                    data=data,
                    asset_class=self.asset_class,
                    environment=self.environment,
                    report_date=self.report_date,
                    use_case=self.use_case_name
                )
                data = deduplicator.run()
                self.logger.info(f"After deduplication: {len(data)} rows remaining")

            return data

        except Exception as e:
            self.logger.error(f"Error reading DerivOne data: {str(e)}", exc_info=True)
            raise

    def validate_data(self, data):
        """
        Validate the read data for basic quality checks.
        Can be extended based on specific validation requirements.
        """
        if data.empty:
            self.logger.warning("No data was read from the DerivOne file(s)")
            return

        # Log basic data statistics
        self.logger.info(f"Data shape: {data.shape}")
        self.logger.info(f"Columns: {', '.join(data.columns)}")

        # Check for missing values
        missing_counts = data.isnull().sum()
        if missing_counts.any():
            self.logger.warning("Missing values found in the following columns:")
            for col, count in missing_counts[missing_counts > 0].items():
                self.logger.warning(f"{col}: {count} missing values")

    def generate_intermediate_data(self, file_paths, usecols=None, nrows=None, deduplicate=True):
        """
        Main method to generate intermediate DerivOne data.
        Combines reading and validation steps.
        """
        try:
            # Read the data
            data = self.read_derivone_data(
                file_paths=file_paths,
                usecols=usecols,
                nrows=nrows,
                deduplicate=deduplicate
            )

            # Validate the data
            self.validate_data(data)

            return data

        except Exception as e:
            self.logger.error(f"Error generating intermediate data: {str(e)}", exc_info=True)
            raise

    def save_data(self, data, output_path):
        """
        Save the processed data to a CSV file.
        """
        try:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Save the data
            data.to_csv(output_path, index=False)
            self.logger.info(f"Data successfully saved to {output_path}")

        except Exception as e:
            self.logger.error(f"Error saving data to {output_path}: {str(e)}", exc_info=True)
            raise


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process DerivOne data files.')
    parser.add_argument('--input-file', required=True, help='Path to the input DerivOne file')
    parser.add_argument('--output-file', required=True, help='Path to save the processed data')
    parser.add_argument('--asset-class', required=True, help='Asset class (e.g., EQ, FX, CR)')
    parser.add_argument('--environment', default='dev', help='Environment (dev/prod)')
    parser.add_argument('--report-date', required=True, help='Report date (YYYY-MM-DD)')
    parser.add_argument('--no-dedup', action='store_true', help='Skip deduplication')
    return parser.parse_args()


def main():
    """Main function to run the DerivOne data processing."""
    # Parse command line arguments
    args = parse_arguments()

    try:
        # Initialize the generator
        generator = IntermediateDerivOneGenerator(asset_class=args.asset_class, environment=args.environment,
                                                  report_date=args.report_date)

        # Process the data
        processed_data = generator.generate_intermediate_data(file_paths=args.input_file, deduplicate=not args.no_dedup)

        # Save the processed data
        generator.save_data(processed_data, args.output_file)

        return 0

    except Exception as e:
        print(f"Error processing DerivOne data: {str(e)}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())