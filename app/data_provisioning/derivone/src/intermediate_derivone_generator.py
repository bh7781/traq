import time
import sys
import argparse
import os
import gc

from common.data_ingestion.data_processor import DataProcessor
from common.config.logger_config import get_logger
from common.scripts.derivone_deduplicator import DerivOneDeduplicator
from common.config.filepath_config import FilePathConfig
from common.scripts.derivone_key_generator import DerivOneKeyGenerator


class IntermediateDerivOneGenerator:
    """
    Class to generate intermediate DerivOne data by reading and processing DerivOne files.
    """

    def __init__(self, asset_class, env, report_date):
        """
        Initialize the IntermediateDerivOneGenerator.
        """
        self.asset_class = asset_class
        self.env = env.lower()
        self.report_date = report_date

        # Initialize DataProcessor for DerivOne
        self.data_processor = DataProcessor(
            report_type='derivone',
            skiprow=0,
            skipfooter=0,
            asset_class=self.asset_class,
            dtype=None,
            logger=logger
        )

        # Creating instance of FilePathConfig to fetch TSR & DerivOne file paths
        filepath_config = FilePathConfig(self.report_date, self.env, logger)

        # Read DerivOne Files
        self.derivone_filepaths = filepath_config.get_derivone_filepaths(report_date=report_date)

        if not self.derivone_filepaths.get(asset_class):
            error_msg = f"DerivOne file not found for asset class {asset_class} for report date {report_date}"
            logger.error(error_msg)
            logger.error("Terminating program execution due to missing DerivOne file.")
            sys.exit(1)

        logger.info(f"DerivOne File Paths for {asset_class}: {self.derivone_filepaths.get(asset_class)}")

    def cleanup(self):
        """
        Explicit cleanup method to release resources.
        """
        if hasattr(self, 'data_processor'):
            self.data_processor = None
        if hasattr(self, 'derivone_filepaths'):
            self.derivone_filepaths = None
        gc.collect()

    def __enter__(self):
        """
        Context manager enter method.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit method ensuring cleanup.
        """
        self.cleanup()

    def read_derivone_data(self, file_paths):
        """
        Read DerivOne data from the specified file paths, generate keys, and deduplicate records.
        """
        try:
            logger.info(f"Reading DerivOne data from {file_paths}")

            # Process the data using DataProcessor
            data = self.data_processor.process_data(file_paths=file_paths)
            logger.info(f"Successfully read {len(data)} rows from DerivOne file(s)")

            # Generate matching keys
            logger.info("Starting key generation process")
            key_generator = DerivOneKeyGenerator(
                data=data,
                asset_class=self.asset_class,
                environment=self.env,
                report_date=self.report_date,
                use_case=use_case_name
            )
            data = key_generator.generate_keys()
            del key_generator
            logger.info("Key generation completed")

            # Deduplicate the data
            logger.info("Starting deduplication process")
            deduplicator = DerivOneDeduplicator(
                data=data,
                asset_class=self.asset_class,
                environment=self.env,
                report_date=self.report_date,
                use_case=use_case_name,
                log_to_file=False
            )
            data = deduplicator.run()
            del deduplicator
            logger.info(f"After deduplication: {len(data)} rows remaining")

            return data

        except Exception as e:
            logger.error(f"Error reading DerivOne data: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def validate_data(data):
        """
        Validate the read data for basic quality checks.
        """
        if data.empty:
            logger.warning("No data was read from the DerivOne file(s)")
            return

        logger.info(f"Data shape: {data.shape}")
        logger.info(f"Columns: {', '.join(data.columns)}")

    def generate_intermediate_data(self):
        """
        Main method to generate intermediate DerivOne data.
        """
        try:
            file_paths = self.derivone_filepaths.get(self.asset_class)
            data = self.read_derivone_data(file_paths=file_paths)
            self.validate_data(data)
            return data

        except Exception as e:
            logger.error(f"Error generating intermediate data: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def save_data(data, output_path):
        """
        Save the processed data to a CSV file.
        """
        try:
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            data.to_csv(output_path, index=False)
            logger.info(f"Data successfully saved to {output_path}")

        except Exception as e:
            logger.error(f"Error saving data to {output_path}: {str(e)}", exc_info=True)
            raise


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process DerivOne data files.')
    parser.add_argument('-e', '--env', required=True, type=str, help='Environment parameter', choices=['qa', 'prod'])
    parser.add_argument('-d', '--run_date', required=True, help='Run Date or date of execution')
    parser.add_argument('-a', '--asset_classes', nargs='+', help='List of asset classes to process')
    return parser.parse_args()


def process_asset_class(asset_class, env, run_date):
    """
    Process a single asset class and clean up resources afterward.
    """
    try:
        # Use context manager to ensure cleanup
        with IntermediateDerivOneGenerator(
                asset_class=asset_class,
                env=env,
                report_date=run_date
        ) as generator:
            # Process the data
            processed_data = generator.generate_intermediate_data()

            # Save the processed data
            output_file = rf'C:\Users\{os.getlogin()}\Morgan Stanley\TTRO Independent Testing - APAC New Build\ASIC + MAS + JFSA\Diagnostic Output\intermediate_derivone\{asset_class}_intermediate_derivone_{run_date}.csv'
            generator.save_data(processed_data, output_file)

            # Explicitly clean up processed data
            del processed_data
            gc.collect()

    except Exception as e:
        logger.error(f"Error processing asset class {asset_class}: {str(e)}", exc_info=True)
        raise


def main():
    """Main function to run the DerivOne data processing."""
    logger.info('*********************Execution Started*********************')
    logger.info(f'RUN_DATE = {args.run_date}')
    logger.info(f'ENVIRONMENT = {args.env.upper()}')
    logger.info(f'ASSET_CLASSES = {args.asset_classes}')

    try:
        for asset_class in args.asset_classes:
            process_asset_class(asset_class, args.env, args.run_date)
            # Force garbage collection after each asset class
            gc.collect()

        logger.info('*********************Execution Finished*********************')
        logger.info(f'Total time required = {round((time.time() - start_time) / 60, 2)} minutes')
        return 0

    except Exception as e:
        print(f"Error processing DerivOne data: {str(e)}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    try:
        # Parse command line arguments
        args = parse_arguments()
        start_time = time.time()
        use_case_name = 'intermediate_derivone_generator'

        # Initialize the logger instance
        logger = get_logger(__name__, args.env, args.run_date, use_case_name=use_case_name, log_to_file=False)

        exit_code = main()
    finally:
        # Final cleanup
        gc.collect()
        sys.exit(exit_code)