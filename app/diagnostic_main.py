import time
import argparse
import traceback
import os
import json

import pandas as pd

from common import constants
from common.config.args_config import Config
from common.config.logger_config import get_logger
from common.data_ingestion.data_processor import DataProcessor
from common import utility

from common.config.ref_data_filepaths import get_ref_data_location
from common.key_generation.tsr_key_generator import (JFSATSRKeyGenerator, ASICTSRKeyGenerator, MASTSRKeyGenerator)
from common.config.tsr_attribute_mappings import TSR_COLUMNS_WITH_LEI
from common.config.tsr_attribute_mappings import MSR_COLUMNS_WITH_LEI
from common.key_generation.derivone_key_generator import DerivOneKeyGenerator
from common.scripts.derivone_deduplicator import DerivOneDeduplicator
from common.data_ingestion.data_merger import DataMerger
from diagnostic_pandq.output_filepath import get_column_json_location
from diagnostic_pandq.pandq_models.model_config import get_model_configs
from diagnostic_pandq.data_processing.pandq_data_processor import PANDQDataProcessor
from diagnostic_pandq.output_filepath import get_output_location
from diagnostic_pandq.pandq_models.model_generator_api import PANDQModelsGenerator
from common.config.filepath_config import FilePathConfig
from common.config.derivone_dtype_dict import derivone_dtype


def rename_columns_from_json(df, json_file_path):
    """
    Rename specific DataFrame columns based on key-value pairs in a JSON file.
    """
    # Load the JSON file containing only the columns that need to be renamed
    with open(json_file_path, 'r') as f:
        column_mappings = json.load(f)

    # Rename DataFrame columns based on JSON key-value pairs (if the columns exist in the DataFrame)
    df.rename(columns=column_mappings, inplace=True)

    return df


def save_columns_to_json(df, regime_name, asset_class):
    """
    Save column names from the DataFrame as a JSON file.
    """
    # Get the appropriate file path for saving the JSON
    column_json_location = get_column_json_location(Config().env.lower())
    json_file = column_json_location.get(regime_name.upper()).get(asset_class)

    # Save the JSON file
    with open(json_file, 'w') as f:
        json.dump(df.columns.tolist(), f, indent=4)  # Save as a JSON array with indentation for readability

    logger.info(f"Saved columns for {regime_name}-{asset_class} at {json_file}")


def load_columns_from_json(regime_name, asset_class):
    """
    Load column names from a previously saved JSON file.
    """
    # Get the appropriate file path for loading the JSON
    column_json_location = get_column_json_location(Config().env.lower())
    json_file = column_json_location.get(regime_name).get(asset_class)

    # Check if the JSON file exists
    if not os.path.exists(json_file):
        logger.error(f"JSON file not found: {json_file}. Cannot validate columns for {asset_class}.")
        raise FileNotFoundError(f"JSON file not found: {json_file}")

    # Load the JSON file
    with open(json_file, 'r') as f:
        saved_columns = json.load(f)  # Load the JSON array

    logger.info(f"Loaded columns from JSON for {regime_name}-{asset_class}.")
    return saved_columns


def log_matching_status_summary(report_date, asset_class, df, summary_dict):
    """
    Records the value counts of the 'matching_status' column for each asset class and stores them in a summary dictionary.
    """
    # Extract the counts of each unique value in the 'matching_status' column
    status_counts = df['matching_flag'].value_counts().to_dict()

    # Store the counts in the summary dictionary
    summary_dict[asset_class] = {
        'report_date': report_date,
        'status_counts': status_counts
    }


def print_matching_status_summary(summary_dict):
    """
    Logs the summary of 'matching_status' counts for each asset class after the loop is complete.

    Parameters:
    - summary_dict: The dictionary containing the summary of counts for each asset class.
    - logger: The logger object to log the summary.
    """
    # Find all unique matching statuses across all asset classes
    all_statuses = set()
    for asset_class, data in summary_dict.items():
        all_statuses.update(data['status_counts'].keys())
    all_statuses = sorted(all_statuses)  # Sort the statuses for consistent output

    # Log the header
    header = ['Report Date', 'Asset Class'] + list(all_statuses)
    logger.info(f"{' | '.join(header)}")

    # Log each row of the table
    for asset_class, data in summary_dict.items():
        row = [str(data['report_date']), asset_class] + [str(data['status_counts'].get(status, 0)) for status in all_statuses]
        logger.info(f"{' | '.join(row)}")


def apply_pandq_processing(df_merged, asset_class):
    """
    Applies PANDQ-specific data processing to the merged dataset.
    """

    logger.info('Applying PANDQ specific data processing on merged data.')
    regime = Config().regime.upper()

    output_filepath = OUTPUT_LOCATION.get(regime).get(asset_class)

    if not output_filepath:
        logger.error(f"No output file path found for regime: {regime}, asset class: {asset_class}")
        raise ValueError(f"No output file path found for regime: {regime}, asset class: {asset_class}")

    data_processor = PANDQDataProcessor(output_filepath=output_filepath, data=df_merged)

    data_processor.clean_data()  # Clean the merged data

    return data_processor


def merge_datasets(df_tsr, df_derivone, asset_class):
    """
    Merges TSR and DerivOne data for a given asset class.
    """
    # Merge TSR and DerivOne data
    data_merger = DataMerger(
        df_left=df_tsr, df_right=df_derivone,
        regulator=Config().regime.upper(), asset_class=asset_class,
        left_prefix='TSR_', right_prefix='Deriv1_', use_case_name=use_case_name
    )
    df_merged = data_merger.merge_data(return_type='left')
    logger.info(f'{Config().regime.upper()}-{asset_class} merged data shape: {df_merged.shape}')
    return df_merged


def read_datasets(report_type, filepath_list, skiprow=0, skipfooter=0, asset_class=None, dtype=None, regime=None):
    """
    Reads and processes datasets based on the provided report type and file paths.
    """
    data_processor = DataProcessor(report_type, skiprow, skipfooter, asset_class, dtype=dtype,
                                   regime=regime, logger=logger)
    return data_processor.process_data(file_paths=filepath_list)


def process_derivone(report_date, asset_class, filepath_config):
    """
    Processes DerivOne data for a given asset class.
    """
    # Read DerivOne Files
    derivone_filepaths = filepath_config.get_derivone_filepaths(report_date=report_date)
    logger.info('Started reading DerivOne Report')

    # Check if the asset class has a specific dtype configuration in the derivone_dtype dictionary
    if asset_class in derivone_dtype:
        dtype_config = derivone_dtype[asset_class]
        logger.info(f'Using specific dtype configuration for {asset_class}')
    else:
        dtype_config = str  # Default dtype if no specific configuration is found
        logger.info(f'Using default dtype configuration (str) for {asset_class}')

    df_derivone = read_datasets(
        report_type='derivone',
        asset_class=asset_class,
        filepath_list=derivone_filepaths.get(asset_class),
        dtype=dtype_config
    )
    logger.info('Finished reading DerivOne Report')
    logger.info(f'DerivOne Shape before deleting duplicates: {df_derivone.shape}')

    # Deduplicate DerivOne data
    d1_deduplicator = DerivOneDeduplicator(data=df_derivone, asset_class=asset_class, environment=Config().env.lower(),
                                           report_date=Config().run_date, use_case=use_case_name)
    df_derivone = d1_deduplicator.run()
    logger.info(f'DerivOne Shape after deleting duplicates: {df_derivone.shape}')

    logger.info('Started creating matching keys in DerivOne Report')
    deriv1_key_generator = DerivOneKeyGenerator(data=df_derivone, asset_class=asset_class,
                                                environment=Config().env.lower(), report_date=Config().run_date,
                                                use_case=use_case_name)
    df_derivone = deriv1_key_generator.generate_keys()
    logger.info('Finished creating matching keys in DerivOne Report')

    return df_derivone


def process_tsr(tsr_filepaths, asset_class, df_gleif):
    """
    Processes TSR data for a given asset class.
    """
    # Read TSR Files
    df_tsr = read_datasets(
        report_type='tsr',
        filepath_list=tsr_filepaths.get(asset_class),
        skiprow=constants.TSR_SKIPROWS.get(Config().regime.upper()),
        skipfooter=constants.TSR_SKIPFOOTERS.get(Config().regime.upper()),
        asset_class=asset_class,
        dtype=str,
        regime=Config().regime.upper()
    )

    tsr_key_generator = None

    # Generate keys if regime is JFSA
    if Config().regime.upper() == constants.JFSA:
        tsr_key_generator = JFSATSRKeyGenerator(data=df_tsr, asset_class=asset_class,
                                                environment=Config().env.lower(), report_date=Config().run_date,
                                                use_case=use_case_name)

    # Generate keys if regime is ASIC
    elif Config().regime.upper() == constants.ASIC:
        if Config().env.lower() not in ['prod']:
            logger.info(f'{Config().regime.upper()}-{asset_class} TSR Shape: {df_tsr.shape}')
            df_tsr = df_tsr[df_tsr["Collateral portfolio code (variation margin)"].str.contains("PPF", na=False)]

        tsr_key_generator = ASICTSRKeyGenerator(data=df_tsr, asset_class=asset_class,
                                                environment=Config().env.lower(), report_date=Config().run_date,
                                                use_case=use_case_name)

    # Generate keys if regime is ASIC
    elif Config().regime.upper() == constants.MAS:
        if Config().env.lower() not in ['prod']:
            logger.info(f'{Config().regime.upper()}-{asset_class} TSR Shape: {df_tsr.shape}')
            df_tsr = df_tsr[df_tsr["Variation margin collateral portfolio code"].str.contains("PPF", na=False)]

        tsr_key_generator = MASTSRKeyGenerator(data=df_tsr, asset_class=asset_class,
                                               environment=Config().env.lower(), report_date=Config().run_date,
                                               use_case=use_case_name)

    if tsr_key_generator:
        tsr_key_generator.validate_columns()  # Validate required columns for key generation
        tsr_key_generator.clean_columns(tsr_key_generator.required_columns)  # Clean and preprocess columns
        df_tsr = tsr_key_generator.generate_keys()  # Generate matching keys

    # Map LEI values to Entity Names and add corresponding columns to the dataframe.
    lei_columns = TSR_COLUMNS_WITH_LEI.get(Config().regime.upper())
    df_tsr = utility.add_entity_names(input_df=df_tsr, gleif_dict=df_gleif, lei_columns=lei_columns)

    logger.info(f'{Config().regime.upper()}-{asset_class} TSR Shape: {df_tsr.shape}')

    return df_tsr


def process_msr(tsr_filepaths, asset_class, df_gleif):
    """
    Processes TSR data for a given asset class.
    """
    # Read TSR Files
    df_msr = read_datasets(
        report_type='msr',
        filepath_list=tsr_filepaths.get(asset_class),
        skiprow=constants.MSR_SKIPROWS.get(Config().regime.upper()),
        skipfooter=constants.MSR_SKIPFOOTERS.get(Config().regime.upper()),
        asset_class=asset_class,
        dtype=str,
        regime=Config().regime.upper()
    )

    if Config().regime.upper() == constants.ASIC:
        if Config().env.lower() not in ['prod']:
            logger.info(f'{Config().regime.upper()}-{asset_class} TSR Shape: {df_msr.shape}')
            df_msr = df_msr[df_msr["Collateral portfolio code (variation margin)"].str.contains("PPF", na=False)]

    # Generate keys if regime is ASIC
    elif Config().regime.upper() == constants.MAS:
        if Config().env.lower() not in ['prod']:
            logger.info(f'{Config().regime.upper()}-{asset_class} TSR Shape: {df_msr.shape}')
            df_msr = df_msr[df_msr["Variation margin collateral portfolio code"].str.contains("PPF", na=False)]

    # Map LEI values to Entity Names and add corresponding columns to the dataframe.
    lei_columns = MSR_COLUMNS_WITH_LEI.get(Config().regime.upper())
    df_msr = utility.add_entity_names(input_df=df_msr, gleif_dict=df_gleif, lei_columns=lei_columns)

    logger.info(f'{Config().regime.upper()}-{asset_class} MSR Shape: {df_msr.shape}')
    return df_msr


def process_asset_class(asset_class, tsr_filepaths, gleif_dict, filepath_config, update_columns_flag):
    """
    Processes a single asset class by reading, cleaning, and merging datasets,
    and applying PANDQ-specific data processing.
    """
    try:
        regime_name = Config().regime.upper()
        logger.info(f'Started execution for {regime_name}-{asset_class}')

        # Extract the report date from TSR
        report_date = utility.get_report_date(
            file_path=tsr_filepaths.get(asset_class)[0],
            report_date_line=constants.REPORT_DATE_LINE.get(regime_name)
        )
        logger.info(f'>>> {regime_name}-{asset_class} Report Date: {report_date} <<<')

        if asset_class == constants.COLLATERAL:
            logger.info(f'Starting Margin State Report (MSR) Specific Processing...')
            df_merged = process_msr(tsr_filepaths, asset_class, gleif_dict)

            logger.info('Adding report_date to underlying data')
            # df_merged.loc[:, 'report_date'] = pd.Series(report_date, index=df_merged.index)
            df_merged = pd.concat([df_merged, pd.DataFrame({'report_date': report_date},
                                                           index=df_merged.index)], axis=1)

            # Clean up unused variables and log memory usage
            utility.log_memory_usage_before_after_gc(logger=logger)
        else:
            # Process TSR and DerivOne data
            logger.info(f'Starting TSR Processing...')
            df_tsr = process_tsr(tsr_filepaths, asset_class, gleif_dict)
            logger.info(f'TSR Processing finished.')

            logger.info(f'Starting DerivOne Processing...')
            df_derivone = process_derivone(report_date, asset_class, filepath_config)

            # Merge TSR and DerivOne datasets
            df_merged = merge_datasets(df_tsr, df_derivone, asset_class)

            logger.info('Adding report_date to underlying data')
            # df_merged.loc[:, 'report_date'] = pd.Series(report_date, index=df_merged.index)
            df_merged = pd.concat([df_merged, pd.DataFrame({'report_date': report_date},
                                                           index=df_merged.index)], axis=1)

            # Clean up unused variables and log memory usage
            del df_tsr, df_derivone
            utility.log_memory_usage_before_after_gc(logger=logger)

            # Handle column validation or saving
            df_merged['matching_flag'] = df_merged['matching_flag'].replace('left_only', 'unmatched')

        # Apply PANDQ-specific processing on the merged data
        logger.info(f'Applying PANDQ Processing...')
        data_processor = apply_pandq_processing(df_merged, asset_class)
        logger.info(f'PANDQ Processing Finished.')

        # Now perform the column renaming on data_processor.data after PANDQ processing
        json_file_path = get_model_configs(env=Config().env).get(Config().regime.upper())['column_mapping']
        data_processor.data = rename_columns_from_json(data_processor.data, json_file_path)

        column_json_location = get_column_json_location(Config().env.lower())
        json_file = column_json_location.get(regime_name.upper()).get(asset_class)

        if not os.path.exists(json_file) or update_columns_flag:
            logger.info(f"Saving column names to JSON file as it's either the first run or --update_columns flag is TRUE")
            save_columns_to_json(data_processor.data, regime_name, asset_class)
        else:
            logger.info(f"Loading column names from JSON file as it's a subsequent run")
            saved_columns = load_columns_from_json(regime_name, asset_class)

            # Find new columns that are not in the saved JSON and log them
            current_columns = set(data_processor.data.columns)
            saved_columns_set = set(saved_columns)
            new_columns = current_columns - saved_columns_set

            if new_columns:
                logger.warning(f"New columns detected that are not in the saved JSON: {new_columns}")

            # Use only the columns that are saved in the JSON
            data_processor.data = data_processor.data[saved_columns]

        # Save the cleaned and updated data
        logger.info(f'Saving the final processed data...')
        data_processor.save_data(separator='|')  # Save the cleaned and updated data
        logger.info(f'Final data saved at {data_processor.output_filepath}')

        return data_processor.data  # Return the final processed data

    except Exception as ex:
        logger.error(f'Error occurred while processing {Config().regime.upper()}-{asset_class}: {ex}')
        logger.error(traceback.format_exc())
        raise


def main():
    """
    Main function that orchestrates the processing of all asset classes for the given regime.
    """
    # Creating instance of FilePathConfig to fetch TSR & DerivOne file paths
    filepath_config = FilePathConfig(Config().run_date, Config().env.lower(), logger)

    # Determine asset classes to process
    asset_classes = args.asset_classes if args.asset_classes else constants.ASSET_CLASS_LIST.get(Config().regime.upper())

    # Add Collateral in asset class list for specific regimes (ASIC, MAS, JFSA)
    if ((not args.asset_classes) and
            (Config().regime.upper() in [constants.JFSA, constants.ASIC, constants.MAS]) and
            (constants.COLLATERAL not in asset_classes)):
        asset_classes.append(constants.COLLATERAL)

    logger.info(f'List of asset classes: {asset_classes}')

    # Get TSR file paths
    tsr_filepaths = filepath_config.get_tsr_files_for_regime(
        regime=Config().regime.upper(),
        asset_classes=asset_classes
    )
    logger.info(f"TSR File Paths: {tsr_filepaths}")

    # Read and prepare GLEIF data
    logger.info('Reading GLEIF file.')
    gleif_filepath = get_ref_data_location(Config().env.lower()).get('GLEIF')
    df_gleif = read_datasets(
        report_type='gleif',
        filepath_list=gleif_filepath,
        skiprow=0,
        skipfooter=0,
        dtype=str
    )
    logger.info(f'GLEIF report shape: {df_gleif.shape}')

    # Convert df_gleif to a dictionary for efficient lookups
    logger.info(f'Converting GLEIF dataframe to a dictionary for efficient lookups')
    gleif_dict = dict(zip(df_gleif['LEI'], df_gleif['Entity Name']))
    logger.info(f'Deleting GLEIF dataframe to free up memory')
    del df_gleif  # Free up memory
    logger.info(f'Deleted GLEIF dataframe')

    # Initialize summary dictionary and lists for successful and failed asset classes
    summary_dict = {}
    successful_asset_classes = []
    failed_asset_classes = []

    # Process each asset class
    for asset_class in asset_classes:
        try:
            df_merged = process_asset_class(asset_class, tsr_filepaths, gleif_dict, filepath_config, args.update_columns)

            # Log matching status summary for this asset class
            if df_merged is not None and not df_merged.empty:
                logger.info(f'Creating Report Date column in final output...')
                report_date = df_merged['report_date'].iloc[0]
                if asset_class not in [constants.COLLATERAL]:
                    logger.info(f'Creating Matching Summary Report...')
                    log_matching_status_summary(report_date, asset_class, df_merged, summary_dict)

                # If the processing succeeds, append to successful asset classes
                successful_asset_classes.append(asset_class)
            else:
                logger.warning(f"No data processed for {asset_class}")
                failed_asset_classes.append(asset_class)  # Append to failed if no data processed

        except Exception as ex:
            logger.error(f'Error occurred while processing {Config().regime.upper()}-{asset_class}: {ex}')
            logger.error(traceback.format_exc())
            # Append to failed asset classes if an exception occurs
            failed_asset_classes.append(asset_class)

        logger.info('----------------------------------------------------')

    del gleif_dict  # Free up memory

    # After the loop, print the matching status summary to the logs
    print_matching_status_summary(summary_dict)

    # Log the successful and failed asset classes
    logger.info(f"Successfully processed asset classes: {', '.join(successful_asset_classes) if successful_asset_classes else 'None'}")
    logger.info(f"Failed asset classes: {', '.join(failed_asset_classes) if failed_asset_classes else 'None'}")

    # Generate regime-specific configuration files required for PANDQ model onboarding
    if args.generate_model_config:
        logger.info(f'Creating PANDQ Model Config files...')
        generator = PANDQModelsGenerator(use_case_name=use_case_name)
        generator.generate_model_files()
        logger.info(f'PANDQ Model Config files created.')


if __name__ == "__main__":
    """
    Entry point for Diagnostic PANDQ Data Processing Use case.
    """
    start_time = time.time()
    use_case_name = 'diagnostic_pandq'

    parser = argparse.ArgumentParser(description="Run the pipeline for a mentioned regulator/regime")
    parser.add_argument('-e', '--env', required=True, type=str, help='Environment parameter', choices=['qa', 'prod'])
    parser.add_argument('-r', '--regime', required=True, help='The regulator to run the pipeline for.',
                        choices=[constants.ASIC.lower(), constants.MAS.lower(), constants.JFSA.lower()])
    parser.add_argument('-d', '--run_date', required=True, help='Run Date or date of execution')
    parser.add_argument('-a', '--asset_classes', nargs='+', help='List of asset classes to process')
    parser.add_argument('-u', '--update_columns', action='store_true', help='Flag to update columns in the saved JSON file')
    parser.add_argument('-g', '--generate_model_config', action='store_true', help='Flag to generate model configuration')

    args = parser.parse_args()

    # Initialize global configuration object
    Config(env=args.env, regime=args.regime, run_date=args.run_date)

    # Initialize OUTPUT_LOCATION based on environment
    OUTPUT_LOCATION = get_output_location(Config().env.lower())

    # Initialize the logger instance
    logger = get_logger(__name__, Config().env, Config().run_date, use_case_name=use_case_name)

    logger.info('*********************Execution Started*********************')
    logger.info(f'Command line arguments: ENV={args.env}, REGIME={args.regime}, RUN_DATE={args.run_date}, '
                f'UPDATE_COLUMNS={args.update_columns}, ASSET_CLASSES={args.asset_classes}')

    try:
        logger.info(f'Starting execution: main()')
        main()
    except Exception as e:
        logger.error(f'Error during execution: {e}')
        logger.error(traceback.format_exc())
        raise

    logger.info('*********************Execution Finished*********************')
    logger.info(f'Total time required = {round((time.time() - start_time) / 60, 2)} minutes')
