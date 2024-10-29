"""
This module generates matching keys in DerivOne files based on specified business logic.

The matching keys are generated using combinations of various identifiers such as USI, UTI,
and Harmonized UTI, along with their prefixes and Party1 LEI where applicable.

The code is optimized for performance and memory efficiency on large datasets.
"""

import pandas as pd
from common.config import upstream_attribute_mappings
from common import constants
from common.config.logger_config import get_logger


class DerivOneKeyGenerator:
    """
    A class to generate matching keys in DerivOne data based on business logic.
    """

    def __init__(self, data, asset_class, environment, report_date, use_case):
        """
        Initialize the DerivOneKeyGenerator with a DataFrame.
        """
        self.logger = get_logger(name=__name__, env=environment, date=report_date, use_case_name=use_case)
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input data must be a pandas DataFrame.")

        self.asset_class = asset_class

        # Get the column names for HUTI prefix and value based on asset class
        self.huti_prefix_col = upstream_attribute_mappings.HARMONIZED_UTI_PREFIX.get(self.asset_class)
        self.huti_value_col = upstream_attribute_mappings.HARMONIZED_UTI_VALUE.get(self.asset_class)

        # Define required columns
        self.required_columns = ['USI Prefix', 'USI Value', 'UTI Prefix', 'UTI Value',
                                 self.huti_prefix_col, self.huti_value_col]

        if self.asset_class not in [constants.EQUITY_DERIVATIVES, constants.EQUITY_SWAPS]:
            # For other asset classes, 'Party1 LEI' is required
            self.party1_lei_col = upstream_attribute_mappings.PARTY1_LEI.get(self.asset_class)
            self.required_columns.append(self.party1_lei_col)
        else:
            self.party1_lei_col = None

        # Select only the required columns to reduce memory usage
        # self.data = data[self.required_columns].copy()

        self.data = data

        self.validate_columns()
        self.clean_columns()

    def validate_columns(self):
        """
        Validate that the DataFrame contains all required columns.
        """
        self.logger.debug('Checking if required columns are present in DerivOne report')
        missing_columns = [col for col in self.required_columns if col not in self.data.columns]
        if missing_columns:
            raise KeyError(f"Missing required column(s): {', '.join(missing_columns)}")

    def clean_columns(self):
        """
        Clean the values in the required columns by removing NaNs, stripping spaces,
        and converting everything to uppercase.
        """
        try:
            # cleaning columns
            self.logger.debug('Cleaning the values in the required columns.')
            self.data[self.required_columns] = (
                self.data[self.required_columns]
                .fillna('')
                .astype(str)
                .apply(lambda x: x.str.strip().str.upper())
            )
        except Exception as e:
            print(f"Error cleaning columns: {e}")
            raise

    def generate_keys(self):
        """
        Generate matching keys based on business logic.
        """
        try:
            # Define the regex pattern to remove non-alphanumeric characters
            pattern = r'[^A-Z0-9]'

            if self.asset_class in [constants.EQUITY_DERIVATIVES, constants.EQUITY_SWAPS]:
                # For Equity Derivatives and Swaps

                # Generate matching keys by concatenating relevant columns
                self.logger.debug(f'Creating matching_key_usi for {self.asset_class}')
                self.data['matching_key_usi'] = self.data['USI Prefix'].str.cat(self.data['USI Value'], na_rep='')

                self.logger.debug(f'Creating matching_key_uti for {self.asset_class}')
                self.data['matching_key_uti'] = self.data['UTI Prefix'].str.cat(self.data['UTI Value'], na_rep='')

                self.logger.debug(f'Creating matching_key_huti for {self.asset_class}')
                self.data['matching_key_huti'] = self.data[self.huti_prefix_col].str.cat(self.data[self.huti_value_col], na_rep='')

                self.logger.debug(f'Creating matching_key_usi_value for {self.asset_class}')
                self.data['matching_key_usi_value'] = self.data['USI Value']

                self.logger.debug(f'Creating matching_key_uti_value for {self.asset_class}')
                self.data['matching_key_uti_value'] = self.data['UTI Value']

            else:
                # For other asset classes, include Party1 LEI in the keys

                # Concatenate Party1 LEI with prefixes and values
                party1_lei = self.data[self.party1_lei_col]

                self.logger.debug(f'Creating matching_key_usi for {self.asset_class}')
                self.data['matching_key_usi'] = party1_lei.str.cat(self.data['USI Prefix'], na_rep='').str.cat(self.data['USI Value'], na_rep='')

                self.logger.debug(f'Creating matching_key_uti for {self.asset_class}')
                self.data['matching_key_uti'] = party1_lei.str.cat(self.data['UTI Prefix'], na_rep='').str.cat(self.data['UTI Value'], na_rep='')

                self.logger.debug(f'Creating matching_key_huti for {self.asset_class}')
                self.data['matching_key_huti'] = party1_lei.str.cat(self.data[self.huti_prefix_col], na_rep='').str.cat(self.data[self.huti_value_col], na_rep='')

                self.logger.debug(f'Creating matching_key_usi_value for {self.asset_class}')
                self.data['matching_key_usi_value'] = party1_lei.str.cat(self.data['USI Value'], na_rep='')

                self.logger.debug(f'Creating matching_key_uti_value for {self.asset_class}')
                self.data['matching_key_uti_value'] = party1_lei.str.cat(self.data['UTI Value'], na_rep='')

            # List of columns to clean
            columns_to_clean = ['matching_key_usi', 'matching_key_uti', 'matching_key_huti',
                                'matching_key_usi_value', 'matching_key_uti_value']

            # Remove non-alphanumeric characters and convert to uppercase
            self.logger.debug('Removing non-alphanumeric characters and converting to uppercase')
            self.data[columns_to_clean] = (
                self.data[columns_to_clean]
                .apply(lambda x: x.str.replace(pattern, '', regex=True).str.upper())
            )

        except Exception as e:
            print(f"Error generating keys: {e}")
            raise

        self.logger.debug('Key creation is complete.')
        return self.data
