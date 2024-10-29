"""
This module provides the FilePathConfig class, which constructs file paths for TSR and DerivOne files
based on the provided run date. It supports different regimes and asset classes and adjusts file paths
according to the operating system.
"""

import os
import glob
from datetime import datetime
from common import constants
from common.utility import adjust_path_for_os


class FilePathConfig:
    """
    A class to configure and retrieve file paths for TSR and DerivOne files.
    """

    # Regimes and their respective configurations
    REGIMES_CONFIG = {
        constants.EMIR_REFIT: {
            'subfolders': ['ESMA', 'FCA'],
            'tsr_file_pattern': 'sFTP_{prefix}_EOD_Trade_State_Report_*_{report_date}.*_{msa_tms_code}*.csv',
            'prefixes': {
                'ESMA': 'EUEMIR',
                'FCA': 'UKEMIR',
            },
            'date_format': None,
        },

        constants.JFSA: {
            'subfolders': None,
            'tsr_file_pattern': 'sFTP_JFSA_EOD_Trade_State_Report_*-{report_date}.*_{msa_tms_code}*.csv',
            'collateral_file_pattern': 'sFTP_JFSA_EOD_Margin_State_Report_*-{report_date}.*.csv',
            'date_format': None,
        },

        constants.ASIC: {
            'subfolders': None,
            'tsr_file_pattern': 'sFTP_ASIC_EOD_Trade_State_Report_*-{report_date}.*_{msa_tms_code}*.csv',
            'collateral_file_pattern': 'sFTP_ASIC_EOD_Margin_State_Report_*-{report_date}.*.csv',
            'date_format': None,
        },

        constants.MAS: {
            'subfolders': None,
            'tsr_file_pattern': 'sFTP_MAS_EOD_Trade_State_Report_*-{report_date}.*_{msa_tms_code}*.csv',
            'collateral_file_pattern': 'sFTP_MAS_EOD_Margin_State_Report_*-{report_date}.*.csv',
            'date_format': None,
        },
    }

    def __init__(self, run_date, env, logger_obj=None):
        """
        Initializes the FilePathConfig with the provided run date.

        Parameters:
        run_date (str): The run date in 'YYYY-MM-DD' format.
        """
        self.run_date = run_date
        if logger_obj:
            self.logger = logger_obj
        self.env = env.lower()

        # Base directory where all regimes TSRs are located
        self.tsr_base_directory = f'/v/region/eu/appl/gtr/traq/data/{self.env}/input/tsr'
        self.collateral_base_directory = f'/v/region/eu/appl/gtr/traq/data/{self.env}/input/collateral'

        # Adjusts path based on the operating system
        self.tsr_base_directory = adjust_path_for_os(self.tsr_base_directory)
        self.collateral_base_directory = adjust_path_for_os(self.collateral_base_directory)

    @staticmethod
    def report_date_to_filename(report_date, date_format):
        """
        Converts report date from 'YYYY-MM-DD' to the specified date format.

        Parameters:
        report_date (str): The report date in 'YYYY-MM-DD' format.
        date_format (str): The date format to convert to.

        Returns:
        str: The report date in the specified format.
        """
        if not date_format:
            return report_date
        dt = datetime.strptime(report_date, '%Y-%m-%d')
        return dt.strftime(date_format)

    def construct_file_pattern(self, template, report_date, date_format, asset_class, msa_tms_code, prefix=''):
        """
        Constructs the file pattern by formatting the template with the provided variables.

        Parameters:
        template (str): The file name template.
        report_date (str): The report date in 'YYYY-MM-DD' format.
        date_format (str): The date format to use in the filename.
        asset_class (str): The asset class code.
        msa_tms_code (str): The MSA code for the asset class.
        prefix (str): The prefix to use in the filename (if applicable).

        Returns:
        str: The constructed file pattern.
        """
        date_part = self.report_date_to_filename(report_date, date_format)
        file_pattern = template.format(
            prefix=prefix,
            report_date=report_date,
            msa_tms_code=msa_tms_code,
            asset_class=asset_class,
            asset_class_lower=asset_class.lower(),
            date_part=date_part
        )
        return file_pattern

    def get_tsr_files_for_regime(self, regime, asset_classes, report_date=None):
        """
        Finds TSR files for the specified regime and asset classes for the given report date.

        Parameters:
        regime (str): The regime to process (e.g., 'JFSA', 'EMIR_REFIT', etc.)
        asset_classes (str or list): Asset class or list of asset classes specific to the regime.
        report_date (str): The report date in 'YYYY-MM-DD' format. Defaults to self.run_date.

        Returns:
        dict: Dictionary mapping asset classes to lists of matching file paths.
        """
        if report_date is None:
            report_date = self.run_date

        # Convert asset_classes to a list if it's a string
        if isinstance(asset_classes, str):
            asset_classes = [asset_classes]

        # Handle 'EQD' and 'EQS' as 'EQ' when fetching the file paths
        original_asset_classes = asset_classes.copy()  # Keep a copy to use in the final return
        asset_classes = ['EQ' if asset_class in ['EQD', 'EQS'] else asset_class for asset_class in asset_classes]

        # Get the regime configuration
        regime_info = self.REGIMES_CONFIG.get(regime)
        if not regime_info:
            # print(f"Regime '{regime}' not recognized.")
            self.logger.exception(f"Regime '{regime}' not recognized.")
            return {}

        files_found = {}

        try:
            # Process subfolders or top-level directory for asset classes
            if regime_info.get('subfolders'):
                # Handle subfolders (like EMIR_REFIT's ESMA and FCA)
                self._process_subfolders(regime_info, regime, asset_classes, report_date, files_found)
            else:
                # Handle top-level asset classes
                self._process_asset_classes(regime_info, regime, asset_classes, report_date, files_found)

            # Add EQD and EQS to the final return dictionary, with unique lists instead of referencing EQ
            if 'EQ' in files_found:
                for asset_class in original_asset_classes:
                    if asset_class in ['EQD', 'EQS']:
                        # Create a copy of the list instead of assigning directly
                        files_found[asset_class] = list(files_found['EQ'])

        except Exception as e:
            # print(f"Error occurred while processing TSR files for regime {regime}: {str(e)}")
            self.logger.exception(f"Error occurred while processing TSR files for regime {regime}: {str(e)}")

        return files_found

    def _process_subfolders(self, regime_info, regime, asset_classes, report_date, files_found):
        """
        Process asset classes for regimes with subfolders.
        """
        subfolders = regime_info.get('subfolders')
        for subfolder in subfolders:
            prefix = regime_info.get('prefixes', {}).get(subfolder, '')

            for asset_class in asset_classes:
                if asset_class.upper() == 'COL':
                    self._fetch_collateral_files(regime_info, regime, subfolder, asset_class, report_date, files_found)
                else:
                    self._fetch_tsr_files(regime_info, regime, subfolder, asset_class, report_date, prefix, files_found)

    def _process_asset_classes(self, regime_info, regime, asset_classes, report_date, files_found):
        """
        Process asset classes for regimes without subfolders.
        """
        for asset_class in asset_classes:
            if asset_class.upper() == constants.COLLATERAL:
                self._fetch_collateral_files(regime_info, regime, None, asset_class, report_date, files_found)
            else:
                self._fetch_tsr_files(regime_info, regime, None, asset_class, report_date, '', files_found)

    def _fetch_tsr_files(self, regime_info, regime, subfolder, asset_class, report_date, prefix, files_found):
        """
        Fetch TSR files for a given asset class and subfolder.
        """
        msa_tms_code = None
        if asset_class not in [constants.COLLATERAL]:
            msa_tms_code = constants.ASSET_CLASS_MSA_TMS_CODES.get(asset_class)
            if msa_tms_code is None:
                # print(f"Asset class '{asset_class}' not found in MSA configuration.")
                self.logger.exception(f"Asset class '{asset_class}' not found in MSA configuration.")
                return

        # Construct the directory path
        dir_path = os.path.join(self.tsr_base_directory, regime, subfolder or '', asset_class)
        dir_path = adjust_path_for_os(dir_path)

        # Check if the directory exists
        if not os.path.exists(dir_path):
            # print(f"Directory does not exist: {dir_path}")
            self.logger.exception(f"Directory does not exist: {dir_path}")
            return

        # Construct the file pattern
        file_pattern = self.construct_file_pattern(
            regime_info['tsr_file_pattern'],
            report_date,
            regime_info.get('date_format'),
            asset_class,
            msa_tms_code,
            prefix
        )

        # Construct the full glob pattern
        full_glob_pattern = os.path.join(dir_path, file_pattern)

        # Find matching files
        matching_files = glob.glob(full_glob_pattern)

        # Save the matching files
        if asset_class not in files_found:
            files_found[asset_class] = []
        files_found[asset_class].extend(matching_files)

    def _fetch_collateral_files(self, regime_info, regime, subfolder, asset_class, report_date, files_found):
        """
        Fetch collateral files for a given regime.
        """
        dir_path = os.path.join(self.collateral_base_directory, regime)
        dir_path = adjust_path_for_os(dir_path)

        # Check if the directory exists
        if not os.path.exists(dir_path):
            # print(f"Directory does not exist: {dir_path}")
            self.logger.exception(f"Directory does not exist: {dir_path}")
            return

        # Construct the file pattern for collateral files
        collateral_file_pattern = regime_info.get('collateral_file_pattern')
        if not collateral_file_pattern:
            # print(f"No collateral file pattern found for regime '{regime}'.")
            self.logger.exception(f"No collateral file pattern found for regime '{regime}'.")
            return

        file_pattern = collateral_file_pattern.format(report_date=report_date)
        full_glob_pattern = os.path.join(dir_path, file_pattern)

        # Find matching files
        matching_files = glob.glob(full_glob_pattern)

        # Save the matching files
        if asset_class not in files_found:
            files_found[asset_class] = []
        files_found[asset_class].extend(matching_files)

    def get_derivone_filepaths(self, report_date):
        """
        Constructs file paths for DerivOne files based on the provided report date.

        Parameters:
        report_date (str): The report date in 'YYYY-MM-DD' format.

        Returns:
        dict: Dictionary mapping asset classes to lists of file paths.
        """
        report_date_yy_mm_dd = str(report_date)
        report_date_yymmdd = str(report_date).replace('-', '')

        try:
            derivone_filepaths = {
                constants.COMMODITY: [adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/Deriv1/CO/imrecon_com_eod_prod_{report_date_yymmdd}.csv")],

                constants.CREDIT: [adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/Deriv1/CR/imrecon_crd_ny_eod_CR_prod_{report_date_yymmdd}.csv"),
                                   adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/Deriv1/CR/imrecon_crd_ln_eod_CR_prod_{report_date_yymmdd}.csv"),
                                   adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/Deriv1/CR/imrecon_crd_ap_eod_CR_prod_{report_date_yymmdd}.csv")],

                constants.EQUITY_DERIVATIVES: [adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/GINGER/EQD/dfa_eq_ds_prod_{report_date_yy_mm_dd}_*.csv"),
                                               adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/GINGER/EQD/dfa_eq_ex_prod_{report_date_yy_mm_dd}_*.csv"),
                                               adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/GINGER/EQD/dfa_eq_op_prod_{report_date_yy_mm_dd}_*.csv"),
                                               adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/GINGER/EQD/dfa_eq_vs_prod_{report_date_yy_mm_dd}_*.csv")],

                constants.EQUITY_SWAPS: [adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/FRED/EQS/dfa_eq_es_prod_{report_date_yy_mm_dd}_*_ny.csv"),
                                         adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/FRED/EQS/dfa_eq_es_prod_{report_date_yy_mm_dd}_*_ln.csv"),
                                         adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/FRED/EQS/dfa_eq_es_prod_{report_date_yy_mm_dd}_*_hk.csv")],

                constants.FOREIGN_EXCHANGE: [adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/Deriv1/FX/imrecon_fx_eod_prod_{report_date_yymmdd}.csv")],

                constants.INTEREST_RATES: [adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/Deriv1/IR/imrecon_ird_ny_eod_prod_{report_date_yymmdd}.csv"),
                                           adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/Deriv1/IR/imrecon_ird_ln_eod_prod_{report_date_yymmdd}.csv"),
                                           adjust_path_for_os(rf"/v/region/eu/appl/gtr/traq/data/{self.env}/input/Deriv1/IR/imrecon_ird_ap_eod_prod_{report_date_yymmdd}.csv")]
            }

            # Apply globbing for EQD and EQS file paths
            for key in [constants.EQUITY_DERIVATIVES, constants.EQUITY_SWAPS]:
                file_paths = []
                for path_pattern in derivone_filepaths[key]:
                    matched_files = glob.glob(path_pattern)
                    file_paths.extend(matched_files)
                derivone_filepaths[key] = file_paths

            return derivone_filepaths

        except Exception as e:
            # print(f"Error occurred while getting DerivOne file paths: {e}")
            self.logger.exception(f"Error occurred while getting DerivOne file paths: {e}")
            raise
