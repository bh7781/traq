import pandas as pd
from common import constants
from common.config.upstream_attribute_mappings import (HARMONIZED_UTI_VALUE, PARTY1_LEI)
from common.config.logger_config import get_logger


class DerivOneDeduplicator:
    """
    Class to remove duplicate trades from DerivOne data based on a deduplication key.
    """

    def __init__(self, data, asset_class, environment, report_date, use_case):
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input data must be a pandas DataFrame.")

        self.data = data
        self.asset_class = asset_class
        self.logger = get_logger(name=__name__, env=environment, date=report_date, use_case_name=use_case)
        self.huti_col = HARMONIZED_UTI_VALUE.get(self.asset_class)
        self.party1_lei_col = PARTY1_LEI.get(self.asset_class)

    def create_deduplication_key(self):
        """
        Creates the 'deduplication_key' column.
        Works with string operations first, then converts to categorical at the end.
        """
        # Pre-allocate a Series for deduplication keys with the same index as the data
        dedup_keys = pd.Series(index=self.data.index, dtype='object')

        # Process columns in chunks to reduce peak memory usage
        chunk_size = 100000
        total_rows = len(self.data)

        for start_idx in range(0, total_rows, chunk_size):
            end_idx = min(start_idx + chunk_size, total_rows)
            chunk = self.data.iloc[start_idx:end_idx]
            chunk_idx = chunk.index

            # Convert to strings and handle nulls
            huti_values = chunk[self.huti_col].fillna('').astype(str).str.strip()
            uti_values = chunk['UTI Value'].fillna('').astype(str).str.strip()
            usi_values = chunk['USI Value'].fillna('').astype(str).str.strip()

            # Create dedup key for chunk using string operations
            chunk_key = huti_values.copy()
            chunk_key = chunk_key.where(chunk_key != '', uti_values)
            chunk_key = chunk_key.where(chunk_key != '', usi_values)

            # Handle missing values
            mask = chunk_key == ''
            if mask.any():
                # Create placeholder values only for missing entries
                missing_indices = mask[mask].index
                placeholders = [f'missing_placeholder{i}' for i in
                                range(start_idx + 1, start_idx + len(missing_indices) + 1)]
                chunk_key[missing_indices] = placeholders

            # Handle asset class specific prefixes
            if self.asset_class in [constants.EQUITY_DERIVATIVES, constants.EQUITY_SWAPS]:
                uti_prefixes = chunk['UTI Prefix'].fillna('').astype(str)
                usi_prefixes = chunk['USI Prefix'].fillna('').astype(str)
                prefixes = uti_prefixes.where(huti_values != '', usi_prefixes)
                chunk_key = prefixes + chunk_key
                del prefixes, uti_prefixes, usi_prefixes
            else:
                if self.party1_lei_col in chunk.columns:
                    party1_lei = chunk[self.party1_lei_col].fillna('').astype(str)
                    chunk_key = party1_lei + chunk_key
                    del party1_lei

            # Assign chunk results directly to pre-allocated Series
            dedup_keys[chunk_idx] = chunk_key

            # Clean up temporary variables
            del chunk_key, huti_values, uti_values, usi_values, chunk

        # Create new column all at once and convert to categorical
        # Using a temporary DataFrame to avoid fragmentation
        temp_df = pd.DataFrame({'deduplication_key': dedup_keys}, index=self.data.index)
        self.data = pd.concat([self.data, temp_df], axis=1)

        # Clean up
        del dedup_keys, temp_df

        # Convert to categorical
        self.data['deduplication_key'] = self.data['deduplication_key'].astype('category')

    def remove_duplicates(self):
        """
        Removes duplicate trades with minimal memory overhead.
        """
        if 'deduplication_key' not in self.data.columns:
            self.create_deduplication_key()

        self.logger.debug('Removing duplicates...')

        # Get unique indices efficiently
        unique_indices = (
            self.data.reset_index()
            .groupby('deduplication_key', observed=True)['index']
            .first()
            .values
        )

        # Use boolean indexing instead of drop_duplicates
        self.data = self.data.iloc[unique_indices]
        self.data.reset_index(drop=True, inplace=True)

        # Clean up
        # self.data.drop(columns=['deduplication_key'], inplace=True)

        self.data['deduplication_key'] = self.data['deduplication_key'].astype(str)

        return self.data

    def run(self):
        """
        Executes the deduplication process and returns the deduplicated data.
        """
        return self.remove_duplicates()
