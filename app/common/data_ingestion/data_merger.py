import pandas as pd
from common.config.logger_config import get_logger
from common.config.matching_keys_config import get_matching_keys_for_regulator
from common.config.args_config import Config


class DataMerger:
    def __init__(self, df_left, df_right, regulator, asset_class=None, left_prefix='', right_prefix='',
                 use_case_name='default'):
        """
        Initialize DataMerger instance with two dataframes, regulator, and optional asset class.
        Matching keys will be dynamically loaded based on the regulator and asset class.
        """
        self.use_case_name = use_case_name
        self.logger = get_logger(__name__, Config().env.lower(), Config().run_date.lower(),
                                 use_case_name=self.use_case_name)

        # Instead of copying entire dataframes, we'll only add prefixes to column names
        self.df_left = df_left
        self.df_right = df_right
        self.left_prefix = left_prefix
        self.right_prefix = right_prefix

        # Store original column names and dtypes
        self.left_columns = df_left.columns
        self.right_columns = df_right.columns

        # Add prefixes to column names without copying data
        self.df_left.columns = [f"{left_prefix}{col}" for col in df_left.columns]
        self.df_right.columns = [f"{right_prefix}{col}" for col in df_right.columns]

        # Get matching keys with prefixes
        self.on_keys_list = [
            (f"{left_prefix}{key[0]}", f"{right_prefix}{key[1]}")
            for key in get_matching_keys_for_regulator(regulator, asset_class)
        ]

    def _process_matches(self, df_left, df_right, keys):
        """
        Process a single pair of matching keys.
        Returns indices of matched records and the matched data.
        """
        # Perform merge on specific columns only
        left_key, right_key = keys
        merge_result = pd.merge(
            df_left[[left_key]].reset_index(),
            df_right[[right_key]].reset_index(),
            left_on=left_key,
            right_on=right_key,
            how='inner'
        )

        if not merge_result.empty:
            # Get matched records using loc with integer indexing
            matched_left = df_left.iloc[merge_result['index_x']]
            matched_right = df_right.iloc[merge_result['index_y']]

            # Create matched DataFrame efficiently
            matched_data = pd.concat([matched_left, matched_right], axis=1)
            matched_data.insert(len(matched_data.columns), 'matching_flag', ['matched'] * len(matched_data))

            self.logger.info(f'{left_key} <--> {right_key} || {len(merge_result)} records were matched.')

            return merge_result['index_x'], merge_result['index_y'], matched_data

        return pd.Index([]), pd.Index([]), pd.DataFrame()

    def merge_data(self, return_type='full'):
        """
        Memory-efficient implementation of the merge operation.
        """
        if return_type not in {'left', 'right', 'inner', 'full'}:
            raise ValueError(
                f"Invalid return_type '{return_type}'. Must be one of 'left', 'right', 'inner', or 'full'.")

        # Initialize indices for tracking matched records
        left_matched_indices = pd.Index([])
        right_matched_indices = pd.Index([])
        matched_dfs = []

        # Process each pair of keys
        for keys in self.on_keys_list:
            # Get unmatched records indices
            left_unmatched_mask = ~self.df_left.index.isin(left_matched_indices)
            right_unmatched_mask = ~self.df_right.index.isin(right_matched_indices)

            if not left_unmatched_mask.any() or not right_unmatched_mask.any():
                break

            # Process only unmatched records
            temp_left = self.df_left[left_unmatched_mask]
            temp_right = self.df_right[right_unmatched_mask]

            # Store original indices
            temp_left.index = range(len(temp_left))
            temp_right.index = range(len(temp_right))

            # Process matches for current key pair
            new_left_indices, new_right_indices, matched_df = self._process_matches(temp_left, temp_right, keys)

            if not matched_df.empty:
                matched_dfs.append(matched_df)
                # Update matched indices
                left_matched_indices = left_matched_indices.union(temp_left.index[new_left_indices])
                right_matched_indices = right_matched_indices.union(temp_right.index[new_right_indices])

        # Process unmatched records based on return_type
        result_dfs = []

        if matched_dfs:
            result_dfs.extend(matched_dfs)

        if return_type in {'left', 'full'}:
            left_unmatched = self.df_left[~self.df_left.index.isin(left_matched_indices)]
            if not left_unmatched.empty:
                # Create empty DataFrame with NaN/None values for right columns
                right_empty_data = {
                    f"{self.right_prefix}{col}": pd.Series([None] * len(left_unmatched))
                    for col in self.right_columns
                }
                # Create unmatched DataFrame with empty right columns
                df_unmatched = pd.concat([left_unmatched, pd.DataFrame(right_empty_data)], axis=1)
                df_unmatched.insert(len(df_unmatched.columns), 'matching_flag', ['left_only'] * len(df_unmatched))
                result_dfs.append(df_unmatched)
            elif not result_dfs:  # No matches and return_type is 'left'
                # Return empty DataFrame with all columns
                return pd.DataFrame(columns=[
                    *[f"{self.left_prefix}{col}" for col in self.left_columns],
                    *[f"{self.right_prefix}{col}" for col in self.right_columns],
                    'matching_flag'
                ])

        if return_type in {'right', 'full'}:
            right_unmatched = self.df_right[~self.df_right.index.isin(right_matched_indices)]
            if not right_unmatched.empty:
                right_unmatched = right_unmatched.copy()
                right_unmatched.insert(len(right_unmatched.columns), 'matching_flag',
                                       ['right_only'] * len(right_unmatched))
                result_dfs.append(right_unmatched)

        # Restore original column names
        self.df_left.columns = self.left_columns
        self.df_right.columns = self.right_columns

        # Concatenate results only once at the end
        if not result_dfs:
            return pd.DataFrame(columns=[*self.left_columns, *self.right_columns, 'matching_flag'])

        result = pd.concat(result_dfs, ignore_index=True)

        self.logger.info(f'Matching logic implementation complete.')
        return result
