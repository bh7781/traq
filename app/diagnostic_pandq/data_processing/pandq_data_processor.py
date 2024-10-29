import re

class PANDQDataProcessor:
    def __init__(self, output_filepath=None, data=None):
        """
        Initialize the Processor with a file path or DataFrame.
        """
        self.output_filepath = output_filepath
        self.data = data

    def clean_data(self):
        """
        Perform comprehensive data cleaning tasks:
        1. Replace NaN values with empty strings.
        2. Replace occurrences of '?' in the data with empty strings.
        3. Remove carriage returns, newline characters, and specific unwanted characters.
        4. Convert all data values to strings.
        5. Convert all column names to lowercase and remove leading/trailing spaces.
        6. Replace unwanted characters in column names with underscores.
        7. Replace multiple consecutive underscores in column names with a single underscore.
        """
        try:
            # Convert all float64 columns to object type to avoid FutureWarning when filling NaN with strings
            self.data = self.data.astype({col: 'object' for col in self.data.select_dtypes(include=['float64']).columns})

            # Replace NaN values with empty strings
            self.data.fillna('', inplace=True)

            # Replace occurrences of '?' with empty strings
            self.data.replace('?', '', inplace=True)

            # Remove specific unwanted characters
            self.data.replace({
                r'\r': '_',     # Replace carriage return characters ('\r') with an underscore ('_')
                r'\n': '_',     # Replace newline characters ('\n') with an underscore ('_')
                r'\r\n': '_',   # Replace carriage return + newline sequence ('\r\n') with an underscore ('_')
                r'"': '',       # Remove double quote characters ('"') completely (replace with an empty string)
                r'\|': '_',     # Replace pipe characters ('|') with an underscore ('_')
                r',': ''        # Remove commas (',') completely (replace with an empty string)
            }, regex=True, inplace=True)

            # Convert all data values to strings
            self.data = self.data.astype(str)

            # Clean and standardize column names
            self.data.columns = [
                re.sub(r'[^0-9a-zA-Z_]', '_', col.strip().lower())  # Replace unwanted characters with underscores
                for col in self.data.columns
            ]

            # Replace multiple underscores with a single underscore in column names
            self.data.columns = [re.sub(r'_+', '_', col) for col in self.data.columns]

            # Remove trailing underscores from each column name if present
            self.data.columns = [col[:-1] if col.endswith('_') else col for col in self.data.columns]

        except Exception as e:
            print(f"Error during data cleaning: {e}")
            raise

    def save_data(self, separator):
        """
        Save the processed DataFrame to a CSV file.
        """
        try:
            self.data.to_csv(self.output_filepath, sep=separator, index=False)
        except Exception as e:
            print(f"Error saving data to {self.output_filepath}: {e}")
            raise
