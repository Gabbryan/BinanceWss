import pandas as pd


class JSONHandler:
    """
    Handles JSON file reading and writing operations.
    """

    def load(self, file_path):
        """
        Load a DataFrame from a JSON file.

        :param file_path: Path to the JSON file.
        :return: DataFrame loaded from the file.
        """
        try:
            return pd.read_json(file_path)
        except Exception as e:
            raise IOError(f"Error loading JSON file: {str(e)}")

    def save(self, df, file_path):
        """
        Save a DataFrame to a JSON file.

        :param df: DataFrame to save.
        :param file_path: Path to save the JSON file.
        """
        try:
            df.to_json(file_path, orient='records', lines=True)
        except Exception as e:
            raise IOError(f"Error saving JSON file: {str(e)}")
