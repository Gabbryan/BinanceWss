import pandas as pd


class CSVHandler:
    """
    Handles CSV file reading and writing operations.
    """

    def load(self, file_path):
        """
        Load a DataFrame from a CSV file.

        :param file_path: Path to the CSV file.
        :return: DataFrame loaded from the file.
        """
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise IOError(f"Error loading CSV file: {str(e)}")

    def save(self, df, file_path):
        """
        Save a DataFrame to a CSV file.

        :param df: DataFrame to save.
        :param file_path: Path to save the CSV file.
        """
        try:
            df.to_csv(file_path, index=False)
        except Exception as e:
            raise IOError(f"Error saving CSV file: {str(e)}")
