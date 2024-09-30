import pandas as pd


class ParquetHandler:
    """
    Handles Parquet file reading and writing operations.
    """

    def load(self, file_path):
        """
        Load a DataFrame from a Parquet file.

        :param file_path: Path to the Parquet file.
        :return: DataFrame loaded from the file.
        """
        try:
            return pd.read_parquet(file_path)
        except Exception as e:
            raise IOError(f"Error loading Parquet file: {str(e)}")

    def save(self, df, file_path):
        """
        Save a DataFrame to a Parquet file.

        :param df: DataFrame to save.
        :param file_path: Path to save the Parquet file.
        """
        try:
            df.to_parquet(file_path, index=False)
        except Exception as e:
            raise IOError(f"Error saving Parquet file: {str(e)}")
