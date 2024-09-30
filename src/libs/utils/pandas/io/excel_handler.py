import pandas as pd


class ExcelHandler:
    """
    Handles Excel file reading and writing operations.
    """

    def load(self, file_path):
        """
        Load a DataFrame from an Excel file.

        :param file_path: Path to the Excel file.
        :return: DataFrame loaded from the file.
        """
        try:
            return pd.read_excel(file_path)
        except Exception as e:
            raise IOError(f"Error loading Excel file: {str(e)}")

    def save(self, df, file_path):
        """
        Save a DataFrame to an Excel file.

        :param df: DataFrame to save.
        :param file_path: Path to save the Excel file.
        """
        try:
            df.to_excel(file_path, index=False)
        except Exception as e:
            raise IOError(f"Error saving Excel file: {str(e)}")
