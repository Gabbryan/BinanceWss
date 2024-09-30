import pandas as pd


class Validators:
    """
    Utility class for validating inputs such as DataFrame schemas and types.
    """

    def __init__(self, df):
        self.df = df

    def validate_dataframe(self):
        """
        Checks if the input is a valid DataFrame.
        :param df: The object to validate.
        :return: True if valid, raises ValueError otherwise.
        """
        if not isinstance(self.df, pd.DataFrame):
            raise ValueError("The provided input is not a valid pandas DataFrame.")
        return True

    def validate_columns(self, required_columns):
        """
        Validates if the DataFrame contains the required columns.
        :param df: The DataFrame to validate.
        :param required_columns: List of required columns.
        :return: True if valid, raises ValueError otherwise.
        """
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        return True
