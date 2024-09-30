import pandas as pd


class DateOperations:
    """
    Handles date-specific operations like parsing, transforming, and resampling.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def parse_dates(self, column: str, format=None):
        """
        Converts a column to datetime.
        :param column: Column name containing date values.
        :param format: Optional format of the date strings.
        :return: DataFrame with the parsed date column.
        """
        try:
            self.df[column] = pd.to_datetime(self.df[column], format=format, errors='coerce')
            return self.df
        except Exception as e:
            raise ValueError(f"Error parsing dates for column '{column}': {str(e)}")

    def resample(self, rule):
        """
        Resamples the DataFrame based on a time rule (e.g., 'M' for monthly).
        :param rule: Resampling rule (e.g., 'M' for month).
        :return: Resampled DataFrame.
        """
        try:
            return self.df.resample(rule).mean()  # Example: Resample and compute the mean
        except Exception as e:
            raise ValueError(f"Error resampling DataFrame: {str(e)}")
