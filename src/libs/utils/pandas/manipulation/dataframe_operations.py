import pandas as pd


class DataFrameOperations:
    """
    Handles generic DataFrame manipulations like adding, removing, or filtering data.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def filter_rows(self, column: str, value):
        """
        Filters the DataFrame rows based on a condition on the column.
        :param column: The column name to filter by.
        :param value: The value to filter on.
        :return: Filtered DataFrame.
        """
        try:
            return self.df[self.df[column] == value]
        except KeyError:
            raise ValueError(f"Column '{column}' not found in DataFrame")

    def drop_column(self, column: str):
        """
        Drops a column from the DataFrame.
        :param column: The column to drop.
        :return: DataFrame without the specified column.
        """
        try:
            return self.df.drop(columns=[column])
        except KeyError:
            raise ValueError(f"Column '{column}' not found in DataFrame")
