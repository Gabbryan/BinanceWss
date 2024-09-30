import pandas as pd


class ColumnOperations:
    """
    Handles operations related to columns like renaming, adding, and removing columns.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def rename_column(self, old_name: str, new_name: str):
        """
        Renames a column in the DataFrame.
        :param old_name: The old column name.
        :param new_name: The new column name.
        :return: DataFrame with renamed column.
        """
        try:
            return self.df.rename(columns={old_name: new_name})
        except KeyError:
            raise ValueError(f"Column '{old_name}' not found in DataFrame")

    def add_column(self, column_name: str, data):
        """
        Adds a new column to the DataFrame.
        :param column_name: The name of the new column.
        :param data: The data to populate the new column.
        :return: DataFrame with the new column added.
        """
        try:
            self.df[column_name] = data
            return self.df
        except Exception as e:
            raise ValueError(f"Error adding column '{column_name}': {str(e)}")
