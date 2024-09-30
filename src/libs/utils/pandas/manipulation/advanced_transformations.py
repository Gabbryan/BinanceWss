import pandas as pd


class AdvancedTransformations:
    """
    Handles advanced DataFrame transformations like pivoting, melting, and reshaping.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def pivot(self, index, columns, values):
        """
        Pivots the DataFrame.
        :param index: Column to use to make new frame’s index.
        :param columns: Column to use to make new frame’s columns.
        :param values: Column(s) to populate new frame’s values.
        :return: Pivoted DataFrame.
        """
        try:
            return self.df.pivot(index=index, columns=columns, values=values)
        except Exception as e:
            raise ValueError(f"Error pivoting DataFrame: {str(e)}")

    def melt(self, id_vars, value_vars):
        """
        Unpivots the DataFrame from wide format to long format.
        :param id_vars: Columns to keep fixed.
        :param value_vars: Columns to unpivot.
        :return: Melted DataFrame.
        """
        try:
            return self.df.melt(id_vars=id_vars, value_vars=value_vars)
        except Exception as e:
            raise ValueError(f"Error melting DataFrame: {str(e)}")
