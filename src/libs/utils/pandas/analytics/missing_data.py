import pandas as pd


class MissingData:
    """
    Handles missing data operations such as detecting, filling, and dropping missing data.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def detect_missing(self):
        """
        Detects missing values in the DataFrame.
        :return: DataFrame indicating the missing values (True/False).
        """
        try:
            return self.df.isnull()
        except Exception as e:
            raise ValueError(f"Error detecting missing data: {str(e)}")

    def fill_missing(self, method='ffill'):
        """
        Fills missing values using forward fill ('ffill'), backward fill ('bfill'), or a specific value.
        :param method: 'ffill', 'bfill', or a specific value.
        :return: DataFrame with missing values filled.
        """
        try:
            if method == 'ffill':
                return self.df.ffill()
            elif method == 'bfill':
                return self.df.bfill()
            else:
                return self.df.fillna(method)
        except Exception as e:
            raise ValueError(f"Error filling missing data: {str(e)}")

    def drop_missing(self, axis=0, how='any'):
        """
        Drops rows or columns with missing data.
        :param axis: 0 to drop rows, 1 to drop columns.
        :param how: 'any' to drop rows/columns with any missing values, 'all' to drop rows/columns with all missing values.
        :return: DataFrame with missing data dropped.
        """
        try:
            return self.df.dropna(axis=axis, how=how)
        except Exception as e:
            raise ValueError(f"Error dropping missing data: {str(e)}")
