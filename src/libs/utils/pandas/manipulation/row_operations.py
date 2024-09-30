import pandas as pd


class RowOperations:
    """
    Handles operations related to rows like filtering and selecting rows.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def filter_rows_by_condition(self, condition):
        """
        Filters rows based on a provided condition.
        :param condition: A boolean condition to filter rows (e.g., df['column'] > value).
        :return: Filtered DataFrame.
        """
        try:
            return self.df[condition]
        except Exception as e:
            raise ValueError(f"Error filtering rows: {str(e)}")
