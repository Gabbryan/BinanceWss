import pandas as pd


class TimeSeriesOperations:
    """
    Handles time series-specific operations such as rolling windows, resampling, and time-based aggregations.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def rolling_average(self, column, window):
        """
        Computes the rolling average for a specified column.
        :param column: Column to compute rolling average on.
        :param window: Window size for the rolling average.
        :return: Series with the rolling average.
        """
        try:
            return self.df[column].rolling(window=window).mean()
        except Exception as e:
            raise ValueError(f"Error computing rolling average for column '{column}': {str(e)}")

    def resample(self, rule, agg_func='mean'):
        """
        Resamples the time series DataFrame using a specific time rule (e.g., 'M' for monthly).
        :param rule: Resampling rule (e.g., 'M' for month, 'D' for day).
        :param agg_func: Aggregation function to apply (e.g., 'mean', 'sum').
        :return: Resampled DataFrame.
        """
        try:
            return self.df.resample(rule).agg(agg_func)
        except Exception as e:
            raise ValueError(f"Error resampling DataFrame: {str(e)}")
