import pandas as pd


class SummaryOperations:
    """
    Handles descriptive analytics and summary statistics for DataFrames.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def get_summary(self):
        """
        Generates basic summary statistics (mean, median, standard deviation) for numeric columns.
        :return: Dictionary of summary statistics.
        """
        try:
            # Select only numeric columns
            numeric_df = self.df.select_dtypes(include='number')

            if numeric_df.empty:
                raise ValueError("No numeric columns found in the DataFrame for summary statistics.")

            summary = {
                'mean': numeric_df.mean(),
                'median': numeric_df.median(),
                'std_dev': numeric_df.std(),
                'count': numeric_df.count(),
                'min': numeric_df.min(),
                'max': numeric_df.max()
            }
            return summary
        except Exception as e:
            raise ValueError(f"Error generating summary statistics: {str(e)}")

    def describe(self):
        """
        Calls the DataFrame's describe method for a full summary of numeric columns.
        :return: DataFrame with descriptive statistics.
        """
        try:
            # Use pandas' describe method only for numeric columns
            return self.df.describe(include=[pd.np.number])
        except Exception as e:
            raise ValueError(f"Error generating describe: {str(e)}")
