import pandas as pd


class AdvancedStats:
    """
    Handles advanced statistical computations like correlations, regressions, etc.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def correlation(self):
        """
        Computes the correlation matrix for the DataFrame using only numeric columns.
        :return: Correlation matrix DataFrame.
        """
        try:
            # Select only numeric columns for correlation
            numeric_df = self.df.select_dtypes(include='number')

            if numeric_df.empty:
                raise ValueError("No numeric columns found for correlation.")

            return numeric_df.corr()
        except Exception as e:
            raise ValueError(f"Error calculating correlation: {str(e)}")

    def linear_regression(self, x_column, y_column):
        """
        Performs simple linear regression on two columns of the DataFrame.
        :param x_column: The independent variable.
        :param y_column: The dependent variable.
        :return: Slope and intercept of the linear regression.
        """
        try:
            from scipy.stats import linregress
            x = self.df[x_column]
            y = self.df[y_column]
            slope, intercept, r_value, p_value, std_err = linregress(x, y)
            return {
                'slope': slope,
                'intercept': intercept,
                'r_squared': r_value ** 2,
                'p_value': p_value,
                'std_err': std_err
            }
        except Exception as e:
            raise ValueError(f"Error performing linear regression: {str(e)}")
