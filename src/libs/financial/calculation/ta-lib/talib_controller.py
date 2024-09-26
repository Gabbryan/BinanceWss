from typing import List, Dict, Any

import numpy as np
import pandas as pd


class TAIndicatorController:

    def __init__(self, df: pd.DataFrame = None):
        """
        Initialize the controller with a DataFrame containing stock data.
        If no DataFrame is provided, a sample DataFrame will be generated for testing.
        """
        if df is None:
            self.df = self.generate_sample_data(1000)
        else:
            self.df = df

        # Ensure the DataFrame index is datetime and sorted
        self.df.index = pd.to_datetime(self.df.index)
        self.df = self.df.sort_index()

    def generate_sample_data(self, rows: int = 1000) -> pd.DataFrame:
        """
        Generate a DataFrame with coherent stock data for testing.
        """
        np.random.seed(42)

        # Generate synthetic stock prices
        base_price = 100
        prices = base_price + np.cumsum(np.random.normal(0, 1, size=rows))

        # Generate Open, High, Low, Close values
        open_prices = prices + np.random.normal(0, 0.5, size=rows)
        close_prices = prices + np.random.normal(0, 0.5, size=rows)
        high_prices = np.maximum(open_prices, close_prices) + np.random.uniform(0.5, 1, size=rows)
        low_prices = np.minimum(open_prices, close_prices) - np.random.uniform(0.5, 1, size=rows)

        # Generate random volume data
        volumes = np.random.randint(10000, 50000, size=rows)

        # Create a DataFrame with all columns and round to 4 decimal places
        data = {
            'Open': np.round(open_prices, 4),
            'High': np.round(high_prices, 4),
            'Low': np.round(low_prices, 4),
            'Close': np.round(close_prices, 4),
            'Volume': volumes
        }

        df = pd.DataFrame(data)
        return df

    def compute_single_indicator(self, indicator_name: str, params: Dict[str, Any] = {}) -> pd.DataFrame:
        """
        Computes a single technical indicator using pandas-ta and returns the result as a DataFrame.
        """
        try:
            # Compute the indicator using the DataFrame's ta accessor
            result = getattr(self.df.ta, indicator_name)(**params)
            # Append the result to the DataFrame
            if isinstance(result, pd.DataFrame):
                self.df = pd.concat([self.df, result], axis=1)
            elif isinstance(result, pd.Series):
                self.df[result.name] = result
            else:
                raise ValueError(f"Unexpected result type: {type(result)}")
            return self.df
        except Exception as e:
            raise ValueError(f"Error computing indicator '{indicator_name}': {e}")

    def compute_custom_indicators(self, custom_indicators: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Computes a custom list of indicators with specified parameters.
        Returns the DataFrame with all indicators computed.
        """
        for indicator in custom_indicators:
            indicator_name = indicator['name']
            params = indicator.get('params', {})
            try:
                self.compute_single_indicator(indicator_name, params)
            except Exception as e:
                print(f"Failed to compute indicator '{indicator_name}': {e}")
        return self.df

    def get_dataframe(self) -> pd.DataFrame:
        """
        Returns the DataFrame with computed indicators.

        Returns:
        - pd.DataFrame: The DataFrame containing stock data and computed indicators.
        """
        return self.df

    def display_full_dataframe(self) -> None:
        """
        Temporarily changes the Pandas settings to display the entire DataFrame without truncation.
        """
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(self.df.tail())


# Example Usage
if __name__ == "__main__":
    # Instantiate the controller with the generated sample data
    controller = TAIndicatorController()

    # Compute a single indicator (e.g., Relative Strength Index - RSI)
    rsi_df = controller.compute_single_indicator('rsi', {'length': 14})
    print("RSI Indicator DataFrame:")
    controller.display_full_dataframe()

    # Compute a list of custom indicators
    custom_indicators = [
        {'name': 'ema', 'params': {'length': 20}},  # Exponential Moving Average
        {'name': 'macd', 'params': {}},  # MACD
    ]
    custom_df = controller.compute_custom_indicators(custom_indicators)
    print("\nDataFrame with Custom Indicators:")
    controller.display_full_dataframe()
